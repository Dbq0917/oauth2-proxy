package upstream

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	middlewareapi "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/middleware"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/app/pagewriter"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/logger"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// threadSafeBuffer is a bytes.Buffer safe for concurrent writes from the
// logger while the test is serving requests.
type threadSafeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *threadSafeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *threadSafeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

var _ = Describe("DynamicProxy", func() {
	const upstreamPath = "/http/"

	var (
		oldBackend *httptest.Server
		newBackend *httptest.Server
		logs       *threadSafeBuffer
		proxy      *DynamicProxy
	)

	newTestWriter := func() pagewriter.Writer {
		return &pagewriter.WriterFuncs{
			ProxyErrorFunc: func(rw http.ResponseWriter, _ *http.Request, _ error) {
				rw.WriteHeader(http.StatusBadGateway)
				_, _ = rw.Write([]byte("Proxy Error"))
			},
		}
	}

	upstreamConfigFor := func(uri string) options.UpstreamConfig {
		return options.UpstreamConfig{
			Upstreams: []options.Upstream{
				{
					ID:   "http-backend",
					Path: upstreamPath,
					URI:  uri,
				},
			},
		}
	}

	serveRequest := func() (status int, upstreamID string) {
		req := middlewareapi.AddRequestScope(
			httptest.NewRequest(http.MethodGet, "http://example.localhost/http/1234", nil),
			&middlewareapi.RequestScope{},
		)
		req.RemoteAddr = ""
		rw := httptest.NewRecorder()

		proxy.ServeHTTP(rw, req)

		scope := middlewareapi.GetRequestScope(req)
		return rw.Code, scope.Upstream
	}

	BeforeEach(func() {
		oldBackend = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("old-backend"))
		}))
		newBackend = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("new-backend"))
		}))

		logs = &threadSafeBuffer{}
		logger.SetOutput(logs)
		logger.SetErrOutput(logs)

		var err error
		proxy, err = NewDynamicProxy(upstreamConfigFor(oldBackend.URL), nil, newTestWriter())
		Expect(err).ToNot(HaveOccurred())
		proxy.probeTimeout = 200 * time.Millisecond
	})

	AfterEach(func() {
		oldBackend.Close()
		newBackend.Close()
	})

	It("hot reloads an upstream address without a restart and logs the switch", func() {
		status, upstreamID := serveRequest()
		Expect(status).To(Equal(http.StatusOK))
		Expect(upstreamID).To(Equal("http-backend"))

		Expect(proxy.Reload(upstreamConfigFor(newBackend.URL))).To(Succeed())

		// After the reload every subsequent request must be handled by the new
		// upstream while the process keeps running.
		req := middlewareapi.AddRequestScope(
			httptest.NewRequest(http.MethodGet, "http://example.localhost/http/1234", nil),
			&middlewareapi.RequestScope{},
		)
		req.RemoteAddr = ""
		rw := httptest.NewRecorder()
		proxy.ServeHTTP(rw, req)
		Expect(rw.Code).To(Equal(http.StatusOK))
		Expect(rw.Body.String()).To(Equal("new-backend"))

		Expect(proxy.CurrentConfig().Upstreams[0].URI).To(Equal(newBackend.URL))

		// Logs must show the request to switch and the moment it took effect.
		Expect(logs.String()).To(ContainSubstring("upstream reload requested"))
		Expect(logs.String()).To(ContainSubstring("upstream reload applied"))
		Expect(logs.String()).To(ContainSubstring(oldBackend.URL))
		Expect(logs.String()).To(ContainSubstring(newBackend.URL))
	})

	It("keeps the current configuration and raises an alert for an unreachable upstream", func() {
		unreachableConfig := upstreamConfigFor(invalidServer)

		err := proxy.Reload(unreachableConfig)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("not reachable"))

		// The active configuration must be untouched.
		Expect(proxy.CurrentConfig().Upstreams[0].URI).To(Equal(oldBackend.URL))

		// Traffic still reaches the old, healthy upstream.
		status, upstreamID := serveRequest()
		Expect(status).To(Equal(http.StatusOK))
		Expect(upstreamID).To(Equal("http-backend"))

		// The rejection must be visible as an alert in the logs.
		Expect(logs.String()).To(ContainSubstring("upstream reload rejected"))
		Expect(logs.String()).To(ContainSubstring("keeping current configuration"))
	})

	It("keeps the current configuration for an invalid configuration and raises an alert", func() {
		invalidConfig := options.UpstreamConfig{
			Upstreams: []options.Upstream{
				{
					// Missing id and path, empty uri: fails the same validation
					// that runs at process startup.
					ID:   "",
					Path: "",
				},
			},
		}

		err := proxy.Reload(invalidConfig)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("invalid upstream configuration"))

		Expect(proxy.CurrentConfig().Upstreams[0].URI).To(Equal(oldBackend.URL))

		status, upstreamID := serveRequest()
		Expect(status).To(Equal(http.StatusOK))
		Expect(upstreamID).To(Equal("http-backend"))

		Expect(logs.String()).To(ContainSubstring("upstream reload rejected"))
	})

	It("rolls back to the previous upstream with one call and logs it", func() {
		Expect(proxy.Reload(upstreamConfigFor(newBackend.URL))).To(Succeed())

		req := middlewareapi.AddRequestScope(
			httptest.NewRequest(http.MethodGet, "http://example.localhost/http/1234", nil),
			&middlewareapi.RequestScope{},
		)
		rw := httptest.NewRecorder()
		proxy.ServeHTTP(rw, req)
		Expect(rw.Body.String()).To(Equal("new-backend"))

		// One-click rollback: no configuration file edit, no restart.
		Expect(proxy.Rollback()).To(Succeed())

		req = middlewareapi.AddRequestScope(
			httptest.NewRequest(http.MethodGet, "http://example.localhost/http/1234", nil),
			&middlewareapi.RequestScope{},
		)
		rw = httptest.NewRecorder()
		proxy.ServeHTTP(rw, req)
		Expect(rw.Code).To(Equal(http.StatusOK))
		Expect(rw.Body.String()).To(Equal("old-backend"))
		Expect(proxy.CurrentConfig().Upstreams[0].URI).To(Equal(oldBackend.URL))

		Expect(logs.String()).To(ContainSubstring("upstream rollback applied"))

		// Rolling back again restores the newer configuration.
		Expect(proxy.Rollback()).To(Succeed())
		Expect(proxy.CurrentConfig().Upstreams[0].URI).To(Equal(newBackend.URL))
	})

	It("rejects a rollback when no previous configuration exists", func() {
		err := proxy.Rollback()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("no previous upstream configuration"))
		Expect(logs.String()).To(ContainSubstring("upstream rollback rejected"))
	})

	It("rejects a reload that would wipe out every upstream", func() {
		err := proxy.Reload(options.UpstreamConfig{})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("at least one upstream is required"))

		// The running configuration must survive a partial/empty write.
		Expect(proxy.CurrentConfig().Upstreams[0].URI).To(Equal(oldBackend.URL))

		status, upstreamID := serveRequest()
		Expect(status).To(Equal(http.StatusOK))
		Expect(upstreamID).To(Equal("http-backend"))
	})

	It("never serves a partially switched configuration while reloading", func() {
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 25; j++ {
					req := middlewareapi.AddRequestScope(
						httptest.NewRequest(http.MethodGet, "http://example.localhost/http/1234", nil),
						&middlewareapi.RequestScope{},
					)
					rw := httptest.NewRecorder()
					proxy.ServeHTTP(rw, req)
					// Every request must complete against one complete,
					// healthy snapshot, never with a 502 from a half switch.
					Expect(rw.Code).To(BeElementOf(http.StatusOK, http.StatusMovedPermanently))
				}
			}()
		}

		Expect(proxy.Reload(upstreamConfigFor(newBackend.URL))).To(Succeed())
		Expect(proxy.Reload(upstreamConfigFor(oldBackend.URL))).To(Succeed())
		wg.Wait()
	})
})
