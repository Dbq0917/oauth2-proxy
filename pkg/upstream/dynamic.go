package upstream

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/app/pagewriter"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/logger"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/util/ptr"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/validation"
)

// defaultProbeTimeout is the maximum time spent checking that a newly
// configured upstream is reachable before a reload is allowed to take effect.
var defaultProbeTimeout = 2 * time.Second

// upstreamSnapshot is an immutable view of the upstream configuration and the
// handler built for it. Snapshots are never mutated in place; a reload builds a
// brand new snapshot and atomically swaps it in.
type upstreamSnapshot struct {
	config  options.UpstreamConfig
	handler http.Handler
}

// DynamicProxy is an http.Handler that delegates to the upstream handler
// described by the current snapshot.
//
// The active snapshot can be replaced at runtime via Reload and restored via
// Rollback without restarting the process. The swap is an atomic pointer store,
// so in-flight requests keep using the handler they started with while new
// requests immediately use the new one. No request is ever sent to an upstream
// that has been removed or replaced, and a partially built configuration can
// never become active.
type DynamicProxy struct {
	sigData      *options.SignatureData
	writer       pagewriter.Writer
	probeTimeout time.Duration

	// reloadMu serialises reload and rollback attempts so that concurrent
	// configuration changes cannot interleave. It is never held while serving
	// requests; ServeHTTP only reads the atomic pointers.
	reloadMu sync.Mutex
	current  atomic.Pointer[upstreamSnapshot]
	// previous holds the last snapshot that was active before the most recent
	// successful reload, allowing a one-click rollback.
	previous atomic.Pointer[upstreamSnapshot]
}

// NewDynamicProxy builds the initial DynamicProxy from the given configuration.
// It applies the same construction rules as NewProxy, so startup behaviour is
// unchanged. Reachability probing is only performed on subsequent Reload calls.
func NewDynamicProxy(upstreams options.UpstreamConfig, sigData *options.SignatureData, writer pagewriter.Writer) (*DynamicProxy, error) {
	d := &DynamicProxy{
		sigData:      sigData,
		writer:       writer,
		probeTimeout: defaultProbeTimeout,
	}

	snapshot, err := d.buildSnapshot(upstreams, false)
	if err != nil {
		return nil, err
	}
	d.current.Store(snapshot)
	logger.Printf("upstream dynamic proxy initialised: %s", summarizeUpstreams(snapshot.config))
	return d, nil
}

// ServeHTTP forwards every request to the handler of the currently active
// snapshot. The pointer load is lock-free and always returns a complete,
// previously validated snapshot.
func (d *DynamicProxy) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	d.current.Load().handler.ServeHTTP(rw, req)
}

// Reload validates the given configuration, verifies that every newly
// configured network upstream is reachable, builds a brand new handler and
// atomically swaps it in. If validation or probing fails, the currently active
// configuration is kept untouched and the problem is both returned and logged
// as an alert.
func (d *DynamicProxy) Reload(config options.UpstreamConfig) error {
	d.reloadMu.Lock()
	defer d.reloadMu.Unlock()

	current := d.snapshotConfig(d.current.Load())
	logger.Printf("upstream reload requested: current=%s requested=%s",
		summarizeUpstreams(current), summarizeUpstreams(config))

	snapshot, err := d.buildSnapshot(config, true)
	if err != nil {
		logger.Errorf("upstream reload rejected, keeping current configuration (%s): %v",
			summarizeUpstreams(current), err)
		return err
	}

	d.previous.Store(d.current.Load())
	d.current.Store(snapshot)

	logger.Printf("upstream reload applied: %s; changes: %s",
		summarizeUpstreams(snapshot.config), describeURIChanges(current, snapshot.config))
	return nil
}

// Rollback restores the configuration active before the last successful reload.
// Rolling back again swaps back, so the two most recent healthy configurations
// can be toggled without editing any configuration file.
func (d *DynamicProxy) Rollback() error {
	d.reloadMu.Lock()
	defer d.reloadMu.Unlock()

	previous := d.previous.Load()
	if previous == nil {
		err := fmt.Errorf("no previous upstream configuration is available to roll back to")
		logger.Errorf("upstream rollback rejected: %v", err)
		return err
	}

	current := d.current.Load()
	d.previous.Store(current)
	d.current.Store(previous)

	logger.Printf("upstream rollback applied: restored previous configuration %s; changes: %s",
		summarizeUpstreams(previous.config),
		describeURIChanges(d.snapshotConfig(current), previous.config))
	return nil
}

// CurrentConfig returns a copy of the currently active upstream configuration.
func (d *DynamicProxy) CurrentConfig() options.UpstreamConfig {
	return d.snapshotConfig(d.current.Load())
}

// buildSnapshot validates the configuration, optionally probes reachability and
// only then builds the handler. Nothing is swapped here, so any failure leaves
// the active snapshot completely untouched.
func (d *DynamicProxy) buildSnapshot(config options.UpstreamConfig, probe bool) (*upstreamSnapshot, error) {
	desired := cloneUpstreamConfig(config)
	desired.EnsureDefaults()

	if msgs := validation.ValidateUpstreams(desired); len(msgs) > 0 {
		return nil, fmt.Errorf("invalid upstream configuration: %s", joinMessages(msgs))
	}
	// An empty upstream list can legitimately occur at startup (NewProxy has
	// always allowed it), but a reload wiping out every route is treated as a
	// bad/partial configuration write and rejected before it can take effect.
	if probe && len(desired.Upstreams) == 0 {
		return nil, fmt.Errorf("invalid upstream configuration: at least one upstream is required")
	}

	if probe {
		for _, upstream := range desired.Upstreams {
			if err := d.probeUpstream(upstream); err != nil {
				return nil, fmt.Errorf("upstream %q is not reachable: %w", upstream.ID, err)
			}
		}
	}

	handler, err := NewProxy(desired, d.sigData, d.writer)
	if err != nil {
		return nil, fmt.Errorf("could not build upstream proxy: %w", err)
	}

	return &upstreamSnapshot{config: desired, handler: handler}, nil
}

// probeUpstream verifies that a freshly configured upstream can be reached.
// Static upstreams need no network resource. HTTP(S) upstreams are probed with
// a TCP dial, unix upstreams by connecting to the socket and file upstreams by
// checking that the served path exists.
func (d *DynamicProxy) probeUpstream(upstream options.Upstream) error {
	if ptr.Deref(upstream.Static, options.DefaultUpstreamStatic) {
		return nil
	}

	target, err := url.Parse(upstream.URI)
	if err != nil {
		return err
	}

	switch target.Scheme {
	case httpScheme, httpsScheme:
		host := target.Hostname()
		port := target.Port()
		if port == "" {
			if target.Scheme == httpsScheme {
				port = "443"
			} else {
				port = "80"
			}
		}
		address := net.JoinHostPort(host, port)
		conn, err := net.DialTimeout("tcp", address, d.probeTimeout)
		if err != nil {
			return fmt.Errorf("dial %s: %w", address, err)
		}
		return conn.Close()
	case unixScheme:
		conn, err := net.DialTimeout(target.Scheme, target.Path, d.probeTimeout)
		if err != nil {
			return fmt.Errorf("dial unix socket %q: %w", target.Path, err)
		}
		return conn.Close()
	case fileScheme:
		if _, err := os.Stat(target.Path); err != nil {
			return fmt.Errorf("stat file path %q: %w", target.Path, err)
		}
		return nil
	default:
		return fmt.Errorf("unsupported upstream scheme %q", target.Scheme)
	}
}

func (d *DynamicProxy) snapshotConfig(snapshot *upstreamSnapshot) options.UpstreamConfig {
	if snapshot == nil {
		return options.UpstreamConfig{}
	}
	return cloneUpstreamConfig(snapshot.config)
}

// cloneUpstreamConfig returns a deep copy so that callers can never mutate the
// configuration held inside an active or previous snapshot.
func cloneUpstreamConfig(config options.UpstreamConfig) options.UpstreamConfig {
	data, err := json.Marshal(config)
	if err != nil {
		// UpstreamConfig only contains JSON compatible types.
		panic(fmt.Errorf("could not marshal upstream configuration: %w", err))
	}

	var cloned options.UpstreamConfig
	if err := json.Unmarshal(data, &cloned); err != nil {
		panic(fmt.Errorf("could not unmarshal upstream configuration: %w", err))
	}
	return cloned
}

// summarizeUpstreams renders a configuration as a stable, single-line summary
// suitable for logs, e.g. [backend(/)=http://new:8080 static(/ping)=static:202].
func summarizeUpstreams(config options.UpstreamConfig) string {
	if len(config.Upstreams) == 0 {
		return "[]"
	}

	upstreams := append([]options.Upstream(nil), config.Upstreams...)
	sort.Slice(upstreams, func(i, j int) bool {
		return upstreams[i].ID < upstreams[j].ID
	})

	summary := "["
	for i, upstream := range upstreams {
		if i > 0 {
			summary += ", "
		}
		target := upstream.URI
		if ptr.Deref(upstream.Static, options.DefaultUpstreamStatic) {
			target = fmt.Sprintf("static:%d", ptr.Deref(upstream.StaticCode, options.DefaultUpstreamStaticCode))
		}
		summary += fmt.Sprintf("%s(%s)=%s", upstream.ID, upstream.Path, target)
	}
	return summary + "]"
}

// describeURIChanges lists the per-upstream URI changes between two
// configurations so the logs show exactly what a switch did.
func describeURIChanges(before, after options.UpstreamConfig) string {
	beforeURI := make(map[string]string, len(before.Upstreams))
	for _, upstream := range before.Upstreams {
		beforeURI[upstream.ID] = upstreamTarget(upstream)
	}

	changes := ""
	upstreams := append([]options.Upstream(nil), after.Upstreams...)
	sort.Slice(upstreams, func(i, j int) bool {
		return upstreams[i].ID < upstreams[j].ID
	})
	for _, upstream := range upstreams {
		oldTarget, ok := beforeURI[upstream.ID]
		newTarget := upstreamTarget(upstream)
		if !ok {
			changes += fmt.Sprintf("%s added %q; ", upstream.ID, newTarget)
			continue
		}
		if oldTarget != newTarget {
			changes += fmt.Sprintf("%s %q -> %q; ", upstream.ID, oldTarget, newTarget)
		}
	}
	if changes == "" {
		return "none"
	}
	return changes
}

func upstreamTarget(upstream options.Upstream) string {
	if ptr.Deref(upstream.Static, options.DefaultUpstreamStatic) {
		return fmt.Sprintf("static:%d", ptr.Deref(upstream.StaticCode, options.DefaultUpstreamStaticCode))
	}
	return upstream.URI
}

func joinMessages(msgs []string) string {
	joined := ""
	for i, msg := range msgs {
		if i > 0 {
			joined += "; "
		}
		joined += msg
	}
	return joined
}
