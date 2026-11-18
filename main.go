package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/logger"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/validation"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/version"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/watcher"
	"github.com/spf13/pflag"
	"go.yaml.in/yaml/v4"
)

func main() {
	logger.SetFlags(logger.Lshortfile)

	configFlagSet := pflag.NewFlagSet("oauth2-proxy", pflag.ContinueOnError)

	// Because we parse early to determine alpha vs legacy config, we have to
	// ignore any unknown flags for now
	configFlagSet.ParseErrorsAllowlist.UnknownFlags = true

	config := configFlagSet.String("config", "", "path to config file")
	alphaConfig := configFlagSet.String("alpha-config", "", "path to alpha config file (use at your own risk - the structure in this config file may change between minor releases)")
	convertConfig := configFlagSet.Bool("convert-config-to-alpha", false, "if true, the proxy will load configuration as normal and convert existing configuration to the alpha config structure, and print it to stdout")
	showVersion := configFlagSet.Bool("version", false, "print version string")
	configTest := configFlagSet.Bool("config-test", false, "test the configuration and exit")
	configFlagSet.Parse(os.Args[1:])

	if *showVersion {
		fmt.Printf("oauth2-proxy %s (built with %s)\n", version.VERSION, runtime.Version())
		return
	}

	if *convertConfig && *alphaConfig != "" {
		logger.Fatal("cannot use alpha-config and convert-config-to-alpha together")
	}

	if *configTest && *convertConfig {
		logger.Fatal("cannot use config-test and convert-config-to-alpha together")
	}

	opts, err := loadConfiguration(*config, *alphaConfig, configFlagSet, os.Args[1:])
	if err != nil {
		logger.Fatalf("ERROR: %v", err)
	}

	if *configTest {
		if err = validation.Validate(opts); err != nil {
			logger.Errorf("%s", err)
			os.Exit(1)
		}
		fmt.Println("configuration is valid")
		return
	}

	if *convertConfig {
		if err := printConvertedConfig(opts); err != nil {
			logger.Fatalf("ERROR: could not convert config: %v", err)
		}
		return
	}

	if err = validation.Validate(opts); err != nil {
		logger.Fatalf("%s", err)
	}

	validator := NewValidator(opts.EmailDomains, opts.AuthenticatedEmailsFile)
	oauthproxy, err := NewOAuthProxy(opts, validator)
	if err != nil {
		logger.Fatalf("ERROR: Failed to initialise OAuth2 Proxy: %v", err)
	}

	watchUpstreamConfig(*config, *alphaConfig, configFlagSet, os.Args[1:], oauthproxy)

	if err := oauthproxy.Start(); err != nil {
		logger.Fatalf("ERROR: Failed to start OAuth2 Proxy: %v", err)
	}
}

// watchUpstreamConfig reloads the upstream configuration from the configuration
// files whenever they change on disk. The whole options file is re-parsed using
// the exact same loading path and configuration format as at startup, but only
// the upstream section is hot applied. Any parse, validation or reachability
// failure is logged by the DynamicProxy and leaves the running configuration
// untouched, so a bad write can never break the serving proxy.
func watchUpstreamConfig(config, alphaConfig string, extraFlags *pflag.FlagSet, args []string, proxy *OAuthProxy) {
	if config == "" && alphaConfig == "" {
		return
	}

	reload := func() {
		reloaded, err := loadConfiguration(config, alphaConfig, extraFlags, args)
		if err != nil {
			logger.Errorf("upstream reload skipped: failed to reload configuration file, keeping current configuration: %v", err)
			return
		}
		// Only the upstream section is hot applied, so restrict validation to
		// it. Full Options validation also covers flags supplied on the
		// command line, which are intentionally absent from a re-parse of the
		// configuration files and would produce false positives.
		if len(reloaded.UpstreamServers.Upstreams) == 0 {
			logger.Errorf("upstream reload skipped: reloaded configuration contains no upstreams, keeping current configuration (the file may still be being written)")
			return
		}
		if msgs := validation.ValidateUpstreams(reloaded.UpstreamServers); len(msgs) > 0 {
			logger.Errorf("upstream reload skipped: reloaded upstream configuration is invalid, keeping current configuration: %s", strings.Join(msgs, "; "))
			return
		}
		if err := proxy.ReloadUpstreams(reloaded.UpstreamServers); err != nil {
			// The DynamicProxy already logs the detailed rejection reason.
			logger.Errorf("upstream reload failed, current configuration kept: %v", err)
		}
	}

	if config != "" {
		if err := watcher.WatchFileForUpdates(config, nil, reload); err != nil {
			logger.Errorf("failed to watch configuration file %q for upstream reloads: %v", config, err)
		}
	}
	if alphaConfig != "" {
		if err := watcher.WatchFileForUpdates(alphaConfig, nil, reload); err != nil {
			logger.Errorf("failed to watch alpha configuration file %q for upstream reloads: %v", alphaConfig, err)
		}
	}
}

// loadConfiguration will load in the user's configuration.
// It will either load the alpha configuration (if alphaConfig is given)
// or the legacy configuration.
func loadConfiguration(config, yamlConfig string, extraFlags *pflag.FlagSet, args []string) (*options.Options, error) {
	opts, err := loadLegacyOptions(config, extraFlags, args)
	if err != nil {
		return nil, fmt.Errorf("failed to load legacy options: %w", err)
	}

	if yamlConfig != "" {
		logger.Printf("WARNING: You are using alpha configuration. The structure in this configuration file may change without notice. You MUST remove conflicting options from your existing configuration.")
		opts, err = loadYamlOptions(yamlConfig, config, extraFlags, args)
		if err != nil {
			return nil, fmt.Errorf("failed to load yaml options: %w", err)
		}
	}

	// Ensure defaults after loading configuration
	opts.EnsureDefaults()
	return opts, nil
}

// loadLegacyOptions loads the old toml options using the legacy flagset
// and legacy options struct.
func loadLegacyOptions(config string, extraFlags *pflag.FlagSet, args []string) (*options.Options, error) {
	optionsFlagSet := options.NewLegacyFlagSet()
	optionsFlagSet.AddFlagSet(extraFlags)
	if err := optionsFlagSet.Parse(args); err != nil {
		return nil, fmt.Errorf("failed to parse flags: %v", err)
	}

	legacyOpts := options.NewLegacyOptions()
	if err := options.Load(config, optionsFlagSet, legacyOpts); err != nil {
		return nil, fmt.Errorf("failed to load config: %v", err)
	}

	opts, err := legacyOpts.ToOptions()
	if err != nil {
		return nil, fmt.Errorf("failed to convert config: %v", err)
	}

	return opts, nil
}

// loadYamlOptions loads the old style config excluding options converted to
// the new alpha format, then merges the alpha options, loaded from YAML,
// into the core configuration.
func loadYamlOptions(yamlConfig, config string, extraFlags *pflag.FlagSet, args []string) (*options.Options, error) {
	opts, err := loadOptions(config, extraFlags, args)
	if err != nil {
		return nil, fmt.Errorf("failed to load core options: %v", err)
	}

	alphaOpts := options.NewAlphaOptions(opts)
	if err := options.LoadYAML(yamlConfig, alphaOpts); err != nil {
		return nil, fmt.Errorf("failed to load alpha options: %v", err)
	}

	alphaOpts.MergeOptionsWithDefaults(opts)
	return opts, nil
}

// loadOptions loads the configuration using the old style format into the
// core options.Options struct.
// This means that none of the options that have been converted to alpha config
// will be loaded using this method.
func loadOptions(config string, extraFlags *pflag.FlagSet, args []string) (*options.Options, error) {
	optionsFlagSet := options.NewFlagSet()
	optionsFlagSet.AddFlagSet(extraFlags)
	if err := optionsFlagSet.Parse(args); err != nil {
		return nil, fmt.Errorf("failed to parse flags: %v", err)
	}

	opts := options.NewOptions()
	if err := options.Load(config, optionsFlagSet, opts); err != nil {
		return nil, fmt.Errorf("failed to load config: %v", err)
	}

	return opts, nil
}

// printConvertedConfig extracts alpha options from the loaded configuration
// and renders these to stdout in YAML format.
func printConvertedConfig(opts *options.Options) error {
	alphaConfig := options.NewAlphaOptions(opts)

	// Generic interface for loading arbitrary yaml structure
	var buffer map[string]interface{}

	if err := options.Decode(alphaConfig, &buffer); err != nil {
		return fmt.Errorf("unable to decode alpha config into interface: %w", err)
	}

	data, err := yaml.Marshal(buffer)
	if err != nil {
		return fmt.Errorf("unable to marshal config: %v", err)
	}

	if _, err := os.Stdout.Write(data); err != nil {
		return fmt.Errorf("unable to write output: %v", err)
	}

	return nil
}
