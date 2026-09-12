package config

import (
	"flag"
	"fmt"
	"net/url"
	"strings"
	"time"

	"buffered-proxy/pkg/aggregator"
)

type UpstreamConfig struct {
	Name   string
	URL    *url.URL
	APIKey string
}

type Config struct {
	Host              string
	Port              int
	Upstream          string
	Upstreams         []UpstreamConfig
	MaxBufferMB       int
	LowWaterMB        int
	MinCoalesceMs     int
	EnableMetrics     bool
	EnableCompression bool
	ParsedUpstream    *url.URL
}

type stringSliceValue struct {
	values []string
	isSet  bool
}

func (s *stringSliceValue) String() string {
	return strings.Join(s.values, ",")
}

func (s *stringSliceValue) Set(val string) error {
	if !s.isSet {
		s.values = nil
		s.isSet = true
	}
	s.values = append(s.values, val)
	return nil
}

func ParseFlags(args []string) (*Config, error) {
	fs := flag.NewFlagSet("buffered-proxy", flag.ContinueOnError)

	cfg := &Config{}
	fs.StringVar(&cfg.Host, "host", "0.0.0.0", "server bind host")
	fs.IntVar(&cfg.Port, "port", 8080, "server bind port")

	upstreamsFlag := &stringSliceValue{
		values: []string{"http://127.0.0.1:8000"},
		isSet:  false,
	}
	fs.Var(upstreamsFlag, "upstream", "upstream AI provider URL(s), e.g. 'http://127.0.0.1:8000' or 'test=http://127.0.0.1:8000'")
	fs.Var(upstreamsFlag, "upstreams", "alias for -upstream")

	apiKeysFlag := &stringSliceValue{}
	fs.Var(apiKeysFlag, "upstream-api-key", "API key for upstream, e.g. 'local=sk-xxx' or 'sk-xxx'")
	fs.Var(apiKeysFlag, "upstream-key", "alias for -upstream-api-key")
	fs.Var(apiKeysFlag, "api-key", "alias for -upstream-api-key")

	fs.IntVar(&cfg.MaxBufferMB, "max-buffer-mb", 32, "max buffer size per stream in MB (high watermark)")
	fs.IntVar(&cfg.LowWaterMB, "low-water-mb", 24, "low watermark buffer size per stream in MB")
	fs.IntVar(&cfg.MinCoalesceMs, "min-coalesce-ms", 0, "cooperative coalesce delay in milliseconds")
	fs.BoolVar(&cfg.EnableMetrics, "enable-metrics", true, "enable /metrics endpoint")
	fs.BoolVar(&cfg.EnableCompression, "enable-compression", true, "enable response compression (gzip, zstd)")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	keyMap := make(map[string]string)
	for _, raw := range apiKeysFlag.values {
		parts := strings.Split(raw, ",")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if idx := strings.Index(part, "="); idx != -1 {
				kName := strings.TrimSpace(part[:idx])
				kVal := strings.TrimSpace(part[idx+1:])
				keyMap[kName] = kVal
			} else {
				keyMap[""] = part
			}
		}
	}

	seenNames := make(map[string]struct{})
	for _, raw := range upstreamsFlag.values {
		parts := strings.Split(raw, ",")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}

			var name, targetURLStr string
			if idx := strings.Index(part, "="); idx != -1 {
				candidateName := strings.TrimSpace(part[:idx])
				if !strings.Contains(candidateName, "://") && !strings.Contains(candidateName, "/") {
					name = candidateName
					targetURLStr = strings.TrimSpace(part[idx+1:])
				} else {
					name = ""
					targetURLStr = part
				}
			} else {
				name = ""
				targetURLStr = part
			}

			var inlineAPIKey string
			if idx := strings.LastIndex(targetURLStr, "@"); idx != -1 {
				after := targetURLStr[idx+1:]
				before := targetURLStr[:idx]
				if (strings.HasPrefix(before, "http://") || strings.HasPrefix(before, "https://")) && !strings.Contains(after, "/") {
					inlineAPIKey = after
					targetURLStr = before
				}
			}

			u, err := url.Parse(targetURLStr)
			if err != nil || u.Scheme == "" || u.Host == "" {
				return nil, fmt.Errorf("invalid upstream URL: %s", targetURLStr)
			}

			if u.User != nil {
				if inlineAPIKey == "" {
					inlineAPIKey = u.User.Username()
					if pass, ok := u.User.Password(); ok && inlineAPIKey == "" {
						inlineAPIKey = pass
					}
				}
				u.User = nil
			}
			if u.Fragment != "" {
				if inlineAPIKey == "" {
					inlineAPIKey = u.Fragment
				}
				u.Fragment = ""
			}

			if name != "" {
				if _, exists := seenNames[name]; exists {
					return nil, fmt.Errorf("duplicate upstream name: %s", name)
				}
				seenNames[name] = struct{}{}
			}

			resolvedKey := inlineAPIKey
			if k, ok := keyMap[name]; ok {
				resolvedKey = k
			}

			cfg.Upstreams = append(cfg.Upstreams, UpstreamConfig{
				Name:   name,
				URL:    u,
				APIKey: resolvedKey,
			})
		}
	}

	if len(cfg.Upstreams) == 1 && cfg.Upstreams[0].APIKey == "" && keyMap[""] != "" {
		cfg.Upstreams[0].APIKey = keyMap[""]
	}

	if len(cfg.Upstreams) == 0 {
		return nil, fmt.Errorf("no valid upstream configured")
	}

	cfg.Upstream = upstreamsFlag.String()
	cfg.ParsedUpstream = cfg.Upstreams[0].URL

	if cfg.Port <= 0 || cfg.Port > 65535 {
		return nil, fmt.Errorf("invalid port: %d", cfg.Port)
	}

	if cfg.MaxBufferMB <= 0 {
		cfg.MaxBufferMB = 32
	}
	if cfg.LowWaterMB <= 0 || cfg.LowWaterMB >= cfg.MaxBufferMB {
		cfg.LowWaterMB = cfg.MaxBufferMB * 3 / 4
	}

	return cfg, nil
}

func (c *Config) BufferConfig() aggregator.BufferConfig {
	return aggregator.BufferConfig{
		HighWatermark:   int64(c.MaxBufferMB) * 1024 * 1024,
		LowWatermark:    int64(c.LowWaterMB) * 1024 * 1024,
		MinCoalesceWait: time.Duration(c.MinCoalesceMs) * time.Millisecond,
	}
}

func (c *Config) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
