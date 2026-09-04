package config

import (
	"flag"
	"fmt"
	"net/url"
	"time"

	"buffered-proxy/pkg/aggregator"
)

type Config struct {
	Host              string
	Port              int
	Upstream          string
	MaxBufferMB       int
	LowWaterMB        int
	MinCoalesceMs     int
	EnableMetrics     bool
	EnableCompression bool
	ParsedUpstream    *url.URL
}

func ParseFlags(args []string) (*Config, error) {
	fs := flag.NewFlagSet("buffered-proxy", flag.ContinueOnError)

	cfg := &Config{}
	fs.StringVar(&cfg.Host, "host", "0.0.0.0", "server bind host")
	fs.IntVar(&cfg.Port, "port", 8080, "server bind port")
	fs.StringVar(&cfg.Upstream, "upstream", "http://127.0.0.1:8000", "upstream AI provider or CLIProxyAPI URL")
	fs.IntVar(&cfg.MaxBufferMB, "max-buffer-mb", 32, "max buffer size per stream in MB (high watermark)")
	fs.IntVar(&cfg.LowWaterMB, "low-water-mb", 24, "low watermark buffer size per stream in MB")
	fs.IntVar(&cfg.MinCoalesceMs, "min-coalesce-ms", 0, "cooperative coalesce delay in milliseconds")
	fs.BoolVar(&cfg.EnableMetrics, "enable-metrics", true, "enable /metrics endpoint")
	fs.BoolVar(&cfg.EnableCompression, "enable-compression", true, "enable response compression (gzip, zstd)")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	u, err := url.Parse(cfg.Upstream)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("invalid upstream URL: %s", cfg.Upstream)
	}
	cfg.ParsedUpstream = u

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
