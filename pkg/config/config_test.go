package config

import (
	"testing"
	"time"
)

func TestParseFlagsDefaults(t *testing.T) {
	cfg, err := ParseFlags([]string{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if cfg.Host != "0.0.0.0" {
		t.Fatalf("expected host 0.0.0.0, got %s", cfg.Host)
	}
	if cfg.Port != 8080 {
		t.Fatalf("expected port 8080, got %d", cfg.Port)
	}
	if cfg.Upstream != "http://127.0.0.1:8000" {
		t.Fatalf("expected default upstream, got %s", cfg.Upstream)
	}
	if cfg.Address() != "0.0.0.0:8080" {
		t.Fatalf("expected address 0.0.0.0:8080, got %s", cfg.Address())
	}
	if !cfg.EnableCompression {
		t.Fatalf("expected compression enabled by default")
	}
	bufCfg := cfg.BufferConfig()
	if bufCfg.HighWatermark != 32*1024*1024 {
		t.Fatalf("expected 32MB high watermark, got %d", bufCfg.HighWatermark)
	}
	if bufCfg.LowWatermark != 24*1024*1024 {
		t.Fatalf("expected 24MB low watermark, got %d", bufCfg.LowWatermark)
	}
}

func TestParseFlagsCustom(t *testing.T) {
	args := []string{
		"-host", "127.0.0.1",
		"-port", "9090",
		"-upstream", "http://localhost:3000",
		"-max-buffer-mb", "64",
		"-low-water-mb", "48",
		"-min-coalesce-ms", "5",
		"-enable-compression=false",
	}
	cfg, err := ParseFlags(args)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if cfg.EnableCompression {
		t.Fatalf("expected compression disabled")
	}

	if cfg.Host != "127.0.0.1" || cfg.Port != 9090 {
		t.Fatalf("unexpected host/port: %s:%d", cfg.Host, cfg.Port)
	}
	if cfg.Upstream != "http://localhost:3000" {
		t.Fatalf("unexpected upstream: %s", cfg.Upstream)
	}
	bufCfg := cfg.BufferConfig()
	if bufCfg.HighWatermark != 64*1024*1024 {
		t.Fatalf("unexpected high watermark: %d", bufCfg.HighWatermark)
	}
	if bufCfg.MinCoalesceWait != 5*time.Millisecond {
		t.Fatalf("unexpected min coalesce wait: %v", bufCfg.MinCoalesceWait)
	}
}

func TestParseFlagsInvalid(t *testing.T) {
	_, err := ParseFlags([]string{"-upstream", "invalid_url"})
	if err == nil {
		t.Fatalf("expected error on invalid upstream")
	}

	_, err = ParseFlags([]string{"-port", "99999"})
	if err == nil {
		t.Fatalf("expected error on invalid port")
	}

	_, err = ParseFlags([]string{"-upstream", "test=http://127.0.0.1:8000", "-upstream", "test=http://127.0.0.1:8001"})
	if err == nil {
		t.Fatalf("expected error on duplicate upstream name")
	}
}

func TestParseFlagsMultipleUpstreams(t *testing.T) {
	// Repeated -upstream flag
	cfg, err := ParseFlags([]string{
		"-upstream", "test=http://127.0.0.1:8001",
		"-upstream", "prod=http://127.0.0.1:8002",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Upstreams) != 2 {
		t.Fatalf("expected 2 upstreams, got %d", len(cfg.Upstreams))
	}
	if cfg.Upstreams[0].Name != "test" || cfg.Upstreams[0].URL.String() != "http://127.0.0.1:8001" {
		t.Fatalf("unexpected upstream 0: %+v", cfg.Upstreams[0])
	}
	if cfg.Upstreams[1].Name != "prod" || cfg.Upstreams[1].URL.String() != "http://127.0.0.1:8002" {
		t.Fatalf("unexpected upstream 1: %+v", cfg.Upstreams[1])
	}

	// Comma-separated -upstream flag
	cfg, err = ParseFlags([]string{
		"-upstream", "test=http://127.0.0.1:8001,prod=http://127.0.0.1:8002",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Upstreams) != 2 {
		t.Fatalf("expected 2 upstreams, got %d", len(cfg.Upstreams))
	}

	// -upstreams alias
	cfg, err = ParseFlags([]string{
		"-upstreams", "alpha=http://127.0.0.1:8001,beta=http://127.0.0.1:8002",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Upstreams) != 2 || cfg.Upstreams[0].Name != "alpha" || cfg.Upstreams[1].Name != "beta" {
		t.Fatalf("unexpected upstreams: %+v", cfg.Upstreams)
	}
}

func TestParseFlagsUpstreamAPIKey(t *testing.T) {
	// Flags with -upstream-api-key
	cfg, err := ParseFlags([]string{
		"-upstream", "local=http://127.0.0.1:8001",
		"-upstream", "grok=http://127.0.0.1:8002",
		"-upstream-api-key", "local=sk-local-123",
		"-upstream-api-key", "grok=sk-grok-456",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Upstreams) != 2 {
		t.Fatalf("expected 2 upstreams, got %d", len(cfg.Upstreams))
	}
	if cfg.Upstreams[0].APIKey != "sk-local-123" {
		t.Fatalf("expected local key 'sk-local-123', got %q", cfg.Upstreams[0].APIKey)
	}
	if cfg.Upstreams[1].APIKey != "sk-grok-456" {
		t.Fatalf("expected grok key 'sk-grok-456', got %q", cfg.Upstreams[1].APIKey)
	}

	// Comma separated -upstream-api-key
	cfg, err = ParseFlags([]string{
		"-upstream", "local=http://127.0.0.1:8001,grok=http://127.0.0.1:8002",
		"-upstream-api-key", "local=sk-local-123,grok=sk-grok-456",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Upstreams[0].APIKey != "sk-local-123" || cfg.Upstreams[1].APIKey != "sk-grok-456" {
		t.Fatalf("unexpected api keys: %+v", cfg.Upstreams)
	}

	// Inline key via fragment and @
	cfg, err = ParseFlags([]string{
		"-upstream", "local=http://127.0.0.1:8001#sk-inline-local",
		"-upstream", "grok=http://127.0.0.1:8002@sk-inline-grok",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Upstreams[0].APIKey != "sk-inline-local" {
		t.Fatalf("expected 'sk-inline-local', got %q", cfg.Upstreams[0].APIKey)
	}
	if cfg.Upstreams[1].APIKey != "sk-inline-grok" {
		t.Fatalf("expected 'sk-inline-grok', got %q", cfg.Upstreams[1].APIKey)
	}
}
