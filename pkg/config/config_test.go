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
	}
	cfg, err := ParseFlags(args)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
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
}
