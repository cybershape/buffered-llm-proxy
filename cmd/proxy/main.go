package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"buffered-proxy/pkg/config"
	"buffered-proxy/pkg/proxy"
)

func main() {
	cfg, err := config.ParseFlags(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		log.Fatalf("failed to parse config: %v", err)
	}

	proxySrv := proxy.NewProxyServer(proxy.ServerConfig{
		UpstreamURL:     cfg.ParsedUpstream,
		BufferConfig:    cfg.BufferConfig(),
		AllowMetricsAPI: cfg.EnableMetrics,
	})

	server := &http.Server{
		Addr:         cfg.Address(),
		Handler:      proxySrv,
		ReadTimeout:  0,
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("Starting buffered proxy on %s -> upstream %s", cfg.Address(), cfg.Upstream)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen error: %v", err)
		}
	}()

	<-stopChan
	log.Println("Shutting down proxy server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}
	log.Println("Proxy server gracefully stopped")
}
