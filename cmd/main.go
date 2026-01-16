package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/user/scylla-proxy/internal/config"
	"github.com/user/scylla-proxy/internal/proxy"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	proxyServer, err := proxy.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create proxy: %v", err)
	}

	go func() {
		if err := proxyServer.Start(); err != nil {
			log.Fatalf("Proxy server error: %v", err)
		}
	}()

	fmt.Printf("ScyllaDB Proxy started on %s, forwarding to %s\n", cfg.ListenAddr, cfg.TargetAddr)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Shutting down proxy...")
	proxyServer.Stop()
	fmt.Println("Proxy stopped")
}