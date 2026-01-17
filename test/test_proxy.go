package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/falearn/scylla-proxy-wasm/internal/config"
	"github.com/falearn/scylla-proxy-wasm/internal/proxy"
)

func main() {
	cfg := &config.Config{
		ListenAddr: "127.0.0.1:8080",
		TargetAddr: "127.0.0.1:9043",
		WASMDir:    "./wasm_scripts",
		LogLevel:   "info",
	}

	srv, err := proxy.New(cfg)
	if err != nil {
		log.Fatal("fatal error proxy", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		// Wait for server to be ready
		time.Sleep(500 * time.Millisecond)
		testClient()
		// Give some time for the proxy to process
		time.Sleep(500 * time.Millisecond)
		srv.Stop()
	}()

	if err := srv.Start(); err != nil {
		log.Fatal("fatal error serve", err)
	}
	wg.Wait()
}

func testClient() {
	conn, err := net.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		log.Printf("Client connect error: %v", err)
		return
	}
	defer conn.Close()

	_, err = conn.Write([]byte("SELECT email FROM users WHERE id = 1;"))
	if err != nil {
		log.Printf("Write error: %v", err)
		return
	}

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		log.Printf("Read error test: %v", err)
		return
	}
	fmt.Printf("Received: %s\n", buf[:n])
}
