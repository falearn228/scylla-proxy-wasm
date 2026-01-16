package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/user/scylla-proxy/internal/config"
	"github.com/user/scylla-proxy/internal/proxy"
)

func main() {
	cfg := &config.Config{
		ListenAddr: "127.0.0.1:19042",
		TargetAddr: "127.0.0.1:19043",
		WASMDir:    "./wasm_scripts",
		LogLevel:   "debug",
	}

	srv, err := proxy.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		time.Sleep(2 * time.Second)
		testClient()
		srv.Stop()
	}()

	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
}

func testClient() {
	conn, err := net.Dial("tcp", "127.0.0.1:19042")
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
	n, err := conn.Read(buf)
	if err != nil {
		log.Printf("Read error: %v", err)
		return
	}
	fmt.Printf("Received: %s\n", buf[:n])
}