//go:build ignore
// +build ignore

package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

func main() {
	port := flag.String("port", "9043", "Port to listen on")
	flag.Parse()

	addr := fmt.Sprintf("127.0.0.1:%s", *port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	fmt.Printf("Mock ScyllaDB server listening on %s\n", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	fmt.Printf("Client connected: %s\n", conn.RemoteAddr())

	buf := make([]byte, 32*1024)
	for {
		// Set a read deadline
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Printf("Client disconnected: %s\n", conn.RemoteAddr())
			} else {
				log.Printf("Read error: %v", err)
			}
			return
		}

		data := buf[:n]
		fmt.Printf("Received %d bytes from %s: %q\n", n, conn.RemoteAddr(), string(data))

		// Echo back the data
		_, err = conn.Write(data)
		if err != nil {
			log.Printf("Write error: %v", err)
			return
		}

		fmt.Printf("Echoed %d bytes back to %s\n", n, conn.RemoteAddr())
	}
}
