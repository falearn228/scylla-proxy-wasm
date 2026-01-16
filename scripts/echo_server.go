package main

// import (
// 	"fmt"
// 	"io"
// 	"log"
// 	"net"
// )

// func main() {
// 	listener, err := net.Listen("tcp", "127.0.0.1:9043")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer listener.Close()
// 	fmt.Println("Echo server listening on 127.0.0.1:9043")

// 	for {
// 		conn, err := listener.Accept()
// 		if err != nil {
// 			log.Printf("Accept error: %v", err)
// 			continue
// 		}
// 		go handle(conn)
// 	}
// }

// func handle(conn net.Conn) {
// 	defer conn.Close()
// 	buf := make([]byte, 4096)
// 	for {
// 		n, err := conn.Read(buf)
// 		if err != nil {
// 			if err != io.EOF {
// 				log.Printf("Read error: %v", err)
// 			}
// 			return
// 		}
// 		fmt.Printf("Echo server received: %s\n", buf[:n])
// 		// Echo back
// 		conn.Write(buf[:n])
// 	}
// }
