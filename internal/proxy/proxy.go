package proxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/user/scylla-proxy/internal/config"
	"github.com/user/scylla-proxy/internal/wasm"
)

type Server struct {
	cfg      *config.Config
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	engine   *wasm.Engine
}

func New(cfg *config.Config) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())
	engine, err := wasm.NewEngine(cfg.WASMInstancePoolSize)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create WASM engine: %w", err)
	}
	// Load WASM scripts from configured directory
	err = engine.LoadDirectory(cfg.WASMDir)
	if err != nil {
		log.Printf("Warning: failed to load WASM directory: %v", err)
	}
	srv := &Server{
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
		engine: engine,
	}
	return srv, nil
}

func (s *Server) Start() error {
	var listener net.Listener
	var err error

	if s.cfg.TLS.Enabled {
		cert, err := tls.LoadX509KeyPair(s.cfg.TLS.CertFile, s.cfg.TLS.KeyFile)
		if err != nil {
			return fmt.Errorf("failed to load key pair: %w", err)
		}
		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
		listener, err = tls.Listen("tcp", s.cfg.ListenAddr, tlsCfg)
		if err != nil {
			return fmt.Errorf("failed to listen with TLS on %s: %w", s.cfg.ListenAddr, err)
		}
		log.Printf("Proxy listening with TLS on %s", s.cfg.ListenAddr)
	} else {
		listener, err = net.Listen("tcp", s.cfg.ListenAddr)
		if err != nil {
			return fmt.Errorf("failed to listen on %s: %w", s.cfg.ListenAddr, err)
		}
		log.Printf("Proxy listening on %s", s.cfg.ListenAddr)
	}

	s.listener = listener
	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:
			conn, err := listener.Accept()
			if err != nil {
				// Suppress "use of closed network connection" error during shutdown
				if netErr, ok := err.(*net.OpError); ok && netErr.Err.Error() == "use of closed network connection" {
					log.Printf("[SHUTDOWN] Listener closed, stopping accept loop")
					return nil
				}
				log.Printf("Accept error: %v", err)
				continue
			}
			go s.handleConnection(conn)
		}
	}
}

func (s *Server) transformRequest(data []byte) ([]byte, error) {
	if s.engine == nil {
		return data, nil
	}
	modules := s.engine.ListModules()
	if len(modules) == 0 {
		return data, nil
	}
	return s.engine.Transform(data, modules[0])
}

func (s *Server) transformResponse(data []byte) ([]byte, error) {
	if s.engine == nil {
		return data, nil
	}
	modules := s.engine.ListModules()
	if len(modules) == 0 {
		return data, nil
	}
	return s.engine.Transform(data, modules[0])
}

func (s *Server) Stop() {
	s.cancel()
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
}

func (s *Server) handleConnection(client net.Conn) {
	s.wg.Add(1)
	defer s.wg.Done()
	defer client.Close()

	log.Printf("[HANDLE] New connection from %s", client.RemoteAddr())

	target, err := net.DialTimeout("tcp", s.cfg.TargetAddr, 5*time.Second)
	if err != nil {
		log.Printf("Failed to connect to target %s: %v", s.cfg.TargetAddr, err)
		return
	}

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	// Create a channel to signal when one direction closes
	done := make(chan struct{}, 2)

	log.Printf("[HANDLE] Starting pipes for connection from %s", client.RemoteAddr())
	go s.pipe(ctx, client, target, "client->target", done)
	go s.pipe(ctx, target, client, "target->client", done)

	// Wait for either context cancellation or one pipe closing
	select {
	case <-ctx.Done():
		log.Printf("[HANDLE] Context cancelled for %s", client.RemoteAddr())
	case <-done:
		log.Printf("[HANDLE] One pipe closed for %s, cancelling context", client.RemoteAddr())
		// One pipe closed, cancel context to stop the other
		cancel()
	}

	// Wait for both pipes to finish
	<-done
	log.Printf("[HANDLE] Both pipes finished for %s", client.RemoteAddr())

	// Close connections after pipes have finished
	client.Close()
	target.Close()

	log.Printf("[HANDLE] Finished handling connection from %s", client.RemoteAddr())
}

func (s *Server) pipe(ctx context.Context, src, dst net.Conn, label string, done chan struct{}) {
	buf := make([]byte, 32*1024)
	log.Printf("[PIPE DEBUG] Started pipe: %s", label)
	for {
		select {
		case <-ctx.Done():
			log.Printf("[PIPE DEBUG] Context cancelled, stopping pipe: %s", label)
			return
		default:
			src.SetReadDeadline(time.Now().Add(10 * time.Second))
			n, err := src.Read(buf)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					log.Printf("[PIPE DEBUG] Timeout on %s, continuing", label)
					continue
				}
				if err == io.EOF {
					log.Printf("[PIPE DEBUG] EOF on %s (connection closed by remote)", label)
				} else {
					log.Printf("[PIPE DEBUG] Read error on %s: %v", label, err)
				}
				// Signal that this pipe is closing
				select {
				case done <- struct{}{}:
					log.Printf("[PIPE DEBUG] Sent done signal for: %s", label)
				default:
				}
				return
			}
			if n == 0 {
				log.Printf("[PIPE DEBUG] Read 0 bytes on %s, continuing", label)
				continue
			}

			data := buf[:n]
			log.Printf("[PIPE DEBUG] Read %d bytes from %s: %q", n, label, string(data))

			// Try to parse CQL header if we have enough data
			if n >= 9 {
				version := data[0] & 0x7F
				// CQL Binary Protocol v3, v4, v5 are common.
				// Request version is 0x03-0x05, Response is 0x83-0x85
				if version >= 3 && version <= 5 {
					bodyLen := uint32(data[5])<<24 | uint32(data[6])<<16 | uint32(data[7])<<8 | uint32(data[8])
					opcode := data[4]
					if label == "client->target" {
						log.Printf("[AUDIT] CQL Request: Opcode=0x%x, Length=%d", opcode, bodyLen)
					}
				}
			}

			// Apply WASM transformations
			transformed := data
			if label == "client->target" {
				transformed, _ = s.transformRequest(data)
			} else {
				transformed, _ = s.transformResponse(data)
			}

			dst.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err = dst.Write(transformed)
			if err != nil {
				log.Printf("[PIPE DEBUG] Write error on %s: %v", label, err)
				// Signal that this pipe is closing
				select {
				case done <- struct{}{}:
					log.Printf("[PIPE DEBUG] Sent done signal after write error for: %s", label)
				default:
				}
				return
			}
			log.Printf("[PIPE DEBUG] Wrote %d bytes to %s", n, label)
		}
	}
}
