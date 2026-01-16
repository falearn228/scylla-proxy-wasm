package proxy

import (
	"context"
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
	cfg        *config.Config
	listener   net.Listener
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	engine     *wasm.Engine
}

func New(cfg *config.Config) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())
	engine, err := wasm.NewEngine()
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
	listener, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.cfg.ListenAddr, err)
	}
	s.listener = listener
	log.Printf("Proxy listening on %s", s.cfg.ListenAddr)

	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:
			conn, err := listener.Accept()
			if err != nil {
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

	target, err := net.DialTimeout("tcp", s.cfg.TargetAddr, 5*time.Second)
	if err != nil {
		log.Printf("Failed to connect to target %s: %v", s.cfg.TargetAddr, err)
		return
	}
	defer target.Close()

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	go s.pipe(ctx, client, target, "client->target")
	go s.pipe(ctx, target, client, "target->client")

	<-ctx.Done()
}

func (s *Server) pipe(ctx context.Context, src, dst net.Conn, label string) {
	buf := make([]byte, 4096)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			src.SetReadDeadline(time.Now().Add(10 * time.Second))
			n, err := src.Read(buf)
			if err != nil {
				if err != io.EOF {
					log.Printf("Read error (%s): %v", label, err)
				}
				return
			}
			if n == 0 {
				continue
			}
			// Apply WASM transformations here
			data := buf[:n]
			transformed := data
			if label == "client->target" {
				// Request transformation
				transformed, _ = s.transformRequest(data)
			} else {
				// Response transformation
				transformed, _ = s.transformResponse(data)
			}
			dst.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err = dst.Write(transformed)
			if err != nil {
				log.Printf("Write error (%s): %v", label, err)
				return
			}
		}
	}
}