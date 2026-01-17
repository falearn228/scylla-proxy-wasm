package proxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/falearn/scylla-proxy-wasm/internal/config"
	"github.com/falearn/scylla-proxy-wasm/internal/metrics"
	"github.com/falearn/scylla-proxy-wasm/internal/wasm"
)

type Server struct {
	cfg             *config.Config
	listener        net.Listener
	listenerReady   chan struct{}
	metricsListener net.Listener
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	engine          *wasm.Engine
	preparedCache   *PreparedStatementCache
	metrics         *metrics.Metrics
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
	// Start hot reload watcher if enabled
	if cfg.HotReloadEnabled {
		interval := cfg.HotReloadInterval
		if interval == 0 {
			interval = 5 * time.Second
		}
		if err := engine.StartWatcher(cfg.WASMDir, interval); err != nil {
			log.Printf("Warning: failed to start WASM watcher: %v", err)
		}
	}
	srv := &Server{
		cfg:           cfg,
		ctx:           ctx,
		cancel:        cancel,
		engine:        engine,
		preparedCache: NewPreparedStatementCache(),
		metrics:       metrics.New(),
		listenerReady: make(chan struct{}),
	}
	return srv, nil
}

func (s *Server) Start() error {
	// Start health server
	go s.startHealthServer()

	// Start metrics server if enabled
	if s.cfg.MetricsEnabled {
		metricsAddr := fmt.Sprintf(":%d", s.cfg.MetricsPort)
		if s.cfg.MetricsPort == 0 {
			metricsAddr = ":9090"
		}
		go func() {
			http.Handle("/metrics", metrics.Handler())
			log.Printf("Metrics server listening on %s", metricsAddr)
			if err := http.ListenAndServe(metricsAddr, nil); err != nil && err != http.ErrServerClosed {
				log.Printf("Metrics server error: %v", err)
			}
		}()
	}

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
	// Signal that listener is ready
	close(s.listenerReady)

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
			s.metrics.IncActiveConnections()
			go s.handleConnection(conn)
		}
	}
}

// startHealthServer starts the health check HTTP server
func (s *Server) startHealthServer() {
	healthPort := s.cfg.HealthPort
	if healthPort == 0 {
		healthPort = 8080
	}
	addr := fmt.Sprintf(":%d", healthPort)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/ready", s.handleReady)

	srv := &http.Server{
		Addr: addr,
	}

	go func() {
		log.Printf("Health server listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Health server error: %v", err)
		}
	}()

	// Wait for context cancellation
	<-s.ctx.Done()
	srv.Shutdown(context.Background())
}

// handleHealthz is the liveness probe endpoint
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// handleReady is the readiness probe endpoint
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	// Check if listener is ready
	select {
	case <-s.listenerReady:
		// Listener is ready
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Ready"))
	default:
		// Listener not ready yet
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("Not Ready"))
	}
}

func (s *Server) transformRequest(data []byte) ([]byte, error) {
	start := time.Now()
	defer func() {
		s.metrics.RecordProxyLatency("request", time.Since(start))
	}()

	if s.engine == nil {
		s.metrics.RecordRequest("request", "success")
		return data, nil
	}
	modules := s.engine.ListModules()
	if len(modules) == 0 {
		s.metrics.RecordRequest("request", "success")
		return data, nil
	}
	result, err := s.engine.Transform(data, modules[0])
	if err != nil {
		s.metrics.RecordRequest("request", "error")
		return data, err
	}
	s.metrics.RecordRequest("request", "success")
	s.metrics.RecordWASMExecution("transform", time.Since(start))
	return result, nil
}

func (s *Server) transformResponse(data []byte) ([]byte, error) {
	start := time.Now()
	defer func() {
		s.metrics.RecordProxyLatency("response", time.Since(start))
	}()

	if s.engine == nil {
		s.metrics.RecordRequest("response", "success")
		return data, nil
	}
	modules := s.engine.ListModules()
	if len(modules) == 0 {
		s.metrics.RecordRequest("response", "success")
		return data, nil
	}
	result, err := s.engine.Transform(data, modules[0])
	if err != nil {
		s.metrics.RecordRequest("response", "error")
		return data, err
	}
	s.metrics.RecordRequest("response", "success")
	s.metrics.RecordWASMExecution("transform", time.Since(start))
	return result, nil
}

// MaskValue calls WASM mask_value(column_name, value) for PII masking
func (s *Server) MaskValue(columnName, value string) (string, error) {
	start := time.Now()
	defer func() {
		s.metrics.RecordWASMExecution("mask_value", time.Since(start))
	}()

	if s.engine == nil {
		return value, nil
	}
	modules := s.engine.ListModules()
	if len(modules) == 0 {
		return value, nil
	}
	result, err := s.engine.MaskValue(columnName, value, modules[0])
	if err != nil {
		return value, err
	}
	if result != value {
		s.metrics.RecordMaskedValue("pii")
		s.metrics.RecordMaskedBytes("value", len(value)-len(result))
	}
	return result, nil
}

func (s *Server) Stop() {
	s.cancel()
	if s.listener != nil {
		s.listener.Close()
	}
	// Stop WASM watcher
	if s.engine != nil {
		s.engine.StopWatcher()
	}
	s.wg.Wait()
}

func (s *Server) handleConnection(client net.Conn) {
	s.wg.Add(1)
	defer s.wg.Done()
	defer func() {
		client.Close()
		s.metrics.DecActiveConnections()
	}()

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

// bufferedPipe holds a buffer for accumulating TCP data until we have complete CQL messages
type bufferedPipe struct {
	src           net.Conn
	dst           net.Conn
	label         string
	buffer        []byte
	server        *Server
	preparedCache *PreparedStatementCache
	done          chan struct{}
}

func newBufferedPipe(src, dst net.Conn, label string, server *Server, done chan struct{}) *bufferedPipe {
	return &bufferedPipe{
		src:           src,
		dst:           dst,
		label:         label,
		buffer:        make([]byte, 0, 32*1024),
		server:        server,
		preparedCache: server.preparedCache,
		done:          done,
	}
}

// isCQLProtocol checks if the first byte looks like CQL binary protocol
// CQL v3/v4/v5: requests 0x03-0x05, responses 0x83-0x85
// First byte format: [version (7 bits)][direction (1 bit: 0=request, 1=response)]
func isCQLProtocol(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	version := data[0] & 0x7F
	// Valid CQL versions are 3, 4, or 5
	return version >= 3 && version <= 5
}

func (bp *bufferedPipe) process() {
	readBuf := make([]byte, 32*1024)
	log.Printf("[PIPE DEBUG] Started buffered pipe: %s", bp.label)

	for {
		select {
		case <-bp.server.ctx.Done():
			log.Printf("[PIPE DEBUG] Context cancelled, stopping buffered pipe: %s", bp.label)
			return
		default:
			bp.src.SetReadDeadline(time.Now().Add(10 * time.Second))
			n, err := bp.src.Read(readBuf)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					log.Printf("[PIPE DEBUG] Timeout on %s, continuing", bp.label)
					continue
				}
				if err == io.EOF {
					log.Printf("[PIPE DEBUG] EOF on %s (connection closed by remote)", bp.label)
				} else {
					log.Printf("[PIPE DEBUG] Read error on %s: %v", bp.label, err)
				}
				select {
				case bp.done <- struct{}{}:
					log.Printf("[PIPE DEBUG] Sent done signal for: %s", bp.label)
				default:
				}
				return
			}
			if n == 0 {
				log.Printf("[PIPE DEBUG] Read 0 bytes on %s, continuing", bp.label)
				continue
			}

			data := readBuf[:n]
			log.Printf("[PIPE DEBUG] Read %d bytes from %s: %q", n, bp.label, string(data))

			// If data doesn't look like CQL protocol, pass through immediately
			if !isCQLProtocol(data) {
				log.Printf("[PIPE DEBUG] Non-CQL data detected, passing through immediately")
				var transformed []byte
				if bp.label == "client->target" {
					transformed, _ = bp.server.transformRequest(data)
				} else {
					transformed, _ = bp.server.transformResponse(data)
				}

				bp.dst.SetWriteDeadline(time.Now().Add(10 * time.Second))
				_, err = bp.dst.Write(transformed)
				if err != nil {
					log.Printf("[PIPE DEBUG] Write error on %s: %v", bp.label, err)
					select {
					case bp.done <- struct{}{}:
						log.Printf("[PIPE DEBUG] Sent done signal after write error for: %s", bp.label)
					default:
					}
					return
				}
				log.Printf("[PIPE DEBUG] Wrote %d bytes to %s (passthrough)", len(transformed), bp.label)
				continue
			}

			// Append CQL data to buffer
			bp.buffer = append(bp.buffer, data...)
			log.Printf("[PIPE DEBUG] Buffer now has %d bytes from %s", len(bp.buffer), bp.label)

			// Process complete CQL messages from buffer
			for {
			completeMsg:
				for len(bp.buffer) >= 9 {
					version := bp.buffer[0] & 0x7F
					// CQL Binary Protocol v3, v4, v5 are common.
					// Request version is 0x03-0x05, Response is 0x83-0x85
					if version >= 3 && version <= 5 {
						bodyLen := uint32(bp.buffer[5])<<24 | uint32(bp.buffer[6])<<16 | uint32(bp.buffer[7])<<8 | uint32(bp.buffer[8])
						opcode := bp.buffer[4]

						totalMsgLen := 9 + bodyLen
						log.Printf("[PIPE DEBUG] CQL message: header=9, bodyLen=%d, total=%d, buffer=%d", bodyLen, totalMsgLen, len(bp.buffer))

						// Check if we have the complete message
						if int(totalMsgLen) > len(bp.buffer) {
							// Not enough data yet, wait for more
							log.Printf("[PIPE DEBUG] Incomplete message, waiting for more data (have %d, need %d)", len(bp.buffer), totalMsgLen)
							break completeMsg
						}

						// Extract complete message
						msg := make([]byte, totalMsgLen)
						copy(msg, bp.buffer[:totalMsgLen])
						bp.buffer = bp.buffer[totalMsgLen:]

						// Parse CQL message for detailed logging
						cqlMsg, err := ParseCQLMessage(msg)
						if err != nil {
							log.Printf("[CQL] Failed to parse message: %v", err)
						} else {
							// Handle prepared statement caching
							if bp.label == "client->target" {
								switch cqlMsg.Header.Opcode {
								case OpcodePrepare:
									// Store pending prepared statement by stream ID
									if cqlMsg.Query != "" {
										bp.preparedCache.AddPendingPrepared(cqlMsg.Header.Stream, cqlMsg.Query)
										log.Printf("[PREPARE] Client PREPARE for stream %d: %q", cqlMsg.Header.Stream, cqlMsg.Query)
									}
								case OpcodeQuery:
									log.Printf("[AUDIT] CQL QUERY: %q", cqlMsg.Query)
									piiIdx := cqlMsg.FindPIIParamIndex()
									if piiIdx >= 0 {
										log.Printf("[AUDIT] CQL QUERY: PII parameter detected at index %d", piiIdx)
									}
								case OpcodeExecute:
									log.Printf("[AUDIT] CQL EXECUTE: %d parameters", len(cqlMsg.Params))
									// Get the original query from cache for PII detection
									if cqlMsg.PreparedID != nil {
										cachedQuery := bp.preparedCache.GetPreparedQuery(cqlMsg.PreparedID)
										if cachedQuery != "" {
											cqlMsg.Query = cachedQuery
										}
									}
									piiIdx := cqlMsg.FindPIIParamIndex()
									if piiIdx >= 0 {
										log.Printf("[AUDIT] CQL EXECUTE: PII parameter detected at index %d", piiIdx)
									}
								default:
									log.Printf("[AUDIT] CQL Request: Opcode=0x%x, Length=%d", opcode, bodyLen)
								}
							} else {
								// Response direction (target->client)
								switch cqlMsg.Header.Opcode {
								case OpcodeResult:
									// Check if this is a RESULT_PREPARED response
									if len(cqlMsg.PreparedID) > 0 {
										// Complete the prepared statement mapping
										query := bp.preparedCache.CompletePrepared(cqlMsg.Header.Stream, cqlMsg.PreparedID)
										log.Printf("[PREPARE] RESULT_PREPARED for stream %d: ID=%x -> %q", cqlMsg.Header.Stream, cqlMsg.PreparedID, query)
									} else if len(cqlMsg.Columns) > 0 {
										log.Printf("[AUDIT] CQL RESULT ROWS: %d columns, %d rows", len(cqlMsg.Columns), len(cqlMsg.Rows))
										emailColIdx := cqlMsg.findEmailColumn()
										if emailColIdx >= 0 {
											log.Printf("[AUDIT] CQL RESULT: Email column '%s' at index %d", cqlMsg.Columns[emailColIdx].Name, emailColIdx)
										}
									}
								default:
									log.Printf("[AUDIT] CQL Response: Opcode=0x%x, Length=%d", opcode, bodyLen)
								}
							}
						}

						// Apply PII masking using mask_value(column_name, value)
						var transformed []byte
						if cqlMsg != nil {
							// Use MaskPIIValues which calls mask_value for each PII value
							maskedMsg, wasMasked := cqlMsg.MaskPIIValues(bp.server.MaskValue)
							if wasMasked {
								transformed = maskedMsg
								log.Printf("[PIPE] Applied PII masking to CQL message")
							} else {
								transformed = msg
							}
						} else {
							// Fallback to original transform if parsing failed
							if bp.label == "client->target" {
								transformed, _ = bp.server.transformRequest(msg)
							} else {
								transformed, _ = bp.server.transformResponse(msg)
							}
						}

						// Write transformed data
						bp.dst.SetWriteDeadline(time.Now().Add(10 * time.Second))
						_, err = bp.dst.Write(transformed)
						if err != nil {
							log.Printf("[PIPE DEBUG] Write error on %s: %v", bp.label, err)
							select {
							case bp.done <- struct{}{}:
								log.Printf("[PIPE DEBUG] Sent done signal after write error for: %s", bp.label)
							default:
							}
							return
						}
						log.Printf("[PIPE DEBUG] Wrote %d bytes to %s (CQL message)", len(transformed), bp.label)
					} else {
						// Invalid version, skip this byte and try again
						log.Printf("[PIPE DEBUG] Invalid CQL version 0x%x, skipping byte", version)
						bp.buffer = bp.buffer[1:]
					}
				}
				// No more complete messages in buffer
				break
			}
		}
	}
}

func (s *Server) pipe(ctx context.Context, src, dst net.Conn, label string, done chan struct{}) {
	bp := newBufferedPipe(src, dst, label, s, done)
	bp.process()
}
