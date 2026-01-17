package metrics

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	// Request counters
	RequestsTotal     *prometheus.CounterVec
	RequestsProcessed *prometheus.CounterVec

	// Data masking stats
	MaskedValuesTotal *prometheus.CounterVec
	MaskedBytesTotal  *prometheus.CounterVec

	// WASM engine latency
	WASMExecutionDuration *prometheus.HistogramVec

	// Proxy latency
	ProxyLatency *prometheus.HistogramVec

	// Active connections
	ActiveConnections prometheus.Gauge

	mu sync.RWMutex
}

var (
	instance *Metrics
	once     sync.Once
)

func New() *Metrics {
	once.Do(func() {
		instance = &Metrics{
			RequestsTotal: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: "scylla_proxy_requests_total",
					Help: "Total number of requests proxied",
				},
				[]string{"direction", "status"},
			),
			RequestsProcessed: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: "scylla_proxy_requests_processed_total",
					Help: "Total number of requests processed by WASM",
				},
				[]string{"type"},
			),
			MaskedValuesTotal: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: "scylla_proxy_masked_values_total",
					Help: "Total number of values masked by WASM",
				},
				[]string{"column_type"},
			),
			MaskedBytesTotal: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: "scylla_proxy_masked_bytes_total",
					Help: "Total number of bytes masked",
				},
				[]string{"type"},
			),
			WASMExecutionDuration: prometheus.NewHistogramVec(
				prometheus.HistogramOpts{
					Name:    "scylla_proxy_wasm_execution_duration_seconds",
					Help:    "WASM execution duration in seconds",
					Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
				},
				[]string{"operation"},
			),
			ProxyLatency: prometheus.NewHistogramVec(
				prometheus.HistogramOpts{
					Name:    "scylla_proxy_latency_seconds",
					Help:    "Proxy request latency in seconds",
					Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
				},
				[]string{"direction"},
			),
			ActiveConnections: prometheus.NewGauge(
				prometheus.GaugeOpts{
					Name: "scylla_proxy_active_connections",
					Help: "Number of active connections",
				},
			),
		}

		prometheus.MustRegister(
			instance.RequestsTotal,
			instance.RequestsProcessed,
			instance.MaskedValuesTotal,
			instance.MaskedBytesTotal,
			instance.WASMExecutionDuration,
			instance.ProxyLatency,
			instance.ActiveConnections,
		)
	})
	return instance
}

// RecordRequest records a proxied request
func (m *Metrics) RecordRequest(direction, status string) {
	m.RequestsTotal.WithLabelValues(direction, status).Inc()
}

// RecordWASMExecution records WASM execution time
func (m *Metrics) RecordWASMExecution(operation string, duration time.Duration) {
	m.WASMExecutionDuration.WithLabelValues(operation).Observe(duration.Seconds())
}

// RecordMaskedValue records a masked value
func (m *Metrics) RecordMaskedValue(columnType string) {
	m.MaskedValuesTotal.WithLabelValues(columnType).Inc()
}

// RecordMaskedBytes records masked bytes
func (m *Metrics) RecordMaskedBytes(dataType string, bytes int) {
	m.MaskedBytesTotal.WithLabelValues(dataType).Add(float64(bytes))
}

// RecordProxyLatency records proxy latency
func (m *Metrics) RecordProxyLatency(direction string, duration time.Duration) {
	m.ProxyLatency.WithLabelValues(direction).Observe(duration.Seconds())
}

// IncActiveConnections increments active connections gauge
func (m *Metrics) IncActiveConnections() {
	m.ActiveConnections.Inc()
}

// DecActiveConnections decrements active connections gauge
func (m *Metrics) DecActiveConnections() {
	m.ActiveConnections.Dec()
}

// Handler returns the Prometheus HTTP handler
func Handler() http.Handler {
	return promhttp.Handler()
}
