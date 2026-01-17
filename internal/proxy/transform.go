package proxy

import (
	"bytes"
	"log"

	"github.com/falearn/scylla-proxy-wasm/internal/wasm"
)

type Transformer struct {
	engine *wasm.Engine
}

func NewTransformer(engine *wasm.Engine) *Transformer {
	return &Transformer{engine: engine}
}

func (t *Transformer) TransformRequest(data []byte) ([]byte, error) {
	// Apply masking to outgoing queries (client -> ScyllaDB)
	// For simplicity, use first loaded module
	modules := t.engine.ListModules()
	if len(modules) == 0 {
		return data, nil
	}
	module := modules[0]
	log.Printf("Transforming request with module %s", module)
	return t.engine.Transform(data, module)
}

func (t *Transformer) TransformResponse(data []byte) ([]byte, error) {
	// Apply unmasking to incoming responses (ScyllaDB -> client)
	// Could use a different module
	modules := t.engine.ListModules()
	if len(modules) == 0 {
		return data, nil
	}
	module := modules[0]
	log.Printf("Transforming response with module %s", module)
	return t.engine.Transform(data, module)
}

// Detect if data contains PII fields (simplified)
func containsPII(data []byte) bool {
	// Very naive detection
	keywords := [][]byte{
		[]byte("email"),
		[]byte("phone"),
		[]byte("ssn"),
		[]byte("password"),
	}
	for _, kw := range keywords {
		if bytes.Contains(data, kw) {
			return true
		}
	}
	return false
}
