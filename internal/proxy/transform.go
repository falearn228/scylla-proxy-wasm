package proxy

import (
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
