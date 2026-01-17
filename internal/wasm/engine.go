package wasm

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

type Engine struct {
	runtime  wazero.Runtime
	modules  map[string]wazero.CompiledModule
	pools    map[string]chan api.Module
	poolSize int
	mu       sync.RWMutex
}

func NewEngine(poolSize int) (*Engine, error) {
	runtime := wazero.NewRuntime(context.Background())
	_, err := wasi_snapshot_preview1.Instantiate(context.Background(), runtime)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate WASI: %w", err)
	}
	if poolSize < 0 {
		poolSize = 0
	}
	return &Engine{
		runtime:  runtime,
		modules:  make(map[string]wazero.CompiledModule),
		pools:    make(map[string]chan api.Module),
		poolSize: poolSize,
	}, nil
}

func (e *Engine) LoadModule(path string) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read WASM file %s: %w", path, err)
	}

	compiled, err := e.runtime.CompileModule(context.Background(), data)
	if err != nil {
		return fmt.Errorf("failed to compile WASM module %s: %w", path, err)
	}

	moduleName := filepath.Base(path)

	e.mu.Lock()
	e.modules[moduleName] = compiled
	if e.poolSize > 0 {
		pool := make(chan api.Module, e.poolSize)
		e.pools[moduleName] = pool
		e.mu.Unlock()
		for i := 0; i < e.poolSize; i++ {
			instance, err := e.instantiate(context.Background(), compiled)
			if err != nil {
				log.Printf("Failed to pre-instantiate module %s: %v", moduleName, err)
				break
			}
			pool <- instance
		}
	} else {
		e.mu.Unlock()
	}

	log.Printf("Loaded WASM module: %s", path)
	return nil
}

func (e *Engine) LoadDirectory(dir string) error {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(dir, 0755)
		}
		return err
	}

	for _, f := range files {
		if filepath.Ext(f.Name()) == ".wasm" {
			e.LoadModule(filepath.Join(dir, f.Name()))
		}
	}
	return nil
}

func (e *Engine) Transform(data []byte, moduleName string) ([]byte, error) {
	ctx := context.Background()
	module, release, err := e.acquireModule(ctx, moduleName)
	if err != nil {
		return data, err
	}
	defer release()

	transformFn := module.ExportedFunction("transform")
	if transformFn == nil {
		return data, fmt.Errorf("transform function not exported")
	}

	// Allocate memory for input
	malloc := module.ExportedFunction("malloc")
	free := module.ExportedFunction("free")
	if malloc == nil || free == nil {
		return data, fmt.Errorf("malloc/free not exported")
	}

	inputSize := uint64(len(data))
	inputPtr, err := malloc.Call(ctx, inputSize)
	if err != nil {
		return data, fmt.Errorf("malloc failed: %w", err)
	}
	defer free.Call(ctx, inputPtr[0])

	// Write data to memory
	if !module.Memory().Write(uint32(inputPtr[0]), data) {
		return data, fmt.Errorf("memory write failed")
	}

	// Call transform(inputPtr, inputLen) -> outputPtr
	result, err := transformFn.Call(ctx, inputPtr[0], uint64(inputSize))
	if err != nil {
		return data, fmt.Errorf("transform call failed: %w", err)
	}
	outputPtr := uint32(result[0])

	// Read output length (first 4 bytes)
	mem := module.Memory()
	lenBytes, ok := mem.Read(outputPtr, 4)
	if !ok {
		return data, fmt.Errorf("failed to read length")
	}
	length := uint32(lenBytes[0]) | uint32(lenBytes[1])<<8 | uint32(lenBytes[2])<<16 | uint32(lenBytes[3])<<24
	output, ok := mem.Read(outputPtr+4, length)
	if !ok {
		return data, fmt.Errorf("failed to read output")
	}

	return output, nil
}

func (e *Engine) ListModules() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	keys := make([]string, 0, len(e.modules))
	for k := range e.modules {
		keys = append(keys, k)
	}
	return keys
}

func (e *Engine) Close() error {
	e.mu.Lock()
	pools := e.pools
	e.pools = make(map[string]chan api.Module)
	e.mu.Unlock()

	for _, pool := range pools {
		close(pool)
		for module := range pool {
			module.Close(context.Background())
		}
	}
	return e.runtime.Close(context.Background())
}

func (e *Engine) instantiate(ctx context.Context, compiled wazero.CompiledModule) (api.Module, error) {
	return e.runtime.InstantiateModule(ctx, compiled, wazero.NewModuleConfig())
}

func (e *Engine) acquireModule(ctx context.Context, moduleName string) (api.Module, func(), error) {
	e.mu.RLock()
	compiled, ok := e.modules[moduleName]
	pool := e.pools[moduleName]
	e.mu.RUnlock()
	if !ok {
		return nil, func() {}, fmt.Errorf("WASM module %s not found", moduleName)
	}

	if pool != nil {
		select {
		case module := <-pool:
			return module, func() {
				e.releaseModule(ctx, moduleName, module)
			}, nil
		default:
		}
	}

	module, err := e.instantiate(ctx, compiled)
	if err != nil {
		return nil, func() {}, fmt.Errorf("failed to instantiate module: %w", err)
	}

	return module, func() {
		module.Close(ctx)
	}, nil
}

func (e *Engine) releaseModule(ctx context.Context, moduleName string, module api.Module) {
	e.mu.RLock()
	pool := e.pools[moduleName]
	e.mu.RUnlock()
	if pool == nil {
		module.Close(ctx)
		return
	}
	defer func() {
		if recover() != nil {
			module.Close(ctx)
		}
	}()
	select {
	case pool <- module:
	default:
		module.Close(ctx)
	}
}
