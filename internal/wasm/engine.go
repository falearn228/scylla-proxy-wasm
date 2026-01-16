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
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

type Engine struct {
	runtime wazero.Runtime
	modules map[string]wazero.CompiledModule
	mu      sync.RWMutex
}

func NewEngine() (*Engine, error) {
	runtime := wazero.NewRuntime(context.Background())
	_, err := wasi_snapshot_preview1.Instantiate(context.Background(), runtime)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate WASI: %w", err)
	}
	return &Engine{
		runtime: runtime,
		modules: make(map[string]wazero.CompiledModule),
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

	e.mu.Lock()
	defer e.mu.Unlock()
	e.modules[filepath.Base(path)] = compiled
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
	e.mu.RLock()
	compiled, ok := e.modules[moduleName]
	e.mu.RUnlock()
	if !ok {
		return data, fmt.Errorf("WASM module %s not found", moduleName)
	}

	ctx := context.Background()
	module, err := e.runtime.InstantiateModule(ctx, compiled, wazero.NewModuleConfig())
	if err != nil {
		return data, fmt.Errorf("failed to instantiate module: %w", err)
	}
	defer module.Close(ctx)

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
	return e.runtime.Close(context.Background())
}
