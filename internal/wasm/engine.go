package wasm

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

type Engine struct {
	runtime      wazero.Runtime
	modules      map[string]wazero.CompiledModule
	pools        map[string]chan api.Module
	poolSize     int
	mu           sync.RWMutex
	watchDir     string
	watchEnabled bool
	watchDone    chan struct{}
	watchTicker  *time.Ticker
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
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read WASM file %s: %w", path, err)
	}

	compiled, err := e.runtime.CompileModule(context.Background(), data)
	if err != nil {
		return fmt.Errorf("failed to compile WASM module %s: %w", path, err)
	}

	moduleName := filepath.Base(path)

	e.mu.Lock()
	// Close old compiled module if exists
	if oldCompiled, ok := e.modules[moduleName]; ok {
		oldCompiled.Close(context.Background())
	}
	// Close old pool if exists
	if oldPool, ok := e.pools[moduleName]; ok {
		close(oldPool)
		for module := range oldPool {
			module.Close(context.Background())
		}
	}
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

func (e *Engine) ReloadModule(path string) error {
	moduleName := filepath.Base(path)
	log.Printf("[HOT RELOAD] Reloading WASM module: %s", moduleName)

	// Read new file
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read WASM file %s: %w", path, err)
	}

	// Compile new module
	compiled, err := e.runtime.CompileModule(context.Background(), data)
	if err != nil {
		return fmt.Errorf("failed to compile WASM module %s: %w", path, err)
	}

	e.mu.Lock()
	// Close old compiled module
	if oldCompiled, ok := e.modules[moduleName]; ok {
		oldCompiled.Close(context.Background())
	}
	// Close old pool instances
	if oldPool, ok := e.pools[moduleName]; ok {
		close(oldPool)
		for module := range oldPool {
			module.Close(context.Background())
		}
	}

	e.modules[moduleName] = compiled
	if e.poolSize > 0 {
		pool := make(chan api.Module, e.poolSize)
		e.pools[moduleName] = pool
		// Keep mutex locked during instantiation
		for i := 0; i < e.poolSize; i++ {
			instance, err := e.instantiate(context.Background(), compiled)
			if err != nil {
				log.Printf("Failed to pre-instantiate module %s: %v", moduleName, err)
				break
			}
			pool <- instance
		}
	}
	e.mu.Unlock()

	log.Printf("[HOT RELOAD] Successfully reloaded WASM module: %s", moduleName)
	return nil
}

func (e *Engine) StartWatcher(dir string, interval time.Duration) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return fmt.Errorf("watch directory does not exist: %s", dir)
	}

	e.mu.Lock()
	e.watchDir = dir
	e.watchEnabled = true
	e.watchDone = make(chan struct{})
	e.watchTicker = time.NewTicker(interval)
	e.mu.Unlock()

	go e.watchLoop()
	log.Printf("[HOT RELOAD] Started WASM watcher on directory: %s (interval: %v)", dir, interval)
	return nil
}

func (e *Engine) StopWatcher() {
	e.mu.Lock()
	if e.watchEnabled {
		e.watchEnabled = false
		close(e.watchDone)
		if e.watchTicker != nil {
			e.watchTicker.Stop()
		}
	}
	e.mu.Unlock()
	log.Printf("[HOT RELOAD] Stopped WASM watcher")
}

func (e *Engine) watchLoop() {
	// Track file modification times
	modTimes := make(map[string]time.Time)

	for {
		select {
		case <-e.watchDone:
			return
		case <-e.watchTicker.C:
			e.checkForChanges(modTimes)
		}
	}
}

func (e *Engine) checkForChanges(modTimes map[string]time.Time) {
	files, err := ioutil.ReadDir(e.watchDir)
	if err != nil {
		log.Printf("[HOT RELOAD] Error reading watch directory: %v", err)
		return
	}

	for _, f := range files {
		if filepath.Ext(f.Name()) != ".wasm" {
			continue
		}

		path := filepath.Join(e.watchDir, f.Name())
		modTime := f.ModTime()

		// Check if module already loaded
		moduleName := f.Name()
		e.mu.RLock()
		_, alreadyLoaded := e.modules[moduleName]
		e.mu.RUnlock()

		if lastMod, ok := modTimes[path]; ok {
			if modTime.After(lastMod) {
				if alreadyLoaded {
					log.Printf("[HOT RELOAD] Detected change in: %s", f.Name())
					if err := e.ReloadModule(path); err != nil {
						log.Printf("[HOT RELOAD] Failed to reload module %s: %v", f.Name(), err)
					} else {
						modTimes[path] = modTime
					}
				} else {
					modTimes[path] = modTime
				}
			}
		} else {
			// New file - only load if not already loaded
			if !alreadyLoaded {
				modTimes[path] = modTime
				log.Printf("[HOT RELOAD] Detected new WASM file: %s", f.Name())
				if err := e.LoadModule(path); err != nil {
					log.Printf("[HOT RELOAD] Failed to load new module %s: %v", f.Name(), err)
				}
			} else {
				// Already loaded, just track mod time
				modTimes[path] = modTime
			}
		}
	}
}

func (e *Engine) LoadDirectory(dir string) error {
	files, err := os.ReadDir(dir)
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

// MaskValue calls WASM function mask_value(column_name, value) -> masked_value
func (e *Engine) MaskValue(columnName, value string, moduleName string) (string, error) {
	if len(value) == 0 {
		return value, nil
	}

	ctx := context.Background()
	module, release, err := e.acquireModule(ctx, moduleName)
	if err != nil {
		return value, err
	}
	defer release()

	maskFn := module.ExportedFunction("mask_value")
	if maskFn == nil {
		// If mask_value is not exported, return original value
		log.Printf("[WASM] mask_value not exported, returning original value")
		return value, nil
	}

	// Allocate memory for column_name
	malloc := module.ExportedFunction("malloc")
	free := module.ExportedFunction("free")
	if malloc == nil || free == nil {
		return value, fmt.Errorf("malloc/free not exported")
	}

	// Write column_name to memory
	colNamePtr, err := malloc.Call(ctx, uint64(len(columnName)))
	if err != nil {
		return value, fmt.Errorf("malloc for column_name failed: %w", err)
	}
	defer free.Call(ctx, colNamePtr[0])

	if !module.Memory().Write(uint32(colNamePtr[0]), []byte(columnName)) {
		return value, fmt.Errorf("memory write for column_name failed")
	}

	// Write value to memory
	valuePtr, err := malloc.Call(ctx, uint64(len(value)))
	if err != nil {
		return value, fmt.Errorf("malloc for value failed: %w", err)
	}
	defer free.Call(ctx, valuePtr[0])

	if !module.Memory().Write(uint32(valuePtr[0]), []byte(value)) {
		return value, fmt.Errorf("memory write for value failed")
	}

	// Call mask_value(colNamePtr, colNameLen, valuePtr, valueLen) -> outputPtr
	result, err := maskFn.Call(ctx, colNamePtr[0], uint64(len(columnName)), valuePtr[0], uint64(len(value)))
	if err != nil {
		return value, fmt.Errorf("mask_value call failed: %w", err)
	}
	outputPtr := uint32(result[0])

	// Read output length (first 4 bytes)
	mem := module.Memory()
	lenBytes, ok := mem.Read(outputPtr, 4)
	if !ok {
		return value, fmt.Errorf("failed to read masked length")
	}
	length := uint32(lenBytes[0]) | uint32(lenBytes[1])<<8 | uint32(lenBytes[2])<<16 | uint32(lenBytes[3])<<24
	masked, ok := mem.Read(outputPtr+4, length)
	if !ok {
		return value, fmt.Errorf("failed to read masked value")
	}

	log.Printf("[WASM] Masked %s value: %q -> %q", columnName, value, string(masked))
	return string(masked), nil
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
