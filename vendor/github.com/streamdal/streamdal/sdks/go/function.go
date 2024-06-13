package streamdal

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/streamdal/streamdal/libs/protos/build/go/protos"
)

type function struct {
	ID      string
	Inst    api.Module
	entry   api.Function
	alloc   api.Function
	dealloc api.Function
	mtx     *sync.Mutex
}

func (f *function) Exec(ctx context.Context, req []byte) ([]byte, error) {
	ptrLen := uint64(len(req))

	inputPtr, err := f.alloc.Call(ctx, ptrLen)
	if err != nil {
		return nil, errors.Wrap(err, "unable to allocate memory")
	}

	if len(inputPtr) == 0 {
		return nil, errors.New("unable to allocate memory")
	}

	ptrVal := inputPtr[0]

	if !f.Inst.Memory().Write(uint32(ptrVal), req) {
		return nil, fmt.Errorf("Memory.Write(%d, %d) out of range of memory size %d",
			ptrVal, len(req), f.Inst.Memory().Size())
	}

	result, err := f.entry.Call(ctx, ptrVal, ptrLen)
	if err != nil {
		// Clear mem on error
		if _, err := f.dealloc.Call(ctx, ptrVal, ptrLen); err != nil {
			return nil, errors.Wrap(err, "unable to deallocate memory")
		}
		return nil, errors.Wrap(err, "error during func call")
	}

	resultPtr := uint32(result[0] >> 32)
	resultSize := uint32(result[0])

	// Dealloc request memory
	if _, err := f.dealloc.Call(ctx, ptrVal, ptrLen); err != nil {
		return nil, errors.Wrap(err, "unable to deallocate memory")
	}

	// Read memory starting from result ptr
	resBytes, err := f.readMemory(resultPtr, resultSize)
	if err != nil {
		// Dealloc response memory
		if _, err := f.dealloc.Call(ctx, uint64(resultPtr), uint64(resultSize)); err != nil {
			return nil, errors.Wrap(err, "unable to deallocate memory")
		}
		return nil, errors.Wrap(err, "unable to read memory")
	}

	// Dealloc response memory
	if _, err := f.dealloc.Call(ctx, uint64(resultPtr), uint64(resultSize)); err != nil {
		return nil, errors.Wrap(err, "unable to deallocate memory")
	}

	return resBytes, nil
}

func (s *Streamdal) setFunctionCache(wasmID string, f *function, workerID int) {
	s.functionsMtx.Lock()
	defer s.functionsMtx.Unlock()

	if _, ok := s.functions[workerID]; !ok {
		s.functions[workerID] = make(map[string]*function)
	}

	s.functions[workerID][wasmID] = f
}

func (s *Streamdal) getFunction(_ context.Context, step *protos.PipelineStep, workerID int) (*function, error) {
	// check cache
	fc, ok := s.getFunctionFromCache(step.GetXWasmId(), workerID)
	if ok {
		return fc, nil
	}

	// Function is not in cache - let's create it but make sure that the creation
	// is locked for this specific wasm ID - that way another .Process() call
	// will wait for the create to finish.
	wasmIDMtx := s.getLockForWasmID(step.GetXWasmId())

	wasmIDMtx.Lock()
	defer wasmIDMtx.Unlock()

	fi, err := s.createFunction(step)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create function")
	}

	// Cache function
	s.setFunctionCache(step.GetXWasmId(), fi, workerID)

	return fi, nil
}

func (s *Streamdal) getLockForWasmID(wasmID string) *sync.Mutex {
	s.funcCreateMtx.Lock()
	defer s.funcCreateMtx.Unlock()

	if mtx, ok := s.funcCreate[wasmID]; ok {
		return mtx
	}

	// No existing lock found for wasm ID - create it
	s.funcCreate[wasmID] = &sync.Mutex{}

	return s.funcCreate[wasmID]
}

func (s *Streamdal) getFunctionFromCache(wasmID string, workerID int) (*function, bool) {
	// We need to do this here because there is a possibility that .Process()
	// was called for the first time in parallel and the function has not been
	// created yet. We need a mechanism to wait for the function creation to
	// complete before we perform a cache lookup.
	wasmIDMtx := s.getLockForWasmID(wasmID)

	// If this blocks, it is because createFunction() is in the process of
	// creating a function. Once it complete, the lock will be released and
	// our cache lookup will succeed.
	wasmIDMtx.Lock()
	wasmIDMtx.Unlock()

	s.functionsMtx.RLock()
	defer s.functionsMtx.RUnlock()

	if _, ok := s.functions[workerID]; !ok {
		return nil, false
	}

	f, ok := s.functions[workerID][wasmID]
	return f, ok
}

func (s *Streamdal) createFunction(step *protos.PipelineStep) (*function, error) {
	inst, err := s.createWASMInstance(step)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create WASM instance")
	}

	// This is the actual function we'll be executing
	f := inst.ExportedFunction(step.GetXWasmFunction())
	if f == nil {
		return nil, fmt.Errorf("unable to get exported function '%s'", step.GetXWasmFunction())
	}

	// alloc allows us to pre-allocate memory in order to pass data to the WASM module
	alloc := inst.ExportedFunction("alloc")
	if alloc == nil {
		return nil, errors.New("unable to get alloc func")
	}

	// dealloc allows us to free memory passed to the wasm module after we're done with it
	dealloc := inst.ExportedFunction("dealloc")
	if dealloc == nil {
		return nil, errors.New("unable to get dealloc func")
	}

	return &function{
		ID:      step.GetXWasmId(),
		Inst:    inst,
		entry:   f,
		alloc:   alloc,
		dealloc: dealloc,
		mtx:     &sync.Mutex{},
	}, nil
}

func (s *Streamdal) getWasmBytesCache(funcID string) ([]byte, bool) {
	s.wasmCacheMtx.RLock()
	defer s.wasmCacheMtx.RUnlock()

	wb, ok := s.wasmCache[funcID]
	return wb, ok
}

func (s *Streamdal) setWasmBytesCache(funcID string, wb []byte) {
	s.wasmCacheMtx.Lock()
	defer s.wasmCacheMtx.Unlock()

	s.wasmCache[funcID] = wb
}

func (s *Streamdal) createWASMInstance(step *protos.PipelineStep) (api.Module, error) {
	// We need to cache wasm bytes so that we can instantiate the module
	// When running in async mode, createWASMInstance will be hit multiple times, but we need to wipe the wasmBytes
	// from the pipeline step after the first run, so that we don't hold multiple MB of duplicate data in memory
	wasmBytes, ok := s.getWasmBytesCache(step.GetXWasmId())
	if !ok {
		// Not cached yet, check if it's in the step
		stepWasmBytes := step.GetXWasmBytes()
		if len(stepWasmBytes) == 0 {
			// WASM bytes are not in cache or step, error out
			return nil, errors.New("wasm data is empty")
		}

		// Cache the bytes so we can wipe from the step
		s.setWasmBytesCache(step.GetXWasmId(), stepWasmBytes)
		wasmBytes = stepWasmBytes
	}

	hostFuncs := map[string]func(_ context.Context, module api.Module, ptr, length int32) uint64{
		"kvExists":    s.hf.KVExists,
		"httpRequest": s.hf.HTTPRequest,
	}

	var rCfg wazero.RuntimeConfig

	switch s.config.WazeroExecutionMode {
	case WazeroExecutionModeCompiler:
		rCfg = wazero.NewRuntimeConfig().
			WithMemoryLimitPages(1000). // (1 page = 64KB, 1000 pages = ~62MB)
			WithCompilationCache(s.wazeroCache)
	case WazeroExecutionModeInterpreter:
		rCfg = wazero.NewRuntimeConfigInterpreter().
			WithMemoryLimitPages(1000)
	default:
		return nil, errors.New("invalid wazero execution mode")
	}

	ctx := context.Background()
	r := wazero.NewRuntimeWithConfig(ctx, rCfg)

	wasi_snapshot_preview1.MustInstantiate(ctx, r)

	stdoutOutput := io.Discard
	stderrOutput := io.Discard

	if s.config != nil && s.config.EnableStdout {
		stdoutOutput = os.Stdout
	}

	if s.config != nil && s.config.EnableStderr {
		stderrOutput = os.Stderr
	}

	cfg := wazero.NewModuleConfig().
		WithStderr(stderrOutput).
		WithStdout(stdoutOutput).
		WithSysNanotime().
		WithSysNanosleep().
		WithSysWalltime().
		WithStartFunctions("") // We don't need _start() to be called for our purposes

	builder := r.NewHostModuleBuilder("env")

	// This is how multiple host funcs are exported:
	// https://github.com/tetratelabs/wazero/blob/b7e8191cceb83c7335d6b8922b40b957475beecf/examples/import-go/age-calculator.go#L41
	for name, fn := range hostFuncs {
		builder = builder.NewFunctionBuilder().
			WithFunc(fn).
			Export(name)
	}

	if _, err := builder.Instantiate(ctx); err != nil {
		return nil, errors.Wrap(err, "failed to instantiate module")
	}

	mod, err := r.InstantiateWithConfig(ctx, wasmBytes, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to instantiate wasm module")
	}

	return mod, nil
}

func (f *function) readMemory(ptr, length uint32) ([]byte, error) {
	mem, ok := f.Inst.Memory().Read(ptr, length)
	if !ok {
		return nil, fmt.Errorf("unable to read memory at '%d' with length '%d'", ptr, length)
	}

	return mem, nil

}
