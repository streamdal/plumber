package streamdal

import (
	"context"
	"fmt"
	"io"
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

func (s *Streamdal) setFunctionCache(wasmID string, f *function) {
	s.functionsMtx.Lock()
	defer s.functionsMtx.Unlock()

	s.functions[wasmID] = f
}

func (s *Streamdal) getFunction(_ context.Context, step *protos.PipelineStep) (*function, error) {
	// check cache
	fc, ok := s.getFunctionFromCache(step.GetXWasmId())
	if ok {
		return fc, nil
	}

	fi, err := s.createFunction(step)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create function")
	}

	// Cache function
	s.setFunctionCache(step.GetXWasmId(), fi)

	return fi, nil
}

func (s *Streamdal) getFunctionFromCache(wasmID string) (*function, bool) {
	s.functionsMtx.RLock()
	defer s.functionsMtx.RUnlock()

	f, ok := s.functions[wasmID]
	return f, ok
}

func (s *Streamdal) createFunction(step *protos.PipelineStep) (*function, error) {
	inst, err := s.createWASMInstance(step.GetXWasmBytes())
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

func (s *Streamdal) createWASMInstance(wasmBytes []byte) (api.Module, error) {
	if len(wasmBytes) == 0 {
		return nil, errors.New("wasm data is empty")
	}

	hostFuncs := map[string]func(_ context.Context, module api.Module, ptr, length int32) uint64{
		"kvExists":    s.hf.KVExists,
		"httpRequest": s.hf.HTTPRequest,
	}

	rCfg := wazero.NewRuntimeConfig().
		WithMemoryLimitPages(1000) // 64MB (default is 1MB)

	ctx := context.Background()
	r := wazero.NewRuntimeWithConfig(ctx, rCfg)

	wasi_snapshot_preview1.MustInstantiate(ctx, r)

	cfg := wazero.NewModuleConfig().
		WithStderr(io.Discard).
		WithStdout(io.Discard).
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
