package zstd

/*
#define ZSTD_STATIC_LINKING_ONLY
#include "zstd.h"
#include "stdint.h"  // for uintptr_t

// The following *_wrapper function are used for removing superfluous
// memory allocations when calling the wrapped functions from Go code.
// See https://github.com/golang/go/issues/24450 for details.

static size_t ZSTD_compressCCtx_wrapper(ZSTD_CCtx* cctx, uintptr_t dst, size_t maxDstSize, const uintptr_t src, size_t srcSize, int compressionLevel) {
	return ZSTD_compressCCtx(cctx, (void*)dst, maxDstSize, (const void*)src, srcSize, compressionLevel);
}

static size_t ZSTD_decompressDCtx_wrapper(ZSTD_DCtx* dctx, uintptr_t dst, size_t maxDstSize, uintptr_t src, size_t srcSize) {
	return ZSTD_decompressDCtx(dctx, (void*)dst, maxDstSize, (const void *)src, srcSize);
}

*/
import "C"
import (
	"bytes"
	"io/ioutil"
	"runtime"
	"unsafe"
)

type Ctx interface {
	// Compress src into dst.  If you have a buffer to use, you can pass it to
	// prevent allocation.  If it is too small, or if nil is passed, a new buffer
	// will be allocated and returned.
	Compress(dst, src []byte) ([]byte, error)

	// CompressLevel is the same as Compress but you can pass a compression level
	CompressLevel(dst, src []byte, level int) ([]byte, error)

	// Decompress src into dst.  If you have a buffer to use, you can pass it to
	// prevent allocation.  If it is too small, or if nil is passed, a new buffer
	// will be allocated and returned.
	Decompress(dst, src []byte) ([]byte, error)
}

type ctx struct {
	cctx *C.ZSTD_CCtx
	dctx *C.ZSTD_DCtx
}

// Create a new ZStd Context.
//  When compressing/decompressing many times, it is recommended to allocate a
//  context just once, and re-use it for each successive compression operation.
//  This will make workload friendlier for system's memory.
//  Note : re-using context is just a speed / resource optimization.
//         It doesn't change the compression ratio, which remains identical.
//  Note 2 : In multi-threaded environments,
//         use one different context per thread for parallel execution.
//
func NewCtx() Ctx {
	c := &ctx{
		cctx: C.ZSTD_createCCtx(),
		dctx: C.ZSTD_createDCtx(),
	}

	runtime.SetFinalizer(c, finalizeCtx)
	return c
}

func (c *ctx) Compress(dst, src []byte) ([]byte, error) {
	return c.CompressLevel(dst, src, DefaultCompression)
}

func (c *ctx) CompressLevel(dst, src []byte, level int) ([]byte, error) {
	bound := CompressBound(len(src))
	if cap(dst) >= bound {
		dst = dst[0:bound] // Reuse dst buffer
	} else {
		dst = make([]byte, bound)
	}

	srcPtr := C.uintptr_t(uintptr(0)) // Do not point anywhere, if src is empty
	if len(src) > 0 {
		srcPtr = C.uintptr_t(uintptr(unsafe.Pointer(&src[0])))
	}

	cWritten := C.ZSTD_compressCCtx_wrapper(
		c.cctx,
		C.uintptr_t(uintptr(unsafe.Pointer(&dst[0]))),
		C.size_t(len(dst)),
		srcPtr,
		C.size_t(len(src)),
		C.int(level))

	runtime.KeepAlive(src)
	written := int(cWritten)
	// Check if the return is an Error code
	if err := getError(written); err != nil {
		return nil, err
	}
	return dst[:written], nil
}


func (c *ctx) Decompress(dst, src []byte) ([]byte, error) {
	if len(src) == 0 {
		return []byte{}, ErrEmptySlice
	}
	decompress := func(dst, src []byte) ([]byte, error) {

		cWritten := C.ZSTD_decompressDCtx_wrapper(
			c.dctx,
			C.uintptr_t(uintptr(unsafe.Pointer(&dst[0]))),
			C.size_t(len(dst)),
			C.uintptr_t(uintptr(unsafe.Pointer(&src[0]))),
			C.size_t(len(src)))

		runtime.KeepAlive(src)
		written := int(cWritten)
		// Check error
		if err := getError(written); err != nil {
			return nil, err
		}
		return dst[:written], nil
	}

	if len(dst) == 0 {
		// Attempt to use zStd to determine decompressed size (may result in error or 0)
		size := int(C.size_t(C.ZSTD_getDecompressedSize(unsafe.Pointer(&src[0]), C.size_t(len(src)))))

		if err := getError(size); err != nil {
			return nil, err
		}

		if size > 0 {
			dst = make([]byte, size)
		} else {
			dst = make([]byte, len(src)*3) // starting guess
		}
	}
	for i := 0; i < 3; i++ { // 3 tries to allocate a bigger buffer
		result, err := decompress(dst, src)
		if !IsDstSizeTooSmallError(err) {
			return result, err
		}
		dst = make([]byte, len(dst)*2) // Grow buffer by 2
	}

	// We failed getting a dst buffer of correct size, use stream API
	r := NewReader(bytes.NewReader(src))
	defer r.Close()
	return ioutil.ReadAll(r)
}

func finalizeCtx(c *ctx) {
	C.ZSTD_freeCCtx(c.cctx)
	C.ZSTD_freeDCtx(c.dctx)
}
