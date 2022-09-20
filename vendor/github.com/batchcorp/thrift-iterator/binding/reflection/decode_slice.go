package reflection

import (
	"unsafe"
	"github.com/batchcorp/thrift-iterator/spi"
	"reflect"
)

type sliceDecoder struct {
	sliceType   reflect.Type
	elemType    reflect.Type
	elemDecoder internalDecoder
}

func (decoder *sliceDecoder) decode(ptr unsafe.Pointer, iter spi.Iterator) {
	slice := (*sliceHeader)(ptr)
	slice.Len = 0
	offset := uintptr(0)
	_, length := iter.ReadListHeader()

	if slice.Cap < length {
		newVal := reflect.MakeSlice(decoder.sliceType, 0, length)
		slice.Data = unsafe.Pointer(newVal.Pointer())
		slice.Cap = length
	}

	for i := 0; i < length; i++ {
		decoder.elemDecoder.decode(unsafe.Pointer(uintptr(slice.Data)+offset), iter)
		offset += decoder.elemType.Size()
		slice.Len += 1
	}
}

// grow grows the slice s so that it can hold extra more values, allocating
// more capacity if needed. It also returns the old and new slice lengths.
func growOne(slice *sliceHeader, sliceType reflect.Type, elementType reflect.Type) {
	newLen := slice.Len + 1
	if newLen <= slice.Cap {
		slice.Len = newLen
		return
	}
	newCap := slice.Cap
	if newCap == 0 {
		newCap = 1
	} else {
		for newCap < newLen {
			if slice.Len < 1024 {
				newCap += newCap
			} else {
				newCap += newCap / 4
			}
		}
	}
	newVal := reflect.MakeSlice(sliceType, newLen, newCap)
	dst := unsafe.Pointer(newVal.Pointer())
	// copy old array into new array
	originalBytesCount := slice.Len * int(elementType.Size())
	srcSliceHeader := (unsafe.Pointer)(&sliceHeader{slice.Data, originalBytesCount, originalBytesCount})
	dstSliceHeader := (unsafe.Pointer)(&sliceHeader{dst, originalBytesCount, originalBytesCount})
	copy(*(*[]byte)(dstSliceHeader), *(*[]byte)(srcSliceHeader))
	slice.Data = dst
	slice.Len = newLen
	slice.Cap = newCap
}