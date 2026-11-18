// Package ann wraps pinned hnswlib 0.8.0 with query-local ef and counters.
// Searches share a read lock; graph mutations take the exclusive lock.
package ann

/*
#cgo CXXFLAGS: -std=c++14 -O3
#cgo darwin LDFLAGS: -lc++
#cgo linux LDFLAGS: -lstdc++
#include <stdlib.h>
#include "bridge.h"
*/
import "C"
import (
	"errors"
	"os"
	"runtime"
	"unsafe"
)

type handle struct{ p unsafe.Pointer }
type Graph struct {
	h         *handle
	dimension int
}

// Vector copies an immutable node vector, including obsolete record versions.
// Patch expansion uses this lookup without evaluating any distances.
func (g *Graph) Vector(label uint64) ([]float32, error) {
	v := make([]float32, g.dimension)
	err := nativeError(C.ann_vector(g.h.p, C.uint64_t(label), (*C.float)(unsafe.Pointer(&v[0]))))
	runtime.KeepAlive(g.h)
	return v, err
}

func nativeError(p *C.char) error {
	if p == nil {
		return nil
	}
	defer C.free(unsafe.Pointer(p))
	return errors.New(C.GoString(p))
}
func New(d, m, ef int) (*Graph, error) {
	if d < 1 || d > 4096 || m < 2 || m > 64 || ef < m || ef > 4096 {
		return nil, errors.New("invalid graph configuration")
	}
	var err *C.char
	p := C.ann_new(C.int(d), C.int(m), C.int(ef), &err)
	if e := nativeError(err); e != nil {
		return nil, e
	}
	h := &handle{p: p}
	runtime.SetFinalizer(h, func(h *handle) { C.ann_free(h.p) })
	return &Graph{h: h, dimension: d}, nil
}
func (g *Graph) Add(id uint64, v []float32) error {
	if len(v) != g.dimension {
		return errors.New("invalid vector dimension")
	}
	e := nativeError(C.ann_add(g.h.p, C.uint64_t(id), (*C.float)(unsafe.Pointer(&v[0]))))
	runtime.KeepAlive(g.h)
	return e
}
func (g *Graph) Search(v []float32, k, ef int) ([]uint64, int, error) {
	if len(v) != g.dimension || k < 1 || k > 100000 || ef < 1 || ef > 100000 {
		return nil, 0, errors.New("invalid search arguments")
	}
	ids := make([]uint64, k)
	var n C.int
	var count C.uint64_t
	e := nativeError(C.ann_search(g.h.p, (*C.float)(unsafe.Pointer(&v[0])), C.int(k), C.int(ef), (*C.uint64_t)(unsafe.Pointer(&ids[0])), &n, &count))
	runtime.KeepAlive(g.h)
	if e != nil {
		return nil, 0, e
	}
	return ids[:int(n)], int(count), nil
}
func (g *Graph) Export() ([]byte, error) {
	f, e := os.CreateTemp("", "vector-hnsw-*.bin")
	if e != nil {
		return nil, e
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)
	p := C.CString(path)
	defer C.free(unsafe.Pointer(p))
	e = nativeError(C.ann_save(g.h.p, p))
	runtime.KeepAlive(g.h)
	if e != nil {
		return nil, e
	}
	return os.ReadFile(path)
}

// Import accepts only bytes validated by the owning vector snapshot decoder.
func (g *Graph) Import(data []byte) error {
	f, e := os.CreateTemp("", "vector-hnsw-load-*.bin")
	if e != nil {
		return e
	}
	path := f.Name()
	defer os.Remove(path)
	if _, e = f.Write(data); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	p := C.CString(path)
	defer C.free(unsafe.Pointer(p))
	e = nativeError(C.ann_load(g.h.p, p))
	runtime.KeepAlive(g.h)
	return e
}

// Close releases native memory promptly. The owner must exclude concurrent use;
// after Close, the graph must not be used again. Repeated Close is harmless.
func (g *Graph) Close() {
	if g.h.p != nil {
		runtime.SetFinalizer(g.h, nil)
		C.ann_free(g.h.p)
		g.h.p = nil
	}
}

// Patch capture requires the owning index's exclusive lock. FinishPatch rolls
// the native graph back to its base and returns a reversible graph mutation.
func (g *Graph) BeginPatch() error {
	e := nativeError(C.ann_patch_begin(g.h.p))
	runtime.KeepAlive(g.h)
	return e
}
func (g *Graph) FinishPatch() ([]byte, error) {
	var out *C.char
	var size C.uint64_t
	e := nativeError(C.ann_patch_finish(g.h.p, &out, &size))
	runtime.KeepAlive(g.h)
	if e != nil {
		return nil, e
	}
	defer C.free(unsafe.Pointer(out))
	if uint64(size) > 512<<20 {
		return nil, errors.New("graph patch too large")
	}
	return C.GoBytes(unsafe.Pointer(out), C.int(size)), nil
}
func (g *Graph) ApplyPatch(data []byte, reverse bool) error {
	if len(data) == 0 || len(data) > 512<<20 {
		return errors.New("invalid graph patch size")
	}
	direction := 0
	if reverse {
		direction = 1
	}
	e := nativeError(C.ann_patch_apply(g.h.p, (*C.char)(unsafe.Pointer(&data[0])), C.uint64_t(len(data)), C.int(direction)))
	runtime.KeepAlive(g.h)
	return e
}
func (g *Graph) Metadata() ([]byte, error) {
	var out *C.char
	var size C.uint64_t
	e := nativeError(C.ann_meta_save(g.h.p, &out, &size))
	runtime.KeepAlive(g.h)
	if e != nil {
		return nil, e
	}
	defer C.free(unsafe.Pointer(out))
	return C.GoBytes(unsafe.Pointer(out), C.int(size)), nil
}
func (g *Graph) RestoreMetadata(data []byte) error {
	if len(data) == 0 || len(data) > 8192 {
		return errors.New("invalid graph metadata")
	}
	e := nativeError(C.ann_meta_load(g.h.p, (*C.char)(unsafe.Pointer(&data[0])), C.uint64_t(len(data))))
	runtime.KeepAlive(g.h)
	return e
}
