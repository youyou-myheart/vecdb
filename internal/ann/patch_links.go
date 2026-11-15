package ann

import (
	"encoding/binary"
	"errors"
)

// NodeLinks is a final adjacency image. Neighbor IDs are internal graph IDs;
// Label joins the graph delta's Nodes mapping to record ID and revision.
// Layers[0] is the base layer. A reset snapshot starts a new ID space.
type NodeLinks struct {
	InternalID uint32     `json:"internal_id"`
	Label      uint64     `json:"label"`
	Layers     [][]uint32 `json:"layers"`
}
type patchReader struct {
	data []byte
	err  error
}

func (r *patchReader) take(n uint64) []byte {
	if r.err != nil {
		return nil
	}
	if n > uint64(len(r.data)) {
		r.err = errors.New("truncated graph patch")
		return nil
	}
	b := r.data[:n]
	r.data = r.data[n:]
	return b
}
func (r *patchReader) u32() uint32 {
	b := r.take(4)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint32(b)
}
func (r *patchReader) u64() uint64 {
	b := r.take(8)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}
func (r *patchReader) str() []byte { return r.take(r.u64()) }
func (r *patchReader) meta() (uint64, uint64) {
	r.u64()
	r.u64()
	stride, upper := r.u64(), r.u64()
	r.u32()
	r.u32()
	r.str()
	return stride, upper
}
func (r *patchReader) image() (int32, []byte, []byte) {
	level := int32(r.u32())
	base, upper := r.str(), r.str()
	return level, base, upper
}

// PatchLinks decodes only adjacency metadata; it never computes distances.
func PatchLinks(data []byte) ([]NodeLinks, error) {
	r := patchReader{data: data}
	if r.u32() != 0x47504431 {
		return nil, errors.New("unsupported graph patch")
	}
	r.meta()
	stride, upper := r.meta()
	count := r.u64()
	if r.err != nil || stride < 12 || upper < 4 || count > uint64(len(r.data))/44 {
		return nil, errors.New("invalid graph patch geometry")
	}
	out := make([]NodeLinks, 0, int(count))
	for n := uint64(0); n < count; n++ {
		id := r.u32()
		r.image()
		level, base, extra := r.image()
		if r.err != nil {
			return nil, r.err
		}
		if level < 0 || level > 64 || uint64(len(base)) != stride || uint64(len(extra)) != uint64(level)*upper {
			return nil, errors.New("invalid graph node image")
		}
		row := NodeLinks{InternalID: id, Label: binary.LittleEndian.Uint64(base[len(base)-8:]), Layers: make([][]uint32, level+1)}
		for layer := 0; layer <= int(level); layer++ {
			b := base
			if layer > 0 {
				b = extra[uint64(layer-1)*upper : uint64(layer)*upper]
			}
			edges := uint64(binary.LittleEndian.Uint32(b))
			if edges > (uint64(len(b))-4)/4 {
				return nil, errors.New("invalid graph adjacency")
			}
			row.Layers[layer] = make([]uint32, int(edges))
			for k := range row.Layers[layer] {
				row.Layers[layer][k] = binary.LittleEndian.Uint32(b[4+k*4:])
			}
		}
		out = append(out, row)
	}
	if r.err != nil {
		return nil, r.err
	}
	if len(r.data) != 0 {
		return nil, errors.New("trailing graph patch bytes")
	}
	return out, nil
}
