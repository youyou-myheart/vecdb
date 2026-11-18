package ann

import (
	"encoding/binary"
	"errors"
	"math"
)

// CompactPatch removes vector bytes from both before and after node images.
// Adjacency, labels, geometry and RNG state remain byte-for-byte reversible.
func CompactPatch(data []byte, dimension int) ([]byte, error) {
	return transformPatch(data, dimension, nil)
}

// ExpandPatch restores vectors from immutable labels. It neither changes the
// graph nor computes neighbors. The owner must exclude graph mutations.
func ExpandPatch(data []byte, dimension int, vector func(uint64) ([]float32, error)) ([]byte, error) {
	if vector == nil {
		return nil, errors.New("missing patch vector resolver")
	}
	return transformPatch(data, dimension, vector)
}

func transformPatch(data []byte, dimension int, vector func(uint64) ([]float32, error)) ([]byte, error) {
	if dimension < 1 || dimension > 4096 || len(data) > 512<<20 {
		return nil, errors.New("invalid compact patch geometry")
	}
	r := patchReader{data: data}
	from, to := uint32(0x47504431), uint32(0x47504432)
	if vector != nil {
		from, to = to, from
	}
	if r.u32() != from {
		return nil, errors.New("unsupported compact patch")
	}
	out := binary.LittleEndian.AppendUint32(nil, to)
	start := r.data
	r.meta()
	stride, upper := r.meta()
	count := r.u64()
	if r.err != nil || stride < uint64(dimension*4+12) || upper < 4 || count > uint64(len(r.data))/44 {
		return nil, errors.New("invalid compact patch header")
	}
	out = append(out, start[:len(start)-len(r.data)]...)
	for n := uint64(0); n < count; n++ {
		out = binary.LittleEndian.AppendUint32(out, r.u32())
		for side := 0; side < 2; side++ {
			level, base, extra := r.image()
			if r.err != nil {
				return nil, r.err
			}
			if level == -1 {
				if len(base) != 0 || len(extra) != 0 {
					return nil, errors.New("invalid absent node image")
				}
			} else {
				want := stride
				if vector != nil {
					want -= uint64(dimension * 4)
				}
				if level < 0 || level > 64 || uint64(len(base)) != want || uint64(len(extra)) != uint64(level)*upper {
					return nil, errors.New("invalid compact node image")
				}
				cut := len(base) - 8
				if vector == nil {
					base = append(append([]byte(nil), base[:cut-dimension*4]...), base[cut:]...)
				} else {
					v, err := vector(binary.LittleEndian.Uint64(base[cut:]))
					if err != nil {
						return nil, err
					}
					if len(v) != dimension {
						return nil, errors.New("patch vector dimension mismatch")
					}
					full := append([]byte(nil), base[:cut]...)
					for _, f := range v {
						full = binary.LittleEndian.AppendUint32(full, math.Float32bits(f))
					}
					base = append(full, base[cut:]...)
				}
			}
			out = binary.LittleEndian.AppendUint32(out, uint32(level))
			out = binary.LittleEndian.AppendUint64(out, uint64(len(base)))
			out = append(out, base...)
			out = binary.LittleEndian.AppendUint64(out, uint64(len(extra)))
			out = append(out, extra...)
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	if len(r.data) != 0 {
		return nil, errors.New("trailing compact patch bytes")
	}
	return out, nil
}
