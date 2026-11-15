package vector

import (
	"encoding/binary"
	"errors"

	"math"
)

const graphFormat = "hnswlib-0.8.0-le64-v1"

type IndexSnapshot struct {
	GraphState    []byte                 `json:"graph_state,omitempty"`
	GraphSequence uint64                 `json:"graph_sequence,omitempty"`
	GraphHash     string                 `json:"graph_hash,omitempty"`
	LogVersion    int                    `json:"log_version,omitempty"`
	Offset        uint64                 `json:"offset,omitempty"`
	Format        string                 `json:"format"`
	Dimension     int                    `json:"dimension"`
	Config        IndexConfig            `json:"config"`
	Records       []Record               `json:"records"`
	Nodes         map[uint64]nodeVersion `json:"nodes"`
	Next          uint64                 `json:"next"`
	Graph         []byte                 `json:"graph"`
}

func (i *HNSWIndex) ExportState() (IndexSnapshot, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	data, err := i.graph.Export()
	if err != nil {
		return IndexSnapshot{}, err
	}
	meta, err := i.graph.Metadata()
	if err != nil {
		return IndexSnapshot{}, err
	}
	nodes := make(map[uint64]nodeVersion, len(i.nodes))
	for k, v := range i.nodes {
		nodes[k] = v
	}
	return IndexSnapshot{Format: graphFormat, Dimension: i.dimension, Config: i.config, Records: snapshotRecords(i.records), Nodes: nodes, Next: i.next, Graph: data, GraphState: meta}, nil
}
func RestoreHNSW(s IndexSnapshot) (*HNSWIndex, error) {
	if s.Format != graphFormat {
		return nil, errors.New("unsupported graph snapshot format")
	}
	i, err := NewHNSWIndex(s.Dimension, s.Config)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			i.graph.Close()
		}
	}()
	if err = validateBatch(s.Records, s.Dimension); err != nil {
		return nil, err
	}
	for _, r := range s.Records {
		if _, ok := i.records[r.ID]; ok {
			return nil, errors.New("duplicate snapshot record")
		}
		i.records[r.ID] = cloneRecord(r)
		i.postings.add(r)
		if !r.Deleted {
			i.live++
		}
	}
	active := map[string]int{}
	for key, meta := range s.Nodes {
		r, ok := i.records[meta.ID]
		if key == 0 || key > s.Next || !ok || meta.Revision == 0 || meta.Revision > r.Revision {
			return nil, errors.New("invalid graph node mapping")
		}
		if meta.Revision == r.Revision {
			if r.Deleted {
				return nil, errors.New("graph contains current deleted version")
			}
			active[r.ID]++
		}
	}
	for id, r := range i.records {
		if !r.Deleted && active[id] != 1 {
			return nil, errors.New("live record missing unique graph node")
		}
	}
	if err = validateGraphBytes(s, i.records); err != nil {
		return nil, err
	}
	if err = i.graph.Import(s.Graph); err != nil {
		return nil, err
	}
	if len(s.GraphState) > 0 {
		if err = i.graph.RestoreMetadata(s.GraphState); err != nil {
			return nil, err
		}
	}
	i.nodes = make(map[uint64]nodeVersion, len(s.Nodes))
	for k, v := range s.Nodes {
		i.nodes[k] = v
	}
	i.next = s.Next
	success = true
	return i, nil
}

// Bound and verify the pinned library format before its allocating decoder runs.
// This also checks cross-layer vectors and neighbor references; a checksum alone
// does not guarantee a structurally valid graph.
func validateGraphBytes(s IndexSnapshot, records map[string]Record) error {
	b := s.Graph
	if len(b) < 96 {
		return errors.New("truncated graph header")
	}
	u64 := func(off int) uint64 { return binary.LittleEndian.Uint64(b[off : off+8]) }
	count := u64(16)
	capacity := u64(8)
	stride := u64(24)
	labelOffset := u64(32)
	dataOffset := u64(40)
	m := uint64(s.Config.M)
	expectedOffset := 4 + 8*m
	expectedStride := expectedOffset + uint64(s.Dimension)*4 + 8
	maxLevel := int32(binary.LittleEndian.Uint32(b[48:52]))
	entry := uint64(binary.LittleEndian.Uint32(b[52:56]))
	if u64(0) != 0 || count != uint64(len(s.Nodes)) || capacity < count || capacity > max(256, count*2) || stride != expectedStride || dataOffset != expectedOffset || labelOffset != expectedStride-8 || u64(56) != m || u64(64) != 2*m || u64(72) != m || u64(88) != uint64(s.Config.EFConstruction) {
		return errors.New("graph header mismatch")
	}
	mult := math.Float64frombits(u64(80))
	if math.IsNaN(mult) || math.IsInf(mult, 0) || math.Abs(mult-1/math.Log(float64(m))) > 1e-12 {
		return errors.New("invalid graph level parameter")
	}
	if capacity*stride > 512<<20 || count*stride > uint64(len(b)-96) {
		return errors.New("graph allocation or length exceeds bounds")
	}
	if count == 0 {
		if maxLevel != -1 || entry != uint64(^uint32(0)) || len(b) != 96 {
			return errors.New("invalid empty graph")
		}
		return nil
	}
	if maxLevel < 0 || maxLevel > 64 || entry >= count {
		return errors.New("invalid entry point")
	}
	labels := map[uint64]bool{}
	checkLinks := func(data []byte, limit uint64) error {
		n := uint64(binary.LittleEndian.Uint32(data[:4]))
		if n > limit {
			return errors.New("invalid adjacency count/deletion flag")
		}
		seen := map[uint32]bool{}
		for k := uint64(0); k < n; k++ {
			id := binary.LittleEndian.Uint32(data[4+k*4 : 8+k*4])
			if uint64(id) >= count || seen[id] {
				return errors.New("invalid neighbor reference")
			}
			seen[id] = true
		}
		return nil
	}
	for j := uint64(0); j < count; j++ {
		off := 96 + j*stride
		row := b[off : off+stride]
		label := binary.LittleEndian.Uint64(row[labelOffset:])
		meta, ok := s.Nodes[label]
		if !ok || labels[label] {
			return errors.New("invalid graph node mapping")
		}
		labels[label] = true
		if err := checkLinks(row[:dataOffset], 2*m); err != nil {
			return err
		}
		rec := records[meta.ID]
		for k := 0; k < s.Dimension; k++ {
			x := math.Float32frombits(binary.LittleEndian.Uint32(row[dataOffset+uint64(k)*4:]))
			if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) || math.Abs(float64(x)) > 1.01e15 {
				return errors.New("invalid vector")
			}
			if !rec.Deleted && rec.Revision == meta.Revision && x != float32(rec.Vector[k]) {
				return errors.New("graph/record vector mismatch")
			}
		}
	}
	offset := int(96 + count*stride)
	levels := make([]int, count)
	upper := make([][]byte, count)
	linkSize := int(4 + 4*m)
	for j := range levels {
		if len(b)-offset < 4 {
			return errors.New("truncated graph layers")
		}
		size := int(binary.LittleEndian.Uint32(b[offset:]))
		offset += 4
		if size%linkSize != 0 || size/linkSize > 64 || size > len(b)-offset {
			return errors.New("invalid layer size")
		}
		levels[j] = size / linkSize
		upper[j] = b[offset : offset+size]
		offset += size
	}
	if offset != len(b) || levels[entry] != int(maxLevel) {
		return errors.New("graph length/entry level mismatch")
	}
	for j, data := range upper {
		if levels[j] > int(maxLevel) {
			return errors.New("invalid maximum level")
		}
		for level := 1; level <= levels[j]; level++ {
			row := data[(level-1)*linkSize : level*linkSize]
			if err := checkLinks(row, m); err != nil {
				return err
			}
			n := int(binary.LittleEndian.Uint32(row))
			for k := 0; k < n; k++ {
				id := binary.LittleEndian.Uint32(row[4+k*4:])
				if levels[id] < level {
					return errors.New("neighbor missing layer")
				}
			}
		}
	}
	return nil
}
