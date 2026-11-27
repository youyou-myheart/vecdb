package vector

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
)

// This frame exists only in the process-local, unlinked history cache. Durable
// WAL and object formats are unchanged. Keep JSON for metadata (including its
// string normalization), but avoid decimal conversion of every vector element.
const rawHistoryFrame = "VRH1"

var errRawHistoryFrame = errors.New("invalid raw history cache frame")

func appendRawHistoryEntry(dst []byte, entry mutationLogEntry) ([]byte, error) {
	vector := entry.Record.Vector
	for _, value := range vector {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return dst, errors.New("non-finite vector in raw history cache")
		}
	}
	entry.Record.Vector = nil
	metadata, err := json.Marshal(entry)
	if err != nil {
		return dst, err
	}
	// Locations use uint32 sizes, including the frame and vector bytes.
	size := uint64(8) + uint64(len(metadata)) + uint64(len(vector))*8
	if size > math.MaxUint32 {
		return dst, errors.New("raw history cache entry too large")
	}
	dst = append(dst, rawHistoryFrame...)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(metadata)))
	dst = append(dst, metadata...)
	for _, value := range vector {
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(value))
	}
	return dst, nil
}

func decodeRawHistoryEntry(data []byte) (mutationLogEntry, error) {
	var entry mutationLogEntry
	if len(data) < 8 || string(data[:4]) != rawHistoryFrame {
		return entry, errRawHistoryFrame
	}
	size := uint64(binary.LittleEndian.Uint32(data[4:8]))
	if size > uint64(len(data)-8) || (uint64(len(data)-8)-size)%8 != 0 {
		return entry, errRawHistoryFrame
	}
	end := 8 + int(size)
	if err := json.Unmarshal(data[8:end], &entry); err != nil {
		return mutationLogEntry{}, err
	}
	if entry.Record.Vector != nil {
		return mutationLogEntry{}, errRawHistoryFrame
	}
	if count := (len(data) - end) / 8; count != 0 {
		entry.Record.Vector = make([]float64, count)
		for i := range entry.Record.Vector {
			value := math.Float64frombits(binary.LittleEndian.Uint64(data[end+i*8:]))
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return mutationLogEntry{}, errRawHistoryFrame
			}
			entry.Record.Vector[i] = value
		}
	}
	return entry, nil
}
