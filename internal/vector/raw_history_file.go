package vector

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// These targets apply to decoded log contents, not the ID/position indexes.
// A single incoming batch may exceed the byte target; it is spilled before
// the next batch. Spill in blocks to avoid a file write for every append.
const rawHistoryHotEntries = 4096
const rawHistoryHotBytes = 8 << 20

type rawHistoryLocation struct {
	offset   int64
	size     uint32
	checksum uint32
}

func rawEntryBytes(e mutationLogEntry) int {
	n := 192 + len(e.RequestID) + len(e.Digest) + len(e.Record.ID) + len(e.Record.Vector)*8
	for k, v := range e.Record.Payload {
		n += 48 + len(k) + len(v)
	}
	return n
}

// The file is a rebuildable cache. TiKV's WAL remains the recovery source.
// Unlink immediately: process exit closes the file without orphaned cache
// files. While snapshots reference the history, its *os.File stays reachable;
// the standard library closes the descriptor when the history is collected.
func (h *rawShardHistory) spillLocked(incomingEntries, incomingBytes int) error {
	for h.spilled < len(h.entries) && (len(h.entries)-h.spilled+incomingEntries > rawHistoryHotEntries || h.hotBytes+incomingBytes > rawHistoryHotBytes) {
		if h.archive == nil {
			file, err := os.CreateTemp("", "vecdb-raw-history-*")
			if err != nil {
				return fmt.Errorf("create raw history cache: %w", err)
			}
			if err = os.Remove(file.Name()); err != nil {
				_ = file.Close()
				return fmt.Errorf("unlink raw history cache: %w", err)
			}
			h.archive = file
		}
		var data []byte
		var locations []rawHistoryLocation
		released := 0
		for i := h.spilled; i < len(h.entries) && len(locations) < 512; i++ {
			start := len(data)
			var err error
			data, err = appendRawHistoryEntry(data, *h.entries[i])
			if err != nil {
				return err
			}
			encoded := data[start:]
			locations = append(locations, rawHistoryLocation{offset: h.archiveEnd + int64(start), size: uint32(len(encoded)), checksum: crc32.ChecksumIEEE(encoded)})
			released += rawEntryBytes(*h.entries[i])
			if len(data) >= 8<<20 {
				break
			}
		}
		n, err := h.archive.WriteAt(data, h.archiveEnd)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
		if err != nil {
			return fmt.Errorf("write raw history cache: %w", err)
		}
		// Publish positions and release contents only after the complete write.
		copy(h.locations[h.spilled:], locations)
		clear(h.entries[h.spilled : h.spilled+len(locations)])
		h.spilled += len(locations)
		h.archiveEnd += int64(len(data))
		h.hotBytes -= released
	}
	return nil
}

// Caller holds h.mu. Reads never interpret an I/O failure as a missing record.
func (h *rawShardHistory) entryLocked(seq uint64) (mutationLogEntry, error) {
	if e := h.entries[seq-1]; e != nil {
		return *e, nil
	}
	loc := h.locations[seq-1]
	data := make([]byte, int(loc.size))
	if _, err := h.archive.ReadAt(data, loc.offset); err != nil {
		return mutationLogEntry{}, fmt.Errorf("read raw history cache: %w", err)
	}
	if crc32.ChecksumIEEE(data) != loc.checksum {
		return mutationLogEntry{}, fmt.Errorf("raw history cache checksum mismatch at %d", seq)
	}
	return decodeRawHistoryEntry(data)
}
