package vector

import (
	"errors"
	"sort"
	"sync"
)

type rawShardHistory struct {
	mu       sync.RWMutex
	entries  []mutationLogEntry
	records  map[string][]uint64
	receipts map[string]uint64
}

func newRawShardHistory() *rawShardHistory {
	return &rawShardHistory{records: make(map[string][]uint64), receipts: make(map[string]uint64)}
}

func (h *rawShardHistory) receipt(id string) (receipt, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	seq := h.receipts[id]
	if seq == 0 {
		return receipt{}, false
	}
	e := h.entries[seq-1]
	return receipt{Digest: e.Digest, Record: e.Record}, true
}

func (h *rawShardHistory) revision(id string) uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	versions := h.records[id]
	if len(versions) == 0 {
		return 0
	}
	return h.entries[versions[len(versions)-1]-1].Record.Revision
}

func (h *rawShardHistory) end() uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return uint64(len(h.entries))
}

func (h *rawShardHistory) extend(batches []mutationLogBatch) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Validate the entire extension before changing any pinned history.
	end := uint64(len(h.entries))
	revisions := make(map[string]uint64)
	requests := make(map[string]struct{})
	for _, batch := range batches {
		if len(batch.Entries) == 0 || len(batch.Entries) > mutationGroupRecords || batch.First != end+1 || batch.last() < batch.First {
			return errors.New("invalid raw batch log range")
		}
		for _, entry := range batch.Entries {
			record := entry.Record
			if record.ID == "" || entry.RequestID == "" || entry.Digest == "" || h.receipts[entry.RequestID] != 0 {
				return errors.New("invalid raw batch receipt")
			}
			if _, found := requests[entry.RequestID]; found {
				return errors.New("duplicate raw batch receipt")
			}
			requests[entry.RequestID] = struct{}{}
			revision := revisions[record.ID]
			if revision == 0 {
				if previous := h.records[record.ID]; len(previous) != 0 {
					revision = h.entries[previous[len(previous)-1]-1].Record.Revision
				}
			}
			if record.Revision == 0 || record.Revision != revision+1 {
				return errors.New("invalid raw batch record revision")
			}
			revisions[record.ID] = record.Revision
		}
		end = batch.last()
	}
	for _, batch := range batches {
		for n, entry := range batch.Entries {
			seq := batch.First + uint64(n)
			h.entries = append(h.entries, entry)
			h.records[entry.Record.ID] = append(h.records[entry.Record.ID], seq)
			h.receipts[entry.RequestID] = seq
		}
	}
	return nil
}

type rawShardSnapshot struct {
	history *rawShardHistory
	end     uint64
	floor   uint64
}

func (s *rawShardSnapshot) entry(seq uint64) (mutationLogEntry, bool) {
	if seq <= s.floor || seq > s.end {
		return mutationLogEntry{}, false
	}
	s.history.mu.RLock()
	defer s.history.mu.RUnlock()
	return s.history.entries[seq-1], true
}

func (s *rawShardSnapshot) record(id string) (Record, bool) {
	s.history.mu.RLock()
	defer s.history.mu.RUnlock()
	versions := s.history.records[id]
	n := sort.Search(len(versions), func(i int) bool { return versions[i] > s.end })
	if n == 0 {
		return Record{}, false
	}
	return s.history.entries[versions[n-1]-1].Record, true
}

func (s *rawShardSnapshot) recordIDs() []string {
	s.history.mu.RLock()
	defer s.history.mu.RUnlock()
	ids := make([]string, 0, len(s.history.records))
	for id, versions := range s.history.records {
		if versions[0] <= s.end {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}
