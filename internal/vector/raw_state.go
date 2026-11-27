package vector

import (
	"errors"
	"os"
	"sort"
	"sync"
)

type rawShardHistory struct {
	mu         sync.RWMutex
	entries    []*mutationLogEntry
	locations  []rawHistoryLocation
	archive    *os.File
	archiveEnd int64
	spilled    int
	hotBytes   int
	records    map[string]rawRecordVersions
	receipts   map[string]uint64
}

type rawRecordVersions struct {
	positions []uint64
	revision  uint64
}

func newRawShardHistory() *rawShardHistory {
	return &rawShardHistory{records: make(map[string]rawRecordVersions), receipts: make(map[string]uint64)}
}

func (h *rawShardHistory) receipt(id string) (receipt, bool, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	seq := h.receipts[id]
	if seq == 0 {
		return receipt{}, false, nil
	}
	e, err := h.entryLocked(seq)
	if err != nil {
		return receipt{}, false, err
	}
	return receipt{Digest: e.Digest, Record: e.Record}, true, nil
}

func (h *rawShardHistory) revision(id string) uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.records[id].revision
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
				revision = h.records[record.ID].revision
			}
			if record.Revision == 0 || record.Revision != revision+1 {
				return errors.New("invalid raw batch record revision")
			}
			revisions[record.ID] = record.Revision
		}
		end = batch.last()
	}
	additionalEntries, additionalBytes := 0, 0
	for _, batch := range batches {
		for _, e := range batch.Entries {
			additionalEntries++
			additionalBytes += rawEntryBytes(e)
		}
	}
	if err := h.spillLocked(additionalEntries, additionalBytes); err != nil {
		return err
	}
	for _, batch := range batches {
		for n, entry := range batch.Entries {
			seq := batch.First + uint64(n)
			copy := entry
			h.entries = append(h.entries, &copy)
			h.locations = append(h.locations, rawHistoryLocation{})
			h.hotBytes += rawEntryBytes(entry)
			versions := h.records[entry.Record.ID]
			versions.positions = append(versions.positions, seq)
			versions.revision = entry.Record.Revision
			h.records[entry.Record.ID] = versions
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

func (s *rawShardSnapshot) entry(seq uint64) (mutationLogEntry, bool, error) {
	if seq <= s.floor || seq > s.end {
		return mutationLogEntry{}, false, nil
	}
	s.history.mu.RLock()
	defer s.history.mu.RUnlock()
	entry, err := s.history.entryLocked(seq)
	return entry, err == nil, err
}

func (s *rawShardSnapshot) record(id string) (Record, bool, error) {
	s.history.mu.RLock()
	defer s.history.mu.RUnlock()
	versions := s.history.records[id].positions
	n := sort.Search(len(versions), func(i int) bool { return versions[i] > s.end })
	if n == 0 {
		return Record{}, false, nil
	}
	entry, err := s.history.entryLocked(versions[n-1])
	return entry.Record, err == nil, err
}

func (s *rawShardSnapshot) recordIDs() []string {
	s.history.mu.RLock()
	defer s.history.mu.RUnlock()
	ids := make([]string, 0, len(s.history.records))
	for id, versions := range s.history.records {
		if versions.positions[0] <= s.end {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}
