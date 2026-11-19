package vector

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

var ErrInvalidList = errors.New("invalid list options or cursor")

type ListOptions struct {
	Limit          int
	Cursor         string
	IncludeDeleted bool
}
type RecordPage struct {
	Records    []Record `json:"records"`
	NextCursor string   `json:"next_cursor,omitempty"`
}
type recordCursor struct {
	Version   int    `json:"v"`
	Scope     string `json:"s"`
	Partition int    `json:"p"`
	After     string `json:"a"`
	Deleted   bool   `json:"d"`
}

func (p *PartitionedService) ListRecords(ctx context.Context, opts ListOptions) (RecordPage, error) {
	repos := make([]*Repository, p.layout.DataPartitions)
	for i := range repos {
		repos[i] = p.data[DataPartitionID(i)]
	}
	return listRecords(ctx, repos, opts)
}
func (r *Repository) ListRecords(ctx context.Context, opts ListOptions) (RecordPage, error) {
	return listRecords(ctx, []*Repository{r}, opts)
}

// Pages follow (data partition, ID bytes), not vector distance or creation time.
// One transaction provides a consistent read within a page. Subsequent pages
// use fresh transactions and therefore do not form an export snapshot.
func listRecords(ctx context.Context, repos []*Repository, opts ListOptions) (RecordPage, error) {
	page := RecordPage{Records: []Record{}}
	if opts.Limit < 1 || opts.Limit > 1000 || len(opts.Cursor) > 2048 || len(repos) == 0 {
		return page, ErrInvalidList
	}
	scopeInput := ""
	for _, r := range repos {
		if r == nil {
			return page, errors.New("missing data partition")
		}
		scopeInput += r.Prefix + "\x00"
	}
	scope := fmt.Sprintf("%x", sha256.Sum256([]byte(scopeInput)))
	cur := recordCursor{Version: 1, Scope: scope, Deleted: opts.IncludeDeleted}
	if opts.Cursor != "" {
		raw, e := base64.RawURLEncoding.DecodeString(opts.Cursor)
		if e != nil {
			return page, ErrInvalidList
		}
		if e = json.Unmarshal(raw, &cur); e != nil {
			return page, ErrInvalidList
		}
		id, e := hex.DecodeString(cur.After)
		if e != nil || len(id) > 256 || cur.Version != 1 || cur.Scope != scope || cur.Deleted != opts.IncludeDeleted || cur.Partition < 0 || cur.Partition >= len(repos) {
			return page, ErrInvalidList
		}
	}
	tx, e := repos[0].beginDataRead(ctx)
	if e != nil {
		return page, e
	}
	defer tx.Rollback()
	scanned := 0
	for ; cur.Partition < len(repos); cur.Partition++ {
		if e = ctx.Err(); e != nil {
			return RecordPage{}, e
		}
		prefix := repos[cur.Partition].Prefix + "record/"
		start := []byte(prefix)
		if cur.After != "" {
			start = []byte(prefix + cur.After + "\x00")
		}
		it, err := tx.Iter(start, []byte(repos[cur.Partition].Prefix+"record0"))
		if err != nil {
			return RecordPage{}, err
		}
		for it.Valid() {
			if e = ctx.Err(); e != nil {
				it.Close()
				return RecordPage{}, e
			}
			var record Record
			if e = json.Unmarshal(it.Value(), &record); e != nil {
				it.Close()
				return RecordPage{}, e
			}
			cur.After = hex.EncodeToString([]byte(record.ID))
			scanned++
			if !record.Deleted || opts.IncludeDeleted {
				page.Records = append(page.Records, record)
			}
			// Bound work even if the entire range consists of tombstones.
			if len(page.Records) == opts.Limit || scanned == 1000 {
				it.Close()
				raw, _ := json.Marshal(cur)
				page.NextCursor = base64.RawURLEncoding.EncodeToString(raw)
				return page, nil
			}
			if e = it.Next(); e != nil {
				it.Close()
				return RecordPage{}, e
			}
		}
		it.Close()
		cur.After = ""
	}
	return page, nil
}
