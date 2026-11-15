package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestListPaginationPartitionsAndScope(t *testing.T) {
	root, db := newFakeRepo()
	layout, _ := newLayout(3)
	p := &PartitionedService{layout: layout, data: map[DataPartitionID]*Repository{}}
	for i := 0; i < 3; i++ {
		r := *root
		r.Prefix = fmt.Sprintf("test/data/%d/", i)
		p.data[DataPartitionID(i)] = &r
		for _, id := range []string{"a", "aa", "b", "中"} {
			b, _ := json.Marshal(Record{ID: id, Revision: 1, Vector: []float64{1, 2}, Deleted: id == "b"})
			db.data[string(r.key("record", id))] = b
		}
	}
	var ids []string
	cursor := ""
	pages := 0
	for {
		page, e := p.ListRecords(context.Background(), ListOptions{Limit: 2, Cursor: cursor})
		if e != nil {
			t.Fatal(e)
		}
		for _, r := range page.Records {
			ids = append(ids, r.ID)
			if r.Deleted {
				t.Fatal("tombstone leaked")
			}
		}
		pages++
		if pages > 10 {
			t.Fatal("pagination loop")
		}
		cursor = page.NextCursor
		if cursor == "" {
			break
		}
	}
	want := []string{"a", "aa", "中", "a", "aa", "中", "a", "aa", "中"}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("got %v", ids)
	}
	page, e := p.ListRecords(context.Background(), ListOptions{Limit: 1})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = p.ListRecords(context.Background(), ListOptions{Limit: 1, Cursor: page.NextCursor, IncludeDeleted: true}); !errors.Is(e, ErrInvalidList) {
		t.Fatal(e)
	}
	if _, e = root.ListRecords(context.Background(), ListOptions{Limit: 1, Cursor: page.NextCursor}); !errors.Is(e, ErrInvalidList) {
		t.Fatal(e)
	}
	all, e := p.ListRecords(context.Background(), ListOptions{Limit: 100, IncludeDeleted: true})
	if e != nil || len(all.Records) != 12 {
		t.Fatalf("%v %v", all, e)
	}
}
func TestListTombstoneBudgetAndErrors(t *testing.T) {
	r, db := newFakeRepo()
	for i := 0; i < 1001; i++ {
		id := fmt.Sprintf("%04d", i)
		b, _ := json.Marshal(Record{ID: id, Deleted: i < 1000})
		db.data[string(r.key("record", id))] = b
	}
	page, e := r.ListRecords(context.Background(), ListOptions{Limit: 2})
	if e != nil || len(page.Records) != 0 || page.NextCursor == "" {
		t.Fatalf("%v %v", page, e)
	}
	next, e := r.ListRecords(context.Background(), ListOptions{Limit: 2, Cursor: page.NextCursor})
	if e != nil || len(next.Records) != 1 || next.NextCursor != "" {
		t.Fatalf("%v %v", next, e)
	}
	for _, o := range []ListOptions{{Limit: 0}, {Limit: 1001}, {Limit: 1, Cursor: "bad!"}} {
		if _, e = r.ListRecords(context.Background(), o); !errors.Is(e, ErrInvalidList) {
			t.Fatal(e)
		}
	}
	db.iterErr = injected
	if _, e = r.ListRecords(context.Background(), ListOptions{Limit: 1}); !errors.Is(e, injected) {
		t.Fatal(e)
	}
	db.iterErr = nil
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e = r.ListRecords(ctx, ListOptions{Limit: 1}); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}
