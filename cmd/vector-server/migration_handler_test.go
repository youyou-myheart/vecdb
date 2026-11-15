package main

import (
	"context"
	"example.com/vector-storage/internal/vector"
	tikverr "github.com/tikv/client-go/v2/error"
	"testing"
)

type fakeMigration struct {
	*fakeBackend
	err error
}

func (f *fakeMigration) Tasks(context.Context, string, int) (vector.TaskPage, error) {
	f.calls++
	return vector.TaskPage{Tasks: []vector.MigrationTask{{ID: "task-1"}}}, f.err
}
func (f *fakeMigration) Task(context.Context, string) (vector.MigrationTask, error) {
	f.calls++
	return vector.MigrationTask{ID: "task-1"}, f.err
}
func (f *fakeMigration) Preview(context.Context) (vector.BalancePreview, error) {
	f.calls++
	return vector.BalancePreview{Partitions: []vector.PlacementExplanation{{Partition: "index-0000"}}}, f.err
}
func TestMigrationHTTPContracts(t *testing.T) {
	f := &fakeMigration{fakeBackend: backend()}
	h := newHandler(f, f, 2, testToken, "ns")
	for _, path := range []string{"/v1/cluster/tasks", "/v1/cluster/task?id=task-1", "/v1/cluster/balance", "/v1/cluster/explain?partition=index-0000"} {
		before := f.calls
		if w := request(h, "GET", path, "", false); w.Code != 401 || f.calls != before {
			t.Fatal(w.Code)
		}
		if w := request(h, "POST", path, "", true); w.Code != 405 {
			t.Fatal(w.Code)
		}
		if w := request(h, "GET", path, "", true); w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	for _, path := range []string{"/v1/cluster/tasks?limit=0", "/v1/cluster/tasks?limit=201", "/v1/cluster/task"} {
		if w := request(h, "GET", path, "", true); w.Code != 400 {
			t.Fatal(path, w.Code)
		}
	}
	if w := request(h, "GET", "/v1/cluster/explain?partition=missing", "", true); w.Code != 404 {
		t.Fatal(w.Code)
	}
	f.err = tikverr.ErrNotExist
	if w := request(h, "GET", "/v1/cluster/task?id=missing", "", true); w.Code != 404 {
		t.Fatal(w.Code)
	}
}
