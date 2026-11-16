package vector

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeBlobs struct {
	data                                          []byte
	exists                                        bool
	existsErr, createErr, putErr, getErr, readErr error
	created, puts                                 int
	closed                                        bool
}

func (f *fakeBlobs) Exists(context.Context, string) (bool, error) { return f.exists, f.existsErr }
func (f *fakeBlobs) Create(context.Context, string) error         { f.created++; return f.createErr }
func (f *fakeBlobs) Put(_ context.Context, b, k string, data []byte) error {
	f.puts++
	if f.putErr != nil {
		return f.putErr
	}
	f.data = append([]byte(nil), data...)
	return nil
}
func (f *fakeBlobs) Get(context.Context, string, string) (io.ReadCloser, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return &fakeRead{Reader: bytes.NewReader(f.data), owner: f}, nil
}

type fakeRead struct {
	*bytes.Reader
	owner *fakeBlobs
}

func (f *fakeRead) Read(p []byte) (int, error) {
	if f.owner.readErr != nil {
		return 0, f.owner.readErr
	}
	return f.Reader.Read(p)
}
func (f *fakeRead) Close() error { f.owner.closed = true; return nil }
func manifestFor(data []byte) Manifest {
	return Manifest{Generation: "g", Key: "x", Size: int64(len(data)), SHA256: fmt.Sprintf("%x", sha256.Sum256(data)), Dimension: 2}
}
func TestObjectsEnsure(t *testing.T) {
	for _, name := range []string{"exists", "create", "exists_error", "create_error"} {
		t.Run(name, func(t *testing.T) {
			b := &fakeBlobs{}
			switch name {
			case "exists":
				b.exists = true
			case "exists_error":
				b.existsErr = injected
			case "create_error":
				b.createErr = injected
			}
			o := &Objects{Client: b, Bucket: "test"}
			e := o.Ensure(context.Background())
			if (e != nil) != strings.HasSuffix(name, "error") {
				t.Fatal(e)
			}
			want := 0
			if name == "create" || name == "create_error" {
				want = 1
			}
			if b.created != want {
				t.Fatal(b.created)
			}
		})
	}
}
func TestObjectsRoundTrip(t *testing.T) {
	b := &fakeBlobs{}
	o := &Objects{Client: b, Bucket: "test"}
	records := []Record{{ID: "a", Revision: 1, Vector: []float64{1, 2}}, {ID: "deleted", Revision: 2, Deleted: true}}
	m, e := o.Save(context.Background(), "tenant/", 2, records)
	if e != nil {
		t.Fatal(e)
	}
	if !strings.HasPrefix(m.Key, "tenant/snapshots/") || !b.closed {
		t.Fatal(m)
	}
	got, e := o.Load(context.Background(), m)
	if e != nil || len(got) != 2 || !got[1].Deleted {
		t.Fatal(got, e)
	}
	other, e := o.Save(context.Background(), "tenant/", 2, records)
	if e != nil || other.Key == m.Key {
		t.Fatal("object overwritten", e)
	}
}
func TestObjectsFailures(t *testing.T) {
	for _, name := range []string{"negative_size", "oversized", "truncated", "extra_bytes", "checksum", "json", "get", "read"} {
		t.Run(name, func(t *testing.T) {
			b := &fakeBlobs{data: []byte("[]")}
			m := manifestFor(b.data)
			switch name {
			case "negative_size":
				m.Size = -1
			case "oversized":
				m.Size = (512 << 20) + 1
			case "truncated":
				m.Size++
			case "extra_bytes":
				b.data = []byte("[]x")
			case "checksum":
				m.SHA256 = "bad"
			case "json":
				b.data = []byte("invalid")
				m = manifestFor(b.data)
			case "get":
				b.getErr = injected
			case "read":
				b.readErr = injected
			}
			if rows, e := (&Objects{Client: b}).Load(context.Background(), m); e == nil || rows != nil {
				t.Fatal(rows, e)
			}
			if name != "get" && name != "negative_size" && name != "oversized" && !b.closed {
				t.Fatal("body leaked")
			}
		})
	}
	for _, name := range []string{"marshal", "put", "verify"} {
		t.Run(name, func(t *testing.T) {
			b := &fakeBlobs{}
			rec := []Record{{Vector: []float64{1}}}
			switch name {
			case "marshal":
				rec[0].Vector[0] = math.NaN()
			case "put":
				b.putErr = injected
			case "verify":
				b.getErr = injected
			}
			if _, e := (&Objects{Client: b}).Save(context.Background(), "x", 1, rec); e == nil {
				t.Fatal("error ignored")
			}
		})
	}
}
func TestObjectsS3Adapter(t *testing.T) {
	data := []byte("[]")
	methods := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods[r.Method]++
		response := data
		if strings.Contains(r.URL.Path, "/.vecdb-readiness/") {
			response = []byte(`{"vecdb":"object-readiness"}`)
		}
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		switch r.Method {
		case "HEAD":
			w.Header().Set("Content-Length", fmt.Sprint(len(response)))
		case "GET":
			w.Header().Set("Content-Length", fmt.Sprint(len(response)))
			w.Header().Set("ETag", `"abc"`)
			_, _ = w.Write(response)
		case "PUT":
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("ETag", `"abc"`)
		case "DELETE":
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()
	o, e := NewObjects(strings.TrimPrefix(server.URL, "http://"), "access", "secret", "test-bucket")
	if e != nil {
		t.Fatal(e)
	}
	ctx := context.Background()
	if e = o.Ensure(ctx); e != nil {
		t.Fatal(e)
	}
	if e = o.Client.Create(ctx, o.Bucket); e != nil {
		t.Fatal(e)
	}
	if e = o.Client.Put(ctx, o.Bucket, "key", data); e != nil {
		t.Fatal(e)
	}
	m := manifestFor(data)
	m.Key = "key"
	if _, e = o.Load(ctx, m); e != nil {
		t.Fatal(e)
	}
	for _, method := range []string{"HEAD", "PUT", "GET", "DELETE"} {
		if methods[method] == 0 {
			t.Fatal("adapter not exercised", method)
		}
	}
	if _, e = NewObjects("http://invalid/path", "a", "b", "x"); e == nil {
		t.Fatal("invalid endpoint accepted")
	}
}
