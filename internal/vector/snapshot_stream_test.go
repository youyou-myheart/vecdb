package vector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"reflect"
	"testing"
)

func TestSnapshotStreamMatchesJSON(t *testing.T) {
	states := []IndexSnapshot{
		{},
		{Sources: map[string]uint64{}, SourceEpochs: map[string]uint64{}, GraphState: []byte{}, Records: []Record{}, Nodes: map[uint64]nodeVersion{}, Graph: []byte{}},
		{SourceEpochs: map[string]uint64{"x": 3}, Sources: map[string]uint64{"x": 9}, LogBytes: 99,
			GraphState: []byte{0, 1, 255}, GraphSequence: 7, GraphHash: "hash", LogVersion: LogVersion, Offset: 8,
			Format: graphFormat, Dimension: 2, Config: DefaultIndexConfig(), Next: math.MaxUint64,
			Records: []Record{{ID: "<&\"\n", Revision: 1, Vector: []float64{1, -2}, Payload: map[string]string{"a": "中\u2028"}}, {ID: "deleted", Revision: 2, Deleted: true}},
			Nodes:   map[uint64]nodeVersion{1: {ID: "first", Revision: 1}, math.MaxUint64: {ID: "last", Revision: 2}}, Graph: bytes.Repeat([]byte{0, 1, 255}, 70001)},
	}
	for _, s := range states {
		var streamed bytes.Buffer
		if err := writeIndexSnapshot(&streamed, s); err != nil {
			t.Fatal(err)
		}
		standard, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		var want, got any
		if err = json.Unmarshal(standard, &want); err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(streamed.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatal("stream changed JSON fields")
		}
		var restored IndexSnapshot
		if err = json.Unmarshal(streamed.Bytes(), &restored); err != nil {
			t.Fatal(err)
		}
		var expected IndexSnapshot
		if err = json.Unmarshal(standard, &expected); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(restored, expected) {
			t.Fatal("stream changed snapshot")
		}
	}
}

type streamTestBlobs struct {
	fakeBlobs
	readers int
	file    *os.File
	corrupt bool
}

func (b *streamTestBlobs) PutReader(ctx context.Context, bucket, key string, reader io.Reader, size int64) error {
	b.readers++
	f, ok := reader.(*os.File)
	if !ok {
		return errors.New("snapshot upload did not use a file")
	}
	b.file = f
	if _, err := os.Stat(f.Name()); !errors.Is(err, os.ErrNotExist) {
		return errors.New("snapshot temporary name remains linked")
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	if int64(len(data)) != size {
		return errors.New("upload size mismatch")
	}
	if b.corrupt {
		data[0] ^= 1
	}
	return b.fakeBlobs.Put(ctx, bucket, key, data)
}

func TestSnapshotStreamUploadRestore(t *testing.T) {
	idx, err := NewHNSWIndex(2, DefaultIndexConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer idx.graph.Close()
	if err = idx.Apply([]Record{{ID: "a", Revision: 1, Vector: []float64{1, 2}}, {ID: "b", Revision: 1, Vector: []float64{2, 3}}}); err != nil {
		t.Fatal(err)
	}
	s, err := idx.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	s.LogVersion, s.Offset = LogVersion, 2
	s.Sources = map[string]uint64{"0": 2}
	for _, failure := range []string{"", "stage", "put", "verify", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			b := &streamTestBlobs{}
			o := &Objects{Client: b, Bucket: "test"}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if failure == "put" {
				b.putErr = injected
			}
			if failure == "verify" {
				b.corrupt = true
			}
			if failure == "cancel" {
				cancel()
			}
			staged := false
			m, err := o.saveStateTracked(ctx, "test/", s, func(m Manifest) error {
				if b.readers != 0 || m.SHA256 == "" || m.Size == 0 {
					t.Fatal("invalid upload intent ordering")
				}
				staged = true
				if failure == "stage" {
					return injected
				}
				return nil
			})
			if (err != nil) != (failure != "") {
				t.Fatal(failure, err)
			}
			if failure == "cancel" || failure == "stage" {
				if b.readers != 0 {
					t.Fatal("uploaded after abort")
				}
				return
			}
			if !staged || b.readers != 1 {
				t.Fatal("streaming path not used")
			}
			if _, err := b.file.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
				t.Fatal("temporary file was not closed", err)
			}
			if failure != "" {
				return
			}
			if !b.closed {
				t.Fatal("verification reader not closed")
			}
			loaded, err := o.LoadState(ctx, m)
			if err != nil {
				t.Fatal(err)
			}
			fresh, err := RestoreHNSW(loaded)
			if err != nil {
				t.Fatal(err)
			}
			defer fresh.graph.Close()
			if !reflect.DeepEqual(idx.Snapshot(), fresh.Snapshot()) {
				t.Fatal("restored records changed")
			}
		})
	}
}

type failingSnapshotSink struct{}

func (failingSnapshotSink) Write([]byte) (int, error) { return 0, injected }

func TestSnapshotStreamErrors(t *testing.T) {
	if err := writeIndexSnapshot(failingSnapshotSink{}, IndexSnapshot{Graph: make([]byte, 100000)}); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if err := writeIndexSnapshot(io.Discard, IndexSnapshot{Records: []Record{{Vector: []float64{math.NaN()}}}}); err == nil {
		t.Fatal("invalid number encoded")
	}
	w := snapshotWriter{ctx: context.Background(), w: io.Discard, size: (512 << 20) - 1}
	if _, err := w.Write([]byte{1, 2}); err == nil {
		t.Fatal("size limit bypassed")
	}
	data := []byte("valid")
	m := manifestFor(data)
	for _, data := range [][]byte{[]byte("valid"), []byte("bad!!"), []byte("vali"), []byte("valid!")} {
		b := &fakeBlobs{data: data}
		err := (&Objects{Client: b}).verifyBytes(context.Background(), m)
		if (err == nil) != bytes.Equal(data, []byte("valid")) || !b.closed {
			t.Fatal("invalid verification", err)
		}
	}
}

func BenchmarkSnapshotEncoding(b *testing.B) {
	s := IndexSnapshot{Graph: make([]byte, 8<<20), Records: make([]Record, 1000)}
	for n := range s.Records {
		s.Records[n] = Record{ID: "record", Revision: 1, Vector: make([]float64, 64)}
	}
	b.Run("whole", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			if _, err := json.Marshal(s); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("stream", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			if err := writeIndexSnapshot(io.Discard, s); err != nil {
				b.Fatal(err)
			}
		}
	})
}
