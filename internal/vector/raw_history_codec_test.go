package vector

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"testing"
)

func TestRawHistoryFrameMatchesJSONSemantics(t *testing.T) {
	for _, record := range []Record{
		{ID: "deleted", Revision: math.MaxUint64, Deleted: true},
		{ID: "empty", Revision: 1, Vector: []float64{}, Payload: map[string]string{}},
		{ID: "向量", Revision: 2, Vector: []float64{0, math.Copysign(0, -1), math.SmallestNonzeroFloat64, math.MaxFloat64, -1.2345678901234567}, Payload: map[string]string{"\x00\n": "<>&\"", "键": "值"}},
		{ID: "invalid-\xff", Revision: 3, Vector: []float64{1}, Payload: map[string]string{"bad\xff": "\xfe\xff", "bad\xfe": "also normalized"}},
	} {
		t.Run(record.ID, func(t *testing.T) {
			entry := mutationLogEntry{RequestID: "request\xff", Digest: "digest", Record: record}
			old, err := json.Marshal(entry)
			if err != nil {
				t.Fatal(err)
			}
			var expected mutationLogEntry
			if err := json.Unmarshal(old, &expected); err != nil {
				t.Fatal(err)
			}
			prefix := []byte("existing frame")
			data, err := appendRawHistoryEntry(bytes.Clone(prefix), entry)
			if err != nil || !bytes.Equal(data[:len(prefix)], prefix) {
				t.Fatal("append changed earlier frame", err)
			}
			decoded, err := decodeRawHistoryEntry(data[len(prefix):])
			if err != nil {
				t.Fatal(err)
			}
			want, _ := json.Marshal(expected)
			got, _ := json.Marshal(decoded)
			if !bytes.Equal(want, got) {
				t.Fatalf("record changed: %s != %s", want, got)
			}
			for i, value := range record.Vector {
				if math.Float64bits(decoded.Record.Vector[i]) != math.Float64bits(value) {
					t.Fatal("vector bits changed", i)
				}
			}
		})
	}
}

func TestRawHistoryFrameRejectsMalformedData(t *testing.T) {
	entry := mutationLogEntry{RequestID: "r", Digest: "d", Record: Record{ID: "id", Revision: 1, Vector: []float64{1}}}
	good, err := appendRawHistoryEntry(nil, entry)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"short", "magic", "length", "json", "tail", "nan", "duplicate_vector"} {
		t.Run(kind, func(t *testing.T) {
			bad := bytes.Clone(good)
			switch kind {
			case "short":
				bad = bad[:7]
			case "magic":
				bad[0]++
			case "length":
				binary.LittleEndian.PutUint32(bad[4:8], math.MaxUint32)
			case "json":
				bad[8] = '!'
			case "tail":
				bad = bad[:len(bad)-1]
			case "nan":
				binary.LittleEndian.PutUint64(bad[len(bad)-8:], math.Float64bits(math.NaN()))
			case "duplicate_vector":
				meta, _ := json.Marshal(entry)
				bad = append([]byte(rawHistoryFrame), binary.LittleEndian.AppendUint32(nil, uint32(len(meta)))...)
				bad = append(bad, meta...)
			}
			if _, err := decodeRawHistoryEntry(bad); err == nil {
				t.Fatal("accepted malformed cache frame")
			}
		})
	}
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		entry.Record.Vector[0] = value
		if data, err := appendRawHistoryEntry([]byte("previous"), entry); err == nil || string(data) != "previous" {
			t.Fatal("invalid vector appended a partial frame", err)
		}
	}
}

func BenchmarkRawHistoryCodec(b *testing.B) {
	for _, dimension := range []int{64, 768} {
		entry := mutationLogEntry{RequestID: "unique-request-id", Digest: "d0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0", Record: Record{ID: "unique-record-id", Revision: 1, Vector: make([]float64, dimension), Payload: map[string]string{"group": "one"}}}
		for i := range entry.Record.Vector {
			entry.Record.Vector[i] = math.Sin(float64(i + 1))
		}
		for _, format := range []string{"json", "vector_bytes"} {
			b.Run(fmt.Sprintf("%d/%s", dimension, format), func(b *testing.B) {
				encode := func(dst []byte) ([]byte, error) {
					if format == "vector_bytes" {
						return appendRawHistoryEntry(dst, entry)
					}
					data, err := json.Marshal(entry)
					return append(dst, data...), err
				}
				encoded, err := encode(nil)
				if err != nil {
					b.Fatal(err)
				}
				b.Run("encode", func(b *testing.B) {
					b.ReportAllocs()
					b.ReportMetric(float64(len(encoded)), "bytes/entry")
					var dst []byte
					for i := 0; i < b.N; i++ {
						dst, err = encode(dst[:0])
						if err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("decode", func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						var value mutationLogEntry
						if format == "vector_bytes" {
							value, err = decodeRawHistoryEntry(encoded)
						} else {
							err = json.Unmarshal(encoded, &value)
						}
						if err != nil || len(value.Record.Vector) != dimension {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
}
