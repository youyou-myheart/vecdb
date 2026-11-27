package vector

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"
)

// Exercise multipart file upload against the real S3 adapter, including the
// streamed read-back verification. Native graph validity is tested separately.
func TestSnapshotStreamMultipartIntegration(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("requires local SeaweedFS")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	o, err := NewObjects("127.0.0.1:18333", "poc-access", "poc-secret-local-only", "vector-index-poc")
	if err != nil {
		t.Fatal(err)
	}
	if err = o.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	s := IndexSnapshot{Format: graphFormat, Dimension: 2, LogVersion: LogVersion, Offset: 17, Graph: bytes.Repeat([]byte{0, 1, 255}, 3<<20)}
	m, err := o.SaveState(ctx, "snapshot-stream-test/", s)
	if m.Key != "" {
		t.Cleanup(func() { _ = o.removeObject(context.Background(), m.Key) })
	}
	if err != nil {
		t.Fatal(err)
	}
	if m.Size <= 8<<20 {
		t.Fatal("multipart upload was not exercised")
	}
	loaded, err := o.LoadState(ctx, m)
	if err != nil || !bytes.Equal(loaded.Graph, s.Graph) || loaded.Offset != s.Offset {
		t.Fatal("multipart snapshot changed", err)
	}
}
