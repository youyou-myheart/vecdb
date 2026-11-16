package vector

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
)

type readinessBlobs struct {
	*partitionBlobs
	fail    string
	puts    atomic.Int64
	deletes atomic.Int64
}

func (b *readinessBlobs) Put(ctx context.Context, bucket, key string, data []byte) error {
	b.puts.Add(1)
	if b.fail == "put" {
		return injected
	}
	return b.partitionBlobs.Put(ctx, bucket, key, data)
}
func (b *readinessBlobs) Get(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	if b.fail == "get" {
		return nil, injected
	}
	if b.fail == "checksum" {
		return io.NopCloser(bytes.NewReader([]byte("corrupt"))), nil
	}
	return b.partitionBlobs.Get(ctx, bucket, key)
}
func (b *readinessBlobs) Delete(ctx context.Context, bucket, key string) error {
	b.deletes.Add(1)
	if b.fail == "delete" {
		return injected
	}
	return b.partitionBlobs.Delete(ctx, bucket, key)
}

func TestObjectReadinessValidatesDataPathAndRetries(t *testing.T) {
	for _, failure := range []string{"put", "get", "checksum", "delete"} {
		t.Run(failure, func(t *testing.T) {
			b := &readinessBlobs{partitionBlobs: &partitionBlobs{values: map[string][]byte{"user": []byte("keep")}}, fail: failure}
			o := &Objects{Client: b, Bucket: "test", probeOnEnsure: true}
			if err := o.Ensure(context.Background()); err == nil || o.ready {
				t.Fatal("metadata-only readiness", err)
			}
			if b.deletes.Load() != 1 || string(b.values["user"]) != "keep" {
				t.Fatal("cleanup missing or touched user data")
			}
			b.fail = ""
			if err := o.Ensure(context.Background()); err != nil || !o.ready {
				t.Fatal(err)
			}
			if b.puts.Load() != 2 {
				t.Fatal("failed readiness cached")
			}
			if failure != "delete" && len(b.values) != 1 {
				t.Fatal("probe object leaked")
			}
		})
	}
}

func TestObjectReadinessConcurrentStartupAndCancellation(t *testing.T) {
	b := &readinessBlobs{partitionBlobs: &partitionBlobs{values: map[string][]byte{}}}
	o := &Objects{Client: b, Bucket: "test", probeOnEnsure: true}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := o.Ensure(ctx); !errors.Is(err, context.Canceled) || b.puts.Load() != 0 {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for n := 0; n < 16; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := o.Ensure(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if b.puts.Load() != 1 || b.deletes.Load() != 1 || len(b.values) != 0 {
		t.Fatal("duplicate startup probes")
	}
}
