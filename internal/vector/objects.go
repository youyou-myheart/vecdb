package vector

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"sync"
	"time"
)

type Objects struct {
	Client        blobAPI
	Bucket        string
	probeOnEnsure bool
	readyMu       sync.Mutex
	ready         bool
}

func NewObjects(endpoint, access, secret, bucket string) (*Objects, error) {
	c, e := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(access, secret, ""), Secure: false, Region: "us-east-1"})
	if e != nil {
		return nil, e
	}
	return &Objects{Client: minioBlobs{c}, Bucket: bucket, probeOnEnsure: true}, nil
}
func (o *Objects) Ensure(ctx context.Context) error {
	ok, e := o.Client.Exists(ctx, o.Bucket)
	if e != nil {
		return e
	}
	if !ok {
		if e = o.Client.Create(ctx, o.Bucket); e != nil {
			// Another replica may have created the same bucket concurrently.
			if exists, check := o.Client.Exists(ctx, o.Bucket); check != nil || !exists {
				return e
			}
		}
	}
	if !o.probeOnEnsure {
		return nil
	}
	return o.ensureWritable(ctx)
}

// Bucket metadata can be available before the object data path (e.g. a
// SeaweedFS filer lock ring). A native adapter checks one complete round trip
// before its replicas announce readiness. This is startup readiness, not an
// ongoing liveness cache or an index synchronization barrier.
func (o *Objects) ensureWritable(ctx context.Context) error {
	o.readyMu.Lock()
	defer o.readyMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if o.ready {
		return nil
	}
	data := []byte(`{"vecdb":"object-readiness"}`)
	key := ".vecdb-readiness/" + uuid.NewString()
	manifest := Manifest{Key: key, Size: int64(len(data)), SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
	err := o.Client.Put(ctx, o.Bucket, key, data)
	if err == nil {
		_, err = o.loadBytes(ctx, manifest)
	}
	// Delete our unique probe even on an uncertain PUT/GET error. A failed
	// cleanup cannot mark readiness as successful; no user object is touched.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	err = errors.Join(err, o.removeObject(cleanupCtx, key))
	if err != nil {
		return err
	}
	o.ready = true
	return nil
}
func (o *Objects) Save(ctx context.Context, namespace string, dimension int, records []Record) (Manifest, error) {
	data, e := json.Marshal(records)
	if e != nil {
		return Manifest{}, e
	}
	return o.saveBytes(ctx, namespace, dimension, "", data)
}
func (o *Objects) saveBytes(ctx context.Context, namespace string, dimension int, format string, data []byte) (Manifest, error) {
	return o.saveTracked(ctx, namespace, dimension, format, data, nil)
}
func (o *Objects) saveTracked(ctx context.Context, namespace string, dimension int, format string, data []byte, before func(Manifest) error) (Manifest, error) {
	if len(data) > 512<<20 {
		return Manifest{}, errors.New("snapshot size outside prototype limit")
	}
	id := uuid.NewString()
	m := Manifest{Generation: id, Key: namespace + "snapshots/" + id + ".json", SHA256: fmt.Sprintf("%x", sha256.Sum256(data)), Size: int64(len(data)), Dimension: dimension, Format: format}
	if before != nil {
		if err := before(m); err != nil {
			return Manifest{}, err
		}
	}
	if err := o.Client.Put(ctx, o.Bucket, m.Key, data); err != nil {
		return Manifest{}, err
	}
	return m, o.verifyBytes(ctx, m)
}

func (o *Objects) Load(ctx context.Context, m Manifest) ([]Record, error) {
	data, e := o.loadBytes(ctx, m)
	if e != nil {
		return nil, e
	}

	var records []Record
	e = json.Unmarshal(data, &records)
	return records, e
}

func (o *Objects) loadBytes(ctx context.Context, m Manifest) ([]byte, error) {
	if m.Size < 0 || m.Size > 512<<20 {
		return nil, errors.New("snapshot size outside prototype limit")
	}
	obj, e := o.Client.Get(ctx, o.Bucket, m.Key)
	if e != nil {
		return nil, e
	}
	defer obj.Close()
	data, e := io.ReadAll(io.LimitReader(obj, m.Size+1))
	if e != nil {
		return nil, e
	}
	if int64(len(data)) != m.Size || fmt.Sprintf("%x", sha256.Sum256(data)) != m.SHA256 {
		return nil, errors.New("snapshot checksum mismatch")
	}
	return data, nil
}

type GraphObjectAPI interface {
	SaveState(context.Context, string, IndexSnapshot) (Manifest, error)
	LoadState(context.Context, Manifest) (IndexSnapshot, error)
}

func (o *Objects) SaveState(ctx context.Context, namespace string, s IndexSnapshot) (Manifest, error) {
	return o.saveStateTracked(ctx, namespace, s, nil)
}
func (o *Objects) LoadState(ctx context.Context, m Manifest) (IndexSnapshot, error) {
	data, err := o.loadBytes(ctx, m)
	if err != nil {
		return IndexSnapshot{}, err
	}
	var s IndexSnapshot
	err = json.Unmarshal(data, &s)
	if err != nil {
		return s, err
	}
	if s.Format != m.Format || s.Dimension != m.Dimension || s.LogVersion != m.LogVersion || s.Offset != m.Offset {
		return s, errors.New("manifest/snapshot mismatch")
	}
	return s, nil
}

type blobAPI interface {
	Exists(context.Context, string) (bool, error)
	Create(context.Context, string) error
	Put(context.Context, string, string, []byte) error
	Get(context.Context, string, string) (io.ReadCloser, error)
}
type minioBlobs struct{ client *minio.Client }

func (m minioBlobs) Exists(ctx context.Context, b string) (bool, error) {
	return m.client.BucketExists(ctx, b)
}
func (m minioBlobs) Create(ctx context.Context, b string) error {
	return m.client.MakeBucket(ctx, b, minio.MakeBucketOptions{})
}
func (m minioBlobs) Put(ctx context.Context, b, k string, data []byte) error {
	_, err := m.client.PutObject(ctx, b, k, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: "application/json"})
	return err
}
func (m minioBlobs) PutReader(ctx context.Context, b, k string, reader io.Reader, size int64) error {
	_, err := m.client.PutObject(ctx, b, k, reader, size, minio.PutObjectOptions{ContentType: "application/json", PartSize: 8 << 20, NumThreads: 1})
	return err
}
func (m minioBlobs) Get(ctx context.Context, b, k string) (io.ReadCloser, error) {
	return m.client.GetObject(ctx, b, k, minio.GetObjectOptions{})
}

func (o *Objects) removeObject(ctx context.Context, key string) error {
	d, ok := o.Client.(interface {
		Delete(context.Context, string, string) error
	})
	if !ok {
		return errors.New("object delete adapter unavailable")
	}
	return d.Delete(ctx, o.Bucket, key)
}
func (m minioBlobs) Delete(ctx context.Context, b, key string) error {
	return m.client.RemoveObject(ctx, b, key, minio.RemoveObjectOptions{})
}
