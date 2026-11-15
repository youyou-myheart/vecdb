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
)

type Objects struct {
	Client blobAPI
	Bucket string
}

func NewObjects(endpoint, access, secret, bucket string) (*Objects, error) {
	c, e := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(access, secret, ""), Secure: false, Region: "us-east-1"})
	if e != nil {
		return nil, e
	}
	return &Objects{minioBlobs{c}, bucket}, nil
}
func (o *Objects) Ensure(ctx context.Context) error {
	ok, e := o.Client.Exists(ctx, o.Bucket)
	if e != nil {
		return e
	}
	if !ok {
		return o.Client.Create(ctx, o.Bucket)
	}
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
	if len(data) > 512<<20 {
		return Manifest{}, errors.New("snapshot size outside prototype limit")
	}
	id := uuid.NewString()
	m := Manifest{Generation: id, Key: namespace + "snapshots/" + id + ".json", SHA256: fmt.Sprintf("%x", sha256.Sum256(data)), Size: int64(len(data)), Dimension: dimension, Format: format}
	if err := o.Client.Put(ctx, o.Bucket, m.Key, data); err != nil {
		return Manifest{}, err
	}
	_, err := o.loadBytes(ctx, m)
	return m, err
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
	data, err := json.Marshal(s)
	if err != nil {
		return Manifest{}, err
	}
	m, err := o.saveBytes(ctx, namespace, s.Dimension, s.Format, data)
	m.LogVersion = s.LogVersion
	m.Offset = s.Offset
	return m, err
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
func (m minioBlobs) Get(ctx context.Context, b, k string) (io.ReadCloser, error) {
	return m.client.GetObject(ctx, b, k, minio.GetObjectOptions{})
}
