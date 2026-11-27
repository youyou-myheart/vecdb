package vector

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// Large snapshots must not pass through json.Marshal/Encoder as one value:
// both retain a buffer for the complete encoded object. Only individual
// records and node mappings are encoded together; graph bytes stream as base64.
func writeIndexSnapshot(w io.Writer, s IndexSnapshot) error {
	b := bufio.NewWriterSize(w, 64<<10)
	write := func(text string) error { _, err := b.WriteString(text); return err }
	value := reflect.ValueOf(s)
	typ := value.Type()
	if err := write("{"); err != nil {
		return err
	}
	separator := ""
	for n := 0; n < value.NumField(); n++ {
		field := value.Field(n)
		name, options, _ := strings.Cut(typ.Field(n).Tag.Get("json"), ",")
		if options == "omitempty" && (field.IsZero() || ((field.Kind() == reflect.Map || field.Kind() == reflect.Slice) && field.Len() == 0)) {
			continue
		}
		if err := write(separator + strconv.Quote(name) + ":"); err != nil {
			return err
		}
		separator = ","
		switch name {
		case "graph":
			if s.Graph == nil {
				if err := write("null"); err != nil {
					return err
				}
				continue
			}
			if err := write("\""); err != nil {
				return err
			}
			encoder := base64.NewEncoder(base64.StdEncoding, b)
			if _, err := encoder.Write(s.Graph); err != nil {
				return err
			}
			if err := encoder.Close(); err != nil {
				return err
			}
			if err := write("\""); err != nil {
				return err
			}
		case "records":
			if s.Records == nil {
				if err := write("null"); err != nil {
					return err
				}
				continue
			}
			if err := write("["); err != nil {
				return err
			}
			for i, record := range s.Records {
				if i > 0 {
					if err := write(","); err != nil {
						return err
					}
				}
				data, err := json.Marshal(record)
				if err != nil {
					return err
				}
				if _, err = b.Write(data); err != nil {
					return err
				}
			}
			if err := write("]"); err != nil {
				return err
			}
		case "nodes":
			if s.Nodes == nil {
				if err := write("null"); err != nil {
					return err
				}
				continue
			}
			if err := write("{"); err != nil {
				return err
			}
			sep := ""
			for id, node := range s.Nodes {
				if err := write(sep + "\"" + strconv.FormatUint(id, 10) + "\":"); err != nil {
					return err
				}
				data, err := json.Marshal(node)
				if err != nil {
					return err
				}
				if _, err = b.Write(data); err != nil {
					return err
				}
				sep = ","
			}
			if err := write("}"); err != nil {
				return err
			}
		default:
			data, err := json.Marshal(field.Interface())
			if err != nil {
				return err
			}
			if _, err = b.Write(data); err != nil {
				return err
			}
		}
	}
	if err := write("}"); err != nil {
		return err
	}
	return b.Flush()
}

type streamingBlobs interface {
	PutReader(context.Context, string, string, io.Reader, int64) error
}

type snapshotWriter struct {
	ctx  context.Context
	w    io.Writer
	size int64
}

func (w *snapshotWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if int64(len(p)) > (512<<20)-w.size {
		return 0, errors.New("snapshot size outside prototype limit")
	}
	n, err := w.w.Write(p)
	w.size += int64(n)
	return n, err
}

func (o *Objects) saveStateTracked(ctx context.Context, namespace string, s IndexSnapshot, before func(Manifest) error) (Manifest, error) {
	client, ok := o.Client.(streamingBlobs)
	if !ok {
		// Compatibility for byte-only adapters. The production adapter streams.
		data, err := json.Marshal(s)
		if err != nil {
			return Manifest{}, err
		}
		m, err := o.saveTracked(ctx, namespace, s.Dimension, s.Format, data, before)
		m.LogVersion, m.Offset = s.LogVersion, s.Offset
		return m, err
	}
	f, err := os.CreateTemp("", "vecdb-snapshot-*")
	if err != nil {
		return Manifest{}, err
	}
	defer f.Close()
	// The descriptor stays usable on Linux/macOS. A killed worker must not
	// leave a partial snapshot behind in the temporary directory.
	if err = os.Remove(f.Name()); err != nil {
		return Manifest{}, err
	}
	hash := sha256.New()
	w := &snapshotWriter{ctx: ctx, w: io.MultiWriter(f, hash)}
	if err = writeIndexSnapshot(w, s); err != nil {
		return Manifest{}, err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return Manifest{}, err
	}
	id := uuid.NewString()
	m := Manifest{Generation: id, Key: namespace + "snapshots/" + id + ".json", SHA256: fmt.Sprintf("%x", hash.Sum(nil)), Size: w.size, Dimension: s.Dimension, Format: s.Format, LogVersion: s.LogVersion, Offset: s.Offset}
	if before != nil {
		if err = before(m); err != nil {
			return Manifest{}, err
		}
	}
	if err = client.PutReader(ctx, o.Bucket, m.Key, f, m.Size); err != nil {
		return Manifest{}, err
	}
	return m, o.verifyBytes(ctx, m)
}

// Upload verification needs the size and digest, not another full object copy.
func (o *Objects) verifyBytes(ctx context.Context, m Manifest) error {
	if m.Size < 0 || m.Size > 512<<20 {
		return errors.New("snapshot size outside prototype limit")
	}
	obj, err := o.Client.Get(ctx, o.Bucket, m.Key)
	if err != nil {
		return err
	}
	defer obj.Close()
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(obj, m.Size+1))
	if err != nil {
		return err
	}
	if n != m.Size || fmt.Sprintf("%x", hash.Sum(nil)) != m.SHA256 {
		return errors.New("snapshot checksum mismatch")
	}
	return nil
}
