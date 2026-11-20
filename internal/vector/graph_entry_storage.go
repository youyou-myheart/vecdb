package vector

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"

	"github.com/klauspost/compress/s2"
)

// Only the TiKV value is framed. Subscribers receive the original delta bytes,
// and both the entry hash and the payload digest are independent of the codec.
const graphFrame = "VGE1"

func encodeGraphEntry(e GraphEntry) ([]byte, error) {
	if len(e.PayloadData) == 0 {
		return json.Marshal(e)
	}
	body := e.PayloadData
	e.PayloadData = nil
	header, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	prefix := len(graphFrame) + 4 + len(header)
	out := make([]byte, prefix, prefix+s2.MaxEncodedLen(len(body)))
	copy(out, graphFrame)
	binary.LittleEndian.PutUint32(out[len(graphFrame):], uint32(len(header)))
	copy(out[len(graphFrame)+4:], header)
	compressed := s2.EncodeSnappy(out[prefix:], body)
	return out[:prefix+len(compressed)], nil
}

func decodeGraphEntry(raw []byte, e *GraphEntry, withBody bool) error {
	body, err := graphEntryHeader(raw, e)
	if err == nil && withBody && body != nil {
		e.PayloadData, err = s2.Decode(nil, body)
	}
	return err
}

// Return a slice of the compressed value so replay can enforce its byte budget
// before it allocates any memory for the uncompressed body.
func graphEntryHeader(raw []byte, e *GraphEntry) ([]byte, error) {
	if !bytes.HasPrefix(raw, []byte(graphFrame)) {
		// Previously stored JSON entries may carry inline bytes or external chunks.
		return nil, json.Unmarshal(raw, e)
	}
	raw = raw[len(graphFrame):]
	if len(raw) < 4 {
		return nil, errors.New("truncated graph frame")
	}
	n := uint64(binary.LittleEndian.Uint32(raw))
	raw = raw[4:]
	if n >= uint64(len(raw)) {
		return nil, errors.New("invalid graph frame header length")
	}
	if err := json.Unmarshal(raw[:n], e); err != nil {
		return nil, err
	}
	size, err := s2.DecodedLen(raw[n:])
	if err != nil || !e.Payload.validSize() || e.Payload.Chunks != 0 || e.PayloadData != nil || size != e.Payload.Size {
		return nil, errors.New("invalid framed graph payload")
	}
	return raw[n:], nil
}

func readGraphEntry(ctx context.Context, tx transactionAPI, key []byte, e *GraphEntry, withBody bool) error {
	raw, err := tx.Get(ctx, key)
	if err != nil {
		return err
	}
	return decodeGraphEntry(raw, e, withBody)
}
