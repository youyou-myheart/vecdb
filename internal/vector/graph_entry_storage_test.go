package vector

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"testing"
)

func TestGraphEntryStorageFramesAndLegacyJSON(t *testing.T) {
	body := bytes.Repeat([]byte("long repeated adjacency"), 4096)
	e := GraphEntry{Kind: "delta", Term: 1, Payload: &GraphPayload{Size: len(body)}, PayloadData: body}
	frame, err := encodeGraphEntry(e)
	if err != nil || len(frame) >= len(body) || !bytes.HasPrefix(frame, []byte(graphFrame)) {
		t.Fatalf("encoded %d bytes from %d: %v", len(frame), len(body), err)
	}
	jsonEntry, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range [][]byte{frame, jsonEntry} {
		var got GraphEntry
		if err = decodeGraphEntry(raw, &got, true); err != nil || !bytes.Equal(got.PayloadData, body) || entryHash(e) != entryHash(got) {
			t.Fatal("encoded entry changed its payload or identity", err)
		}
	}
	var header GraphEntry
	if err = decodeGraphEntry(frame, &header, false); err != nil || header.PayloadData != nil || entryHash(e) != entryHash(header) {
		t.Fatal("head read decoded the body or lost metadata", err)
	}
	headerEnd := 8 + int(binary.LittleEndian.Uint32(frame[4:]))
	for _, bad := range [][]byte{frame[:4], frame[:7], frame[:headerEnd], frame[:len(frame)-1], append(bytes.Clone(frame[:headerEnd]), 0)} {
		var got GraphEntry
		if err := decodeGraphEntry(bad, &got, true); err == nil {
			t.Fatal("accepted truncated or wrong-length frame")
		}
	}
}
