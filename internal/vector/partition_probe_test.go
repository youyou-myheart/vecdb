package vector

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type probeLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *probeLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}
func (b *probeLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func TestPartitionProbeCorrelatesFencedResponseWithoutCredentials(t *testing.T) {
	r, _ := newFakeRepo()
	c := &ClusterService{root: r, token: "private-test-token", locals: map[IndexPartitionID]*localPartition{}}
	var output probeLogBuffer
	previous := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(previous)
	body, _ := json.Marshal(partitionRPC{Namespace: r.Prefix, Partition: "retired", Epoch: 1, Operation: "search", Query: Search{Vector: []float64{12345, 67890}, K: 1000}})
	for _, id := range []string{"00112233445566778899aabbccddeeff", strings.Repeat("g", 32), "invalid\ntrace", ""} {
		req := httptest.NewRequest("POST", "/internal/partition", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("X-Vecdb-Probe-ID", id)
		response := httptest.NewRecorder()
		c.InternalHandler().ServeHTTP(response, req)
		var reply partitionReply
		if err := json.Unmarshal(response.Body.Bytes(), &reply); err != nil {
			t.Fatal(err)
		}
		if response.Code != 503 || reply.Code != "fenced" || reply.Error != ErrFenced.Error() {
			t.Fatal(response.Code, reply)
		}
		want := ""
		if id == "00112233445566778899aabbccddeeff" {
			want = id
		}
		if response.Header().Get("X-Vecdb-Probe-ID") != want {
			t.Fatal("invalid correlation header")
		}
	}
	got := output.String()
	if strings.Count(got, "partition_rpc_probe") != 1 || !strings.Contains(got, "request_id=00112233445566778899aabbccddeeff partition=\"retired\" epoch=1 status=503 code=fenced") {
		t.Fatal(got)
	}
	for _, forbidden := range []string{c.token, "12345", "67890", "invalid"} {
		if strings.Contains(got, forbidden) {
			t.Fatal("probe log included unsafe content")
		}
	}
}
