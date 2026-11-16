package rpcserver

import (
	pb "example.com/vector-storage/api/vectorstore/v1"
	"example.com/vector-storage/internal/vector"
	"testing"
)

func TestClusterRoutingProtoConversion(t *testing.T) {
	r := &vector.ClusterRouting{Version: 1, Centers: [][]float64{{-10, 0}, {10, 0}}}
	out := rpcRouting(r)
	if len(out.Centers) != 2 || out.Centers[0].Vector[0] != -10 {
		t.Fatal(out)
	}
	out.Centers[0].Vector[0] = 0
	if r.Centers[0][0] != -10 {
		t.Fatal("RPC aliases immutable routing")
	}
	state := vector.ClusterStatus{GraphHeads: map[vector.IndexPartitionID]vector.GraphHead{"index-0000": {Sources: map[string]uint64{"0": 9007199254740993}}}, DataProgress: map[string]vector.DataProgress{"data-0000": {Head: 9007199254740994, Completed: 9007199254740993}}}
	wire := &pb.ClusterStatus{}
	if e := convert(state, wire); e != nil {
		t.Fatal(e)
	}
	if wire.GraphHeads["index-0000"].Sources["0"] != 9007199254740993 || wire.DataProgress["data-0000"].CompletedLsn != 9007199254740993 {
		t.Fatal(wire)
	}
	layout := &pb.Layout{}
	if e := convert(vector.PartitionLayout{Bindings: []vector.PartitionBinding{{Data: -1, Index: "index-0000"}}}, layout); e != nil || layout.Bindings[0].DataPartition != -1 {
		t.Fatal(layout, e)
	}
}
