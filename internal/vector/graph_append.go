package vector

import (
	"context"
	"encoding/json"
)

// The single acceptance boundary for both routed and identity graph ingestion.
// Caller holds Service.syncMu and has applied h.Committed before building delta.
func (g *graphReplica) appendDelta(ctx context.Context, s *Service, h GraphHead, offset uint64, delta GraphDelta, inputs []Delivery, events ...LogEvent) error {
	if err := g.validateDelta(s, delta); err != nil {
		return err
	}
	encode := g.track("graph_encode")
	var raw []byte
	var m Manifest
	var e error
	if g.journal.c.referenceLogs {
		refs := inputs
		if g.input == nil {
			for id, repo := range g.journal.c.data {
				if repo.Prefix == g.repo.Prefix {
					refs = []Delivery{{Source: id, Events: events}}
					break
				}
			}
		}
		raw, e = referenceDelta(delta, refs)
	} else {
		raw, e = json.Marshal(delta)
	}
	encode(&e)
	if e != nil {
		return e
	}
	if !g.journal.c.referenceLogs {
		m, e = g.saveBlob(ctx, deltaFormat, raw)
		if e != nil {
			return e
		}
	}
	var cluster *ClusterService
	if g.input != nil {
		cluster = g.input.cluster
	}
	commit := g.track("graph_commit")
	var entry GraphEntry
	e = retryIndexCommit(ctx, func() error {
		measure := g.track("graph_commit_attempt")
		var err error
		if g.fastAppend {
			_, token, e := g.writer.snapshot()
			if e != nil {
				return e
			}
			entry, err = g.journal.commitWithAuthority(ctx, h, offset, "delta", m, inputs, cluster, raw, token, g.writer)
		} else if g.journal.c.referenceLogs {
			entry, err = g.journal.commitPayload(ctx, h, offset, "delta", m, inputs, cluster, raw)
		} else {
			entry, err = g.journal.commitInputs(ctx, h, offset, "delta", m, inputs, cluster)
		}
		measure(&err)
		return err
	}, func() {
		if cluster == nil {
			return
		}
		p := &cluster.observations.pipeline
		p.init()
		p.commitRetries.WithLabelValues(string(g.journal.id)).Inc()
	})
	commit(&e)
	g.observeSourceEpochRejection("commit", e)
	if g.fastAppend {
		if e == nil {
			g.writer.advance(entry)
		} else {
			g.writer.invalidate()
		}
	}
	for _, q := range inputs {
		cluster.signalGraphCommit(g.journal.id, q, e)
	}
	if e == nil {
		// Reuse the prevalidated delta after a confirmed append. On an unknown
		// outcome, recover from the durable journal instead of guessing.
		g.prepared, g.delta, g.reset = entry.Position, &delta, nil
	}
	return e
}
