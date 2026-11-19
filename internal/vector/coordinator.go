package vector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
)

var ErrFenced = errors.New("partition ownership expired or superseded")

type ClusterNode struct {
	WriteEndpoint     string               `json:"write_endpoint,omitempty"`
	HostID            string               `json:"host_id,omitempty"`
	Zone              string               `json:"zone,omitempty"`
	MemoryBudgetBytes uint64               `json:"memory_budget_bytes,omitempty"`
	Capacity          float64              `json:"capacity,omitempty"`
	Load              map[string]ShardLoad `json:"load,omitempty"`
	Draining          bool                 `json:"draining,omitempty"`
	Roles             []string             `json:"roles"`
	ID                string               `json:"id"`
	Session           string               `json:"session"`
	URL               string               `json:"url"`
	Expires           int64                `json:"expires_ms"`
}
type Ownership struct {
	WriteEndpoint string           `json:"write_endpoint,omitempty"`
	Phase         string           `json:"phase,omitempty"`
	LastError     string           `json:"last_error,omitempty"`
	Offset        uint64           `json:"offset,omitempty"`
	Partition     IndexPartitionID `json:"partition"`
	Node          string           `json:"node"`
	Session       string           `json:"session"`
	URL           string           `json:"url"`
	Epoch         uint64           `json:"epoch"`
	Expires       int64            `json:"expires_ms"`
	Ready         bool             `json:"ready"`
	Released      bool             `json:"released,omitempty"`
}
type Coordinator struct {
	leaseReads         bool
	holdReleasedLeases bool
	referenceLogs      bool
	data               map[DataPartitionID]*Repository
	failureDomain      string
	loadBalance        bool
	balanceCooldown    time.Duration
	root               *Repository
	ttl                int64
}

func (c *Coordinator) nodeKey(id string) []byte {
	return []byte(c.root.Prefix + "cluster/nodes/" + hex.EncodeToString([]byte(id)))
}

// PD's timestamp physical component, not the worker's wall clock.
func txnMillis(tx transactionAPI) int64 { return int64(tx.StartTS() >> 18) }
func putJSON(tx transactionAPI, key []byte, value any) error {
	b, e := json.Marshal(value)
	if e != nil {
		return e
	}
	return tx.Set(key, b)
}
func validateNode(n ClusterNode) error {
	if len(n.HostID) > 128 || len(n.Zone) > 128 || strings.TrimSpace(n.HostID) != n.HostID || strings.TrimSpace(n.Zone) != n.Zone {
		return errors.New("invalid failure domain label")
	}
	u, e := url.Parse(n.URL)
	if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("invalid advertised HTTP origin")
	}
	if n.ID == "" || len(n.ID) > 128 || n.Session == "" {
		return errors.New("invalid node identity")
	}
	return nil
}
func (c *Coordinator) Heartbeat(ctx context.Context, node ClusterNode) error {
	if e := validateNode(node); e != nil {
		return e
	}
	tx, e := c.root.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	now := txnMillis(tx)
	b, e := tx.Get(ctx, c.nodeKey(node.ID))
	if e == nil {
		var old ClusterNode
		if e = json.Unmarshal(b, &old); e != nil {
			return e
		}
		if old.Session == node.Session && (old.HostID != node.HostID || old.Zone != node.Zone) {
			return errors.New("failure domain labels cannot change within a node session")
		}
		if old.Session != node.Session && old.Expires > now {
			return ErrConflict
		}
	} else if !tikverr.IsErrNotFound(e) {
		return e
	}
	node.Expires = now + c.ttl
	if e = putJSON(tx, c.nodeKey(node.ID), node); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (c *Coordinator) Nodes(ctx context.Context) ([]ClusterNode, error) {
	tx, e := c.root.begin()
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	return c.nodesIn(ctx, tx)
}
func (c *Coordinator) nodesIn(ctx context.Context, tx transactionAPI) ([]ClusterNode, error) {
	now := txnMillis(tx)
	prefix := c.root.Prefix + "cluster/nodes/"
	it, e := tx.Iter([]byte(prefix), []byte(c.root.Prefix+"cluster/nodes0"))
	if e != nil {
		return nil, e
	}
	defer it.Close()
	nodes := []ClusterNode{}
	count := 0
	for it.Valid() {
		if e = ctx.Err(); e != nil {
			return nil, e
		}
		count++
		if count > 1024 {
			return nil, errors.New("node registry limit")
		}
		var n ClusterNode
		if e = json.Unmarshal(it.Value(), &n); e != nil {
			return nil, e
		}
		if e = validateNode(n); e != nil {
			return nil, e
		}
		if n.Expires > now {
			nodes = append(nodes, n)
		}
		if e = it.Next(); e != nil {
			return nil, e
		}
	}
	sort.Slice(nodes, func(a, b int) bool { return nodes[a].ID < nodes[b].ID })
	return nodes, nil
}
func preferredOwner(id IndexPartitionID, nodes []ClusterNode) string {
	winner := ""
	best := ""
	for _, n := range nodes {
		h := sha256.Sum256([]byte(string(id) + "\x00" + n.ID))
		score := hex.EncodeToString(h[:])
		if score > best {
			best = score
			winner = n.ID
		}
	}
	return winner
}
func (r *Repository) ensureMode(ctx context.Context, cluster bool) error {
	tx, e := r.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	key := []byte(r.Prefix + "cluster-mode")
	b, e := tx.Get(ctx, key)
	if e == nil {
		if string(b) != "v2-master" || !cluster {
			return errors.New("namespace requires cluster mode")
		}
		return nil
	}
	if !tikverr.IsErrNotFound(e) {
		return e
	}
	if !cluster {
		return nil
	}
	if e = tx.Set(key, []byte("v2-master")); e != nil {
		return e
	}
	if e = tx.Set([]byte(r.Prefix+"schema"), []byte(fmt.Sprintf("v5:dimension=%d:metric=l2:cluster-master=1", r.Dimension)+dataLogSchemaSuffix)); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
