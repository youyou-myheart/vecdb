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
	Phase     string           `json:"phase,omitempty"`
	LastError string           `json:"last_error,omitempty"`
	Offset    uint64           `json:"offset,omitempty"`
	Partition IndexPartitionID `json:"partition"`
	Node      string           `json:"node"`
	Session   string           `json:"session"`
	URL       string           `json:"url"`
	Epoch     uint64           `json:"epoch"`
	Expires   int64            `json:"expires_ms"`
	Ready     bool             `json:"ready"`
}
type Coordinator struct {
	failureDomain   string
	loadBalance     bool
	balanceCooldown time.Duration
	root            *Repository
	ttl             int64
}

func (c *Coordinator) nodeKey(id string) []byte {
	return []byte(c.root.Prefix + "cluster/nodes/" + hex.EncodeToString([]byte(id)))
}
func (c *Coordinator) ownerKey(id IndexPartitionID) []byte {
	return []byte(c.root.Prefix + "cluster/owners/" + string(id))
}

func (c *Coordinator) fenceKey(id IndexPartitionID) []byte {
	return []byte(c.root.Prefix + "cluster/fences/" + string(id))
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
func (c *Coordinator) readOwner(ctx context.Context, tx transactionAPI, id IndexPartitionID) (Ownership, error) {
	b, e := tx.Get(ctx, c.ownerKey(id))
	if e != nil {
		return Ownership{}, e
	}
	var o Ownership
	e = json.Unmarshal(b, &o)
	if e == nil && o.Partition != id {
		e = errors.New("ownership key mismatch")
	}
	return o, e
}
func (c *Coordinator) Acquire(ctx context.Context, id IndexPartitionID, node ClusterNode) (Ownership, error) {
	tx, e := c.root.begin()
	if e != nil {
		return Ownership{}, e
	}
	defer tx.Rollback()
	now := txnMillis(tx)
	b, e := tx.Get(ctx, c.nodeKey(node.ID))
	if e != nil {
		return Ownership{}, e
	}
	var registered ClusterNode
	if e = json.Unmarshal(b, &registered); e != nil {
		return Ownership{}, e
	}
	if registered.Session != node.Session || registered.Expires <= now {
		return Ownership{}, ErrFenced
	}
	if id == masterLeaseID {
		if !hasRole(registered, RoleMaster) {
			return Ownership{}, ErrFenced
		}
	} else {
		if !hasRole(registered, RoleWorker) {
			return Ownership{}, ErrFenced
		}
		assignment, err := c.assignmentIn(ctx, tx, id)
		if err != nil {
			return Ownership{}, err
		}
		if assignment.Node != node.ID || assignment.Session != node.Session {
			return Ownership{}, ErrFenced
		}
		// Conflict with a concurrent reassignment rather than claiming from stale intent.
		if err = putJSON(tx, c.assignmentKey(id), assignment); err != nil {
			return Ownership{}, err
		}
	}
	old, e := c.readOwner(ctx, tx, id)
	if e != nil && !tikverr.IsErrNotFound(e) {
		return old, e
	}
	if e == nil && old.Expires > now {
		return old, ErrConflict
	}
	if old.Epoch == ^uint64(0) {
		return old, errors.New("ownership epoch exhausted")
	}
	o := Ownership{Ready: id == masterLeaseID, Partition: id, Node: node.ID, Session: node.Session, URL: strings.TrimRight(node.URL, "/"), Epoch: old.Epoch + 1, Expires: now + c.ttl}
	if e = putJSON(tx, c.ownerKey(id), o); e != nil {
		return o, e
	}
	if e = putJSON(tx, c.fenceKey(id), o); e != nil {
		return o, e
	}
	return o, tx.Commit(ctx)
}
func sameOwner(a, b Ownership) bool {
	return a.Partition == b.Partition && a.Node == b.Node && a.Session == b.Session && a.Epoch == b.Epoch
}
func (c *Coordinator) Update(ctx context.Context, expected Ownership, ready, release bool) (Ownership, error) {
	tx, e := c.root.begin()
	if e != nil {
		return Ownership{}, e
	}
	defer tx.Rollback()
	o, e := c.readOwner(ctx, tx, expected.Partition)
	if e != nil {
		return o, e
	}
	now := txnMillis(tx)
	if !sameOwner(o, expected) || o.Expires <= now {
		return o, ErrFenced
	}
	if expected.Partition != masterLeaseID && !release {
		assignment, err := c.assignmentIn(ctx, tx, expected.Partition)
		if err != nil {
			return o, err
		}
		if assignment.Node != expected.Node || assignment.Session != expected.Session {
			return o, ErrFenced
		}
	}
	o.Ready = o.Ready || ready
	o.Expires = now + c.ttl
	if release {
		o.Expires = 0
		o.Ready = false
	}
	if e = putJSON(tx, c.ownerKey(o.Partition), o); e != nil {
		return o, e
	}
	if release {
		if e = putJSON(tx, c.fenceKey(o.Partition), o); e != nil {
			return o, e
		}
	}
	return o, tx.Commit(ctx)
}
func (c *Coordinator) Resolve(ctx context.Context, id IndexPartitionID) (Ownership, error) {
	tx, e := c.root.begin()
	if e != nil {
		return Ownership{}, e
	}
	defer tx.Rollback()
	o, e := c.readOwner(ctx, tx, id)
	if e != nil {
		return o, e
	}
	if o.Expires <= txnMillis(tx) || !o.Ready {
		return o, ErrFenced
	}
	return o, nil
}
func (c *Coordinator) Check(ctx context.Context, expected Ownership) error {
	current, e := c.Resolve(ctx, expected.Partition)
	if e != nil {
		return e
	}
	if !sameOwner(current, expected) {
		return ErrFenced
	}
	return nil
}

// Fence changes on acquisition/release, independently of renewal. Mutations
// write it transactionally so takeover conflicts with in-flight publications.
func (c *Coordinator) Fence(expected Ownership) func(context.Context, transactionAPI) error {
	return func(ctx context.Context, tx transactionAPI) error {
		o, e := c.readOwner(ctx, tx, expected.Partition)
		if e != nil {
			return e
		}
		if !sameOwner(o, expected) || !o.Ready || o.Expires <= txnMillis(tx) {
			return ErrFenced
		}
		b, e := tx.Get(ctx, c.fenceKey(o.Partition))
		if e != nil {
			return e
		}
		var token Ownership
		if e = json.Unmarshal(b, &token); e != nil {
			return e
		}
		if !sameOwner(token, expected) {
			return ErrFenced
		}
		return tx.Set(c.fenceKey(o.Partition), b)
	}
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
	if e = tx.Set([]byte(r.Prefix+"schema"), []byte(fmt.Sprintf("v5:dimension=%d:metric=l2:cluster-master=1", r.Dimension))); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
