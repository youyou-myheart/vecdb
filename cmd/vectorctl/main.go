// vectorctl is a dependency-free HTTP client and local cluster control command.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const usage = `vectorctl [--endpoint URL] [--socket PATH] [--timeout 90s] COMMAND
Credentials: API_TOKEN environment variable. cluster start also requires CLUSTER_TOKEN (both >=16 characters).
Endpoint: VECTOR_ENDPOINT or http://127.0.0.1:18202.
Output: JSON on stdout; errors on stderr and nonzero exit. Global options precede COMMAND.

  cluster status|nodes|partitions|leader|layout|health
  cluster tasks [--limit 50 --after CURSOR] | task ID | explain PARTITION | balance --dry-run
  cluster checkpoint|rebuild|prune
  cluster start --namespace NAME [--workers 2 --partitions 32 --dimension 16 --base-port 18200 --grpc-port 18250]
  cluster local-status | scale --workers N | stop   (requires --socket)
  record insert --id ID --vector '[1,2,3]' [--payload '{"tag":"a"}'] --request-id KEY
  record update --id ID --vector '[1,2,3]' --revision N --request-id KEY
  record upsert --id ID --vector '[1,2,3]' --request-id KEY
  record list [--limit 100] [--cursor TOKEN] [--include-deleted]
  record get --id ID
  record delete --id ID --revision N --request-id KEY
  search --vector '[1,2,3]' --k 10 [--filter '{"tag":"a"}'] [--exact]
         [--visible-id ID --visible-revision N] [--ef-search N]
JSON arguments support @file. update replaces the whole vector and payload.
search means nearest neighbors by squared L2 distance, not most recently inserted records.
`

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func flags(name string, stderr io.Writer) *flag.FlagSet {
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(stderr)
	return f
}
func parseJSON(value string, target any) error {
	var data []byte
	var err error
	if strings.HasPrefix(value, "@") {
		data, err = os.ReadFile(value[1:])
	} else {
		data = []byte(value)
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}
func output(w io.Writer, value any) error {
	e := json.NewEncoder(w)
	e.SetIndent("", "  ")
	return e.Encode(value)
}
func run(args []string, stdout, stderr io.Writer) error {
	f := flags("vectorctl", stderr)
	endpoint := os.Getenv("VECTOR_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:18202"
	}
	f.StringVar(&endpoint, "endpoint", endpoint, "Gateway URL")
	socket := f.String("socket", os.Getenv("VECTOR_CONTROL_SOCKET"), "local supervisor Unix socket")
	timeout := f.Duration("timeout", 90*time.Second, "request timeout")
	f.Usage = func() { fmt.Fprint(stderr, usage) }
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	args = f.Args()
	if len(args) == 0 {
		fmt.Fprint(stdout, usage)
		return nil
	}
	if *timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	if args[0] == "cluster" && len(args) > 1 && args[1] == "start" {
		return start(args[2:], *socket, stdout, stderr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	method, path := "GET", ""
	var body any
	field := ""
	switch args[0] {
	case "cluster":
		if len(args) < 2 {
			return errors.New("cluster requires a subcommand")
		}
		sub := args[1]
		cf := flags("cluster "+sub, stderr)
		workers := cf.Int("workers", -1, "desired local Workers (0..32)")
		dryRun := cf.Bool("dry-run", false, "preview without changing plans")
		limit := cf.Int("limit", 50, "task page size (1..200)")
		after := cf.String("after", "", "task page cursor")
		if err := cf.Parse(args[2:]); err != nil {
			return err
		}
		positional := 0
		if sub == "task" || sub == "explain" {
			positional = 1
		}
		if cf.NArg() != positional {
			return errors.New("invalid cluster command arguments")
		}
		var flagErr error
		cf.Visit(func(f *flag.Flag) {
			if !(f.Name == "workers" && sub == "scale" || f.Name == "dry-run" && sub == "balance" || (f.Name == "limit" || f.Name == "after") && sub == "tasks") {
				flagErr = fmt.Errorf("flag --%s is invalid for cluster %s", f.Name, sub)
			}
		})
		if flagErr != nil {
			return flagErr
		}
		switch sub {
		case "scale", "stop", "local-status":
			if *socket == "" {
				return errors.New("--socket or VECTOR_CONTROL_SOCKET is required")
			}
			request := map[string]any{"action": sub}
			if sub == "scale" {
				if *workers < 0 || *workers > 32 {
					return errors.New("workers must be 0..32")
				}
				request["workers"] = *workers
			}
			return localRequest(ctx, *socket, request, stdout)
		case "tasks":
			if *limit < 1 || *limit > 200 {
				return errors.New("limit must be 1..200")
			}
			path = "/v1/cluster/tasks?limit=" + strconv.Itoa(*limit) + "&after=" + url.QueryEscape(*after)
		case "task":
			path = "/v1/cluster/task?id=" + url.QueryEscape(cf.Arg(0))
		case "explain":
			path = "/v1/cluster/explain?partition=" + url.QueryEscape(cf.Arg(0))
		case "balance":
			if !*dryRun {
				return errors.New("cluster balance requires --dry-run; automatic scheduling is managed by Master")
			}
			path = "/v1/cluster/balance"
		case "status":
			path = "/v1/cluster"
		case "nodes", "leader":
			path = "/v1/cluster"
			field = sub
		case "partitions":
			path = "/v1/cluster"
			field = "partitions"
		case "layout":
			path = "/v1/layout"
		case "health":
			path = "/healthz"
		case "checkpoint", "rebuild":
			method = "POST"
			path = "/v1/" + sub
			body = map[string]any{}
		case "prune":
			method = "POST"
			path = "/v1/log/prune"
			body = map[string]any{}
		default:
			return fmt.Errorf("unknown cluster command %q", sub)
		}
	case "record":
		if len(args) < 2 {
			return errors.New("record requires a subcommand")
		}
		sub := args[1]
		if sub == "list" {
			lf := flags("record list", stderr)
			limit := lf.Int("limit", 100, "page size (1..1000)")
			cursor := lf.String("cursor", "", "next_cursor from previous page")
			deleted := lf.Bool("include-deleted", false, "include tombstones")
			if err := lf.Parse(args[2:]); err != nil {
				return err
			}
			if lf.NArg() != 0 || *limit < 1 || *limit > 1000 {
				return errors.New("record list: limit must be 1..1000; no positional arguments")
			}
			query := url.Values{"limit": {fmt.Sprint(*limit)}, "include_deleted": {fmt.Sprint(*deleted)}}
			if *cursor != "" {
				query.Set("cursor", *cursor)
			}
			path = "/v1/records?" + query.Encode()
			break
		}
		rf := flags("record "+sub, stderr)
		id := rf.String("id", "", "record ID")
		vec := rf.String("vector", "", "JSON vector or @file")
		payload := rf.String("payload", "{}", "complete string-valued payload or @file")
		rid := rf.String("request-id", "", "stable idempotency key")
		rev := rf.Uint64("revision", 0, "expected current revision")
		if err := rf.Parse(args[2:]); err != nil {
			return err
		}
		if rf.NArg() != 0 {
			return errors.New("unexpected positional arguments")
		}
		if *id == "" || len(*id) > 256 || strings.Contains(*id, "/") || *id == "." || *id == ".." {
			return errors.New("invalid record id")
		}
		path = "/v1/records/" + url.PathEscape(*id)
		if sub == "get" {
			if rf.NFlag() != 1 {
				return errors.New("record get accepts only --id")
			}
			break
		}
		if sub != "insert" && sub != "update" && sub != "upsert" && sub != "delete" {
			return fmt.Errorf("unknown record command %q", sub)
		}
		if *rid == "" || len(*rid) > 256 {
			return errors.New("--request-id is required (1..256 bytes); reuse the same key for retries")
		}
		mutation := map[string]any{"request_id": *rid}
		if sub == "insert" {
			if *rev != 0 {
				return errors.New("insert requires revision 0")
			}
			mutation["expected_revision"] = uint64(0)
		}
		if sub == "update" || sub == "delete" {
			if *rev == 0 {
				return errors.New("--revision must be positive for update/delete")
			}
			mutation["expected_revision"] = *rev
		}
		if sub == "upsert" {
			rf.Visit(func(f *flag.Flag) {
				if f.Name == "revision" {
					mutation["expected_revision"] = *rev
				}
			})
		}
		if sub == "delete" {
			if *vec != "" || *payload != "{}" {
				return errors.New("delete does not accept vector or payload")
			}
			method = "DELETE"
		} else {
			vector, err := readVector(*vec)
			if err != nil {
				return err
			}
			var attrs map[string]string
			if err = parseJSON(*payload, &attrs); err != nil {
				return fmt.Errorf("payload: %w", err)
			}
			mutation["vector"] = vector
			mutation["payload"] = attrs
			method = "PUT"
		}
		body = mutation
	case "search":
		sf := flags("search", stderr)
		vec := sf.String("vector", "", "JSON query vector or @file")
		k := sf.Int("k", 10, "nearest N (1..1000)")
		filter := sf.String("filter", "{}", "attribute equality filters")
		exact := sf.Bool("exact", false, "exact search")
		ef := sf.Int("ef-search", 0, "HNSW ef (0=default, max 4096)")
		id := sf.String("visible-id", "", "read-your-write token ID")
		rev := sf.Uint64("visible-revision", 0, "read-your-write token revision")
		if err := sf.Parse(args[1:]); err != nil {
			return err
		}
		if sf.NArg() != 0 {
			return errors.New("unexpected positional arguments")
		}
		if *k < 1 || *k > 1000 || *ef < 0 || *ef > 4096 {
			return errors.New("k must be 1..1000; ef-search must be 0..4096")
		}
		vector, err := readVector(*vec)
		if err != nil {
			return err
		}
		var attrs map[string]string
		if err = parseJSON(*filter, &attrs); err != nil {
			return fmt.Errorf("filter: %w", err)
		}
		q := map[string]any{"vector": vector, "k": *k, "filter": attrs, "exact": *exact, "ef_search": *ef}
		if (*id == "") != (*rev == 0) {
			return errors.New("visible-id and positive visible-revision must be supplied together")
		}
		if *id != "" {
			q["min_visible"] = map[string]any{"id": *id, "revision": *rev}
		}
		body = q
		method = "POST"
		path = "/v1/search"
	default:
		return fmt.Errorf("unknown command %q; use --help", args[0])
	}
	token := os.Getenv("API_TOKEN")
	if token == "" {
		return errors.New("API_TOKEN must be set")
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("endpoint must be an HTTP(S) URL without credentials, query or fragment")
	}
	var data []byte
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(endpoint, "/")+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(raw)))
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err = decoder.Decode(&value); err != nil {
		return fmt.Errorf("invalid JSON response: %w", err)
	}
	if field != "" {
		state, ok := value.(map[string]any)
		if !ok {
			return errors.New("unexpected cluster response")
		}
		if field == "partitions" {
			value = map[string]any{"assignments": state["assignments"], "owners": state["owners"], "ready": state["ready"], "replicas": state["replicas"], "replica_plans": state["replica_plans"], "replica_owners": state["replica_owners"], "fully_replicated": state["fully_replicated"]}
		} else {
			value = state[field]
		}
	}
	return output(stdout, value)
}
func readVector(value string) ([]float64, error) {
	var v []float64
	if err := parseJSON(value, &v); err != nil {
		return nil, fmt.Errorf("vector: %w", err)
	}
	if len(v) < 1 || len(v) > 4096 {
		return nil, errors.New("vector must contain 1..4096 finite numbers")
	}
	return v, nil
}
func localRequest(ctx context.Context, path string, request any, w io.Writer) error {
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if err = json.NewEncoder(conn).Encode(request); err != nil {
		return err
	}
	var reply struct {
		OK     bool            `json:"ok"`
		Result json.RawMessage `json:"result"`
		Error  string          `json:"error"`
	}
	if err = json.NewDecoder(io.LimitReader(conn, 16<<20)).Decode(&reply); err != nil {
		return err
	}
	if !reply.OK {
		return errors.New(reply.Error)
	}
	return output(w, reply.Result)
}
func start(args []string, socket string, stdout, stderr io.Writer) error {
	f := flags("cluster start", stderr)
	ns := f.String("namespace", "", "namespace")
	workers := f.Int("workers", 2, "Workers")
	parts := f.Int("partitions", 32, "fixed partitions")
	dim := f.Int("dimension", 16, "dimension")
	port := f.Int("base-port", 18200, "base port")
	scheduler := f.String("scheduler", "hash", "hash or load (requires replicas=2)")
	replicas := f.Int("replicas", 1, "index replicas per shard (1 or 2)")
	grpcPort := f.Int("grpc-port", 0, "optional Gateway gRPC port")
	exe, _ := os.Executable()
	defaultLauncher := filepath.Join(filepath.Dir(exe), "..", "scripts", "cluster_local.py")
	launcher := f.String("launcher", defaultLauncher, "path to cluster_local.py")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *ns == "" || socket == "" {
		return errors.New("cluster start requires --namespace and global --socket")
	}
	for _, key := range []string{"API_TOKEN", "CLUSTER_TOKEN"} {
		if len(os.Getenv(key)) < 16 {
			return fmt.Errorf("cluster start: %s must contain at least 16 characters; set it with export %s='<your-token-at-least-16-characters>' and retry (API_TOKEN: client access; CLUSTER_TOKEN: communication between nodes)", key, key)
		}
	}
	cmd := exec.Command("python3", *launcher, "--namespace", *ns, "--workers", fmt.Sprint(*workers), "--partitions", fmt.Sprint(*parts), "--dimension", fmt.Sprint(*dim), "--base-port", fmt.Sprint(*port), "--control-socket", socket)
	cmd.Args = append(cmd.Args, "--replicas", fmt.Sprint(*replicas), "--scheduler", *scheduler)
	if *grpcPort != 0 {
		cmd.Args = append(cmd.Args, "--grpc-port", fmt.Sprint(*grpcPort))
	}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case s := <-signals:
				_ = cmd.Process.Signal(s)
			case <-done:
				return
			}
		}
	}()
	return cmd.Wait()
}
