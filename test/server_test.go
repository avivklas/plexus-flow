package test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/server"
	"github.com/avivklas/plexus-flow/pkg/workerclient"
	"github.com/avivklas/plexus/pkg/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// counterStore is a minimal application store replicated next to the workflow state.
type counterStore struct {
	store.BaseStore
	mu sync.Mutex
	n  map[string]int
}

const cmdBump store.CommandType = "test.counter.bump"

type bumpRequest struct{ Key string }

func newCounterStore() *counterStore {
	c := &counterStore{BaseStore: store.NewBaseStore(), n: map[string]int{}}
	store.HandleTyped(c.Router(), cmdBump, func(_ context.Context, req bumpRequest) (int, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.n[req.Key]++
		return c.n[req.Key], nil
	})
	return c
}

func (c *counterStore) ID() store.StoreID { return "counter" }
func (c *counterStore) Snapshot() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return json.Marshal(c.n)
}
func (c *counterStore) Restore(b []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n = map[string]int{}
	return json.Unmarshal(b, &c.n)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// An application can host its own replicated store and gRPC service inside the
// engine process and still run workflows on remote workers.
func TestEmbeddedServerHostsExtraStoreAndServices(t *testing.T) {
	counter := newCounterStore()
	srv, err := server.Start(context.Background(), server.Options{
		DataDir:     t.TempDir(),
		RaftAddr:    freeAddr(t),
		ActRaftAddr: freeAddr(t),
		GRPCAddr:    "127.0.0.1:0",
		ExtraStores: []store.Store{counter},
		RegisterGRPC: func(g *grpc.Server, _ *server.Server) error {
			healthpb.RegisterHealthServer(g, health.NewServer())
			return nil
		},
		Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	// The extra store is replicated through the same consensus group.
	for i := 1; i <= 3; i++ {
		res, err := srv.Upstream.Apply(context.Background(), cmdBump, bumpRequest{Key: "k"})
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(res) != fmt.Sprint(i) {
			t.Fatalf("bump %d returned %v", i, res)
		}
	}

	// The extra gRPC service answers on the same port workers use.
	conn, err := grpc.NewClient(srv.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("extra service unreachable: %v", err)
	}

	// And a remote worker runs a workflow on the embedded engine.
	w := workerclient.New(srv.GRPCAddr, workerclient.Options{WorkerID: "w1", MaxConcurrency: 1})
	w.Registry().Register("hello", func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`"hi"`), nil
	})
	wctx, stop := context.WithCancel(context.Background())
	defer stop()
	go func() { _ = w.Run(wctx) }()
	time.Sleep(300 * time.Millisecond)

	if _, err := srv.Coordinator.StartWorkflow(context.Background(), flowstore.StartWorkflowRequest{
		WorkflowID: "embedded-1",
		Definition: flow.WorkflowDefinition{Name: "embedded", Steps: []flow.StepDefinition{{Name: "s", Activity: "hello"}}},
	}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, srv.Coordinator, "embedded-1", flow.StatusCompleted)
}

// A node restarted on its data directory recovers its state instead of trying
// to bootstrap a new cluster, and stopping it does not wait for connected workers.
func TestEmbeddedServerRestartsOnItsDataDir(t *testing.T) {
	dir, raftAddr, actAddr, grpcAddr := t.TempDir(), freeAddr(t), freeAddr(t), freeAddr(t)
	start := func(counter *counterStore) *server.Server {
		t.Helper()
		srv, err := server.Start(context.Background(), server.Options{
			DataDir: dir, RaftAddr: raftAddr, ActRaftAddr: actAddr, GRPCAddr: grpcAddr,
			ExtraStores: []store.Store{counter}, Logf: t.Logf,
		})
		if err != nil {
			t.Fatal(err)
		}
		return srv
	}

	first := start(newCounterStore())
	for i := 0; i < 2; i++ {
		if _, err := first.Upstream.Apply(context.Background(), cmdBump, bumpRequest{Key: "k"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := first.Coordinator.StartWorkflow(context.Background(), flowstore.StartWorkflowRequest{
		WorkflowID: "survivor",
		Definition: flow.WorkflowDefinition{Name: "d", Steps: []flow.StepDefinition{{Name: "s", Activity: "remote"}}},
	}); err != nil {
		t.Fatal(err)
	}

	// A worker is connected (and never hangs up) when the node stops.
	w := workerclient.New(grpcAddr, workerclient.Options{WorkerID: "w1", MaxConcurrency: 1})
	w.Registry().Register("other", func(context.Context, json.RawMessage) (json.RawMessage, error) { return nil, nil })
	wctx, stop := context.WithCancel(context.Background())
	defer stop()
	go func() { _ = w.Run(wctx) }()
	time.Sleep(300 * time.Millisecond)

	closed := make(chan struct{})
	go func() { first.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close blocked on a connected worker")
	}

	counter := newCounterStore()
	second := start(counter)
	defer second.Close()
	res, err := second.Upstream.Apply(context.Background(), cmdBump, bumpRequest{Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(res) != "3" {
		t.Fatalf("extra store did not recover: bump returned %v, want 3", res)
	}
	if _, ok := second.FlowStore.GetWorkflow("survivor"); !ok {
		t.Fatal("workflow was lost across the restart")
	}
}
