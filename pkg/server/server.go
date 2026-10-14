// Package server runs a complete plexus-flow node (consensus, engine, worker
// gateway, REST API) inside any Go program, so an application can add its own
// replicated stores and gRPC services next to the workflow engine.
package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/avivklas/plexus-flow/pkg/api"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/graphflow"
	"github.com/avivklas/plexus-flow/pkg/remote"
	"github.com/avivklas/plexus-flow/pkg/worker"
	"github.com/avivklas/plexus/pkg/dedup"
	"github.com/avivklas/plexus/pkg/machine"
	"github.com/avivklas/plexus/pkg/store"
	"google.golang.org/grpc"
)

// Options configures a node.
type Options struct {
	NodeID string // defaults to "node-1"
	// HTTPAddr serves the REST API and dashboard; empty disables it.
	HTTPAddr string
	// GRPCAddr serves remote workers and RegisterGRPC services; empty disables it.
	GRPCAddr string
	// RaftAddr is the workflow consensus address; ActRaftAddr the activity one
	// (default: RaftAddr's port + 1).
	RaftAddr    string
	ActRaftAddr string
	DataDir     string
	// Join lists peer Raft addresses; with none, the node bootstraps itself.
	Join      []string
	Bootstrap bool

	// Registry holds activities run inside this process; remote workers are
	// preferred and these are the fallback.
	Registry *worker.Registry
	// ExtraStores are replicated through the workflow Raft log alongside the
	// workflow state. Applying their commands goes through Server.Upstream.
	ExtraStores []store.Store
	// TimerInterval is how often sleep and signal-wait timers are checked
	// (default 100ms).
	TimerInterval time.Duration
	// RegisterHTTP mounts extra routes on the REST API before it starts.
	RegisterHTTP func(handle func(pattern string, h http.Handler), s *Server) error
	// RegisterGRPC adds services to the worker gRPC server before it starts.
	RegisterGRPC func(*grpc.Server, *Server) error
	// Logf receives progress messages; nil uses the standard logger.
	Logf func(format string, args ...any)
}

// Server is a running node.
type Server struct {
	Coordinator *graphflow.Coordinator
	FlowStore   *flowstore.Store
	// Upstream is the workflow machine; ExtraStores' commands are applied here.
	Upstream   machine.Machine
	Downstream machine.Machine
	Broker     *remote.Broker
	API        *api.Server
	GRPC       *grpc.Server
	// GRPCAddr is the address workers connect to once started.
	GRPCAddr string

	cleanup []func()
}

// IsLeader reports whether this node leads both consensus groups.
func (s *Server) IsLeader() bool { return s.Upstream.IsLeader() && s.Downstream.IsLeader() }

// Start boots the node and returns once it holds leadership (or has joined).
// ctx bounds startup only; stop the node with Close.
func Start(ctx context.Context, opts Options) (*Server, error) {
	opts = withDefaults(opts)
	logf := opts.Logf
	if logf == nil {
		logf = log.Printf
	}

	if err := os.MkdirAll(opts.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	s := &Server{}
	ok := false
	defer func() {
		if !ok {
			s.Close()
		}
	}()

	registry := opts.Registry
	if registry == nil {
		registry = worker.NewRegistry()
	}
	flowStore := flowstore.New()
	actStore := worker.NewActivityStore()
	s.FlowStore = flowStore

	var dedupStore dedup.Store
	if fs, err := dedup.NewFileStore(filepath.Join(opts.DataDir, "dedup.db")); err != nil {
		logf("persistent dedup store unavailable, using memory: %v", err)
		dedupStore = dedup.NewMemoryStore()
	} else {
		dedupStore = fs
	}

	up := machine.NewRaftMachine(raftConfig(opts, "-wf", opts.RaftAddr, filepath.Join(opts.DataDir, "workflow-raft"), true))
	up.Register(flowStore)
	for _, st := range opts.ExtraStores {
		up.Register(st)
	}
	down := machine.NewRaftMachine(raftConfig(opts, "-act", opts.ActRaftAddr, filepath.Join(opts.DataDir, "activity-raft"), false))
	down.Register(actStore)
	s.Upstream, s.Downstream = up, down

	if err := up.Start(ctx); err != nil {
		return nil, fmt.Errorf("start workflow raft: %w", err)
	}
	s.cleanup = append(s.cleanup, func() { _ = up.Stop() })
	if err := down.Start(ctx); err != nil {
		return nil, fmt.Errorf("start activity raft: %w", err)
	}
	s.cleanup = append(s.cleanup, func() { _ = down.Stop() })

	if err := waitLeader(ctx, s, 10*time.Second); err != nil {
		return nil, err
	}

	coord, err := graphflow.NewCoordinator(graphflow.Config{
		GraphName:     "plexus-flow-" + opts.NodeID,
		Upstream:      up,
		Downstream:    down,
		FlowStore:     flowStore,
		ActivityStore: actStore,
		DedupStore:    dedupStore,
		Registry:      registry,
		TimerInterval: opts.TimerInterval,
	})
	if err != nil {
		return nil, fmt.Errorf("create coordinator: %w", err)
	}
	s.Coordinator = coord
	s.cleanup = append(s.cleanup, func() { _ = coord.Close() })

	if opts.GRPCAddr != "" {
		if err := s.startGRPC(opts, registry); err != nil {
			return nil, err
		}
		logf("worker gRPC API on %s", s.GRPCAddr)
	}

	if opts.HTTPAddr != "" {
		s.API = api.NewServer(opts.HTTPAddr, coord, opts.NodeID)
		if opts.RegisterHTTP != nil {
			if err := opts.RegisterHTTP(s.API.Handle, s); err != nil {
				return nil, fmt.Errorf("register http routes: %w", err)
			}
		}
		if err := s.API.Start(); err != nil {
			return nil, fmt.Errorf("start http api on %s: %w", opts.HTTPAddr, err)
		}
		s.cleanup = append(s.cleanup, func() { _ = s.API.Close() })
		logf("HTTP API on %s", s.API.Addr())
	}

	ok = true
	return s, nil
}

func (s *Server) startGRPC(opts Options, registry *worker.Registry) error {
	broker, err := remote.NewBroker(remote.Options{
		Applier:  s.Upstream,
		Flows:    s.FlowStore,
		IsLeader: s.IsLeader,
		Local:    s.Coordinator.Executor(),
		Registry: registry,
	})
	if err != nil {
		return fmt.Errorf("create worker broker: %w", err)
	}
	s.Broker = broker
	s.cleanup = append(s.cleanup, broker.Close)
	s.Coordinator.SetDispatcher(broker)
	broker.Recover() // steps that were running when this process last stopped

	lis, err := net.Listen("tcp", opts.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen for workers on %s: %w", opts.GRPCAddr, err)
	}
	g := grpc.NewServer()
	remote.NewServer(broker).Register(g)
	if opts.RegisterGRPC != nil {
		if err := opts.RegisterGRPC(g, s); err != nil {
			_ = lis.Close()
			return fmt.Errorf("register grpc services: %w", err)
		}
	}
	s.GRPC = g
	s.GRPCAddr = lis.Addr().String()
	go func() { _ = g.Serve(lis) }()
	s.cleanup = append(s.cleanup, func() {
		// Worker streams are long-lived, so a graceful stop would wait for
		// workers that never hang up. Give them a moment, then cut them off.
		done := make(chan struct{})
		go func() { g.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			g.Stop()
		}
	})
	return nil
}

// Close stops everything in reverse order of startup.
func (s *Server) Close() {
	for i := len(s.cleanup) - 1; i >= 0; i-- {
		s.cleanup[i]()
	}
	s.cleanup = nil
}

func withDefaults(o Options) Options {
	if o.NodeID == "" {
		o.NodeID = "node-1"
	}
	if o.DataDir == "" {
		o.DataDir = "./data"
	}
	if o.RaftAddr == "" {
		o.RaftAddr = "127.0.0.1:9000"
	}
	if o.ActRaftAddr == "" {
		o.ActRaftAddr = nextPort(o.RaftAddr)
	}
	if !o.Bootstrap && len(o.Join) == 0 {
		o.Bootstrap = true
	}
	return o
}

func nextPort(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "127.0.0.1:9001"
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		return "127.0.0.1:9001"
	}
	return net.JoinHostPort(host, strconv.Itoa(n+1))
}

func raftConfig(o Options, suffix, addr, dir string, withJoin bool) machine.Config {
	id := o.NodeID + suffix
	cfg := machine.DefaultConfig(machine.MachineID(id), &machine.Node{ID: id, Address: addr, Voter: true}, dir)
	// Bootstrapping is only for a brand-new cluster: a node restarted on its
	// existing data directory recovers its configuration from the log.
	cfg.Bootstrap = o.Bootstrap && !hasState(dir)
	if withJoin {
		for _, a := range o.Join {
			if a = strings.TrimSpace(a); a != "" {
				cfg.JoinAddrs = append(cfg.JoinAddrs, a)
			}
		}
	}
	return cfg
}

// hasState reports whether a Raft data directory already holds state.
func hasState(dir string) bool {
	entries, err := os.ReadDir(dir)
	return err == nil && len(entries) > 0
}

func waitLeader(ctx context.Context, s *Server, d time.Duration) error {
	deadline := time.Now().Add(d)
	for !s.IsLeader() {
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for raft leadership")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return nil
}
