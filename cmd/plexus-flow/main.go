package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/server"
	"github.com/avivklas/plexus-flow/pkg/worker"
)

const version = "1.0.0"

func main() {
	var (
		nodeID                = flag.String("id", envOr("PLEXUS_FLOW_NODE_ID", "node-1"), "Unique node identifier in the cluster")
		httpAddr              = flag.String("http-addr", envOr("PLEXUS_FLOW_HTTP_ADDR", "127.0.0.1:8080"), "HTTP REST API & dashboard listen address (empty disables)")
		grpcAddr              = flag.String("grpc-addr", envOr("PLEXUS_FLOW_GRPC_ADDR", "127.0.0.1:9090"), "gRPC listen address for remote activity workers (empty disables)")
		raftAddr              = flag.String("raft-addr", envOr("PLEXUS_FLOW_RAFT_ADDR", "127.0.0.1:9000"), "Raft consensus listen address for workflow state")
		actRaftAddr           = flag.String("act-raft-addr", envOr("PLEXUS_FLOW_ACT_RAFT_ADDR", ""), "Raft consensus listen address for activity dispatcher (default: raft-addr port + 1)")
		advRaftAddr           = flag.String("advertise-raft-addr", envOr("PLEXUS_FLOW_ADVERTISE_RAFT_ADDR", ""), "Advertised Raft address for workflow state (defaults to raft-addr)")
		advActRaftAddr        = flag.String("advertise-act-raft-addr", envOr("PLEXUS_FLOW_ADVERTISE_ACT_RAFT_ADDR", ""), "Advertised Raft address for activity dispatcher (defaults to act-raft-addr)")
		dataDir               = flag.String("data-dir", envOr("PLEXUS_FLOW_DATA_DIR", "./data"), "Directory for persistent Raft logs, snapshots, cold archive, and dedup records")
		joinAddrs             = flag.String("join", envOr("PLEXUS_FLOW_JOIN", ""), "Comma-separated list of peer workflow Raft addresses to join")
		actJoinAddrs          = flag.String("act-join", envOr("PLEXUS_FLOW_ACT_JOIN", ""), "Comma-separated list of peer activity Raft addresses to join (default: derived from --join)")
		bootstrap             = flag.Bool("bootstrap", envBool("PLEXUS_FLOW_BOOTSTRAP", false), "Bootstrap this node as the initial cluster seed")
		timerInterval         = flag.Duration("timer-interval", envDuration("PLEXUS_FLOW_TIMER_INTERVAL", 100*time.Millisecond), "How often sleep, signal-wait, and retry-backoff timers are checked")
		heartbeatInterval     = flag.Duration("heartbeat-interval", envDuration("PLEXUS_FLOW_HEARTBEAT_INTERVAL", 2*time.Second), "Remote worker heartbeat interval")
		defaultStepTimeout    = flag.Duration("default-step-timeout", envDuration("PLEXUS_FLOW_DEFAULT_STEP_TIMEOUT", 30*time.Second), "Default timeout for steps without an explicit timeout")
		retentionTTL          = flag.Duration("retention-ttl", envDuration("PLEXUS_FLOW_RETENTION_TTL", 0), "Evict terminal workflows older than this duration to cold archive (0 disables)")
		maxCompletedWorkflows = flag.Int("max-completed-workflows", envInt("PLEXUS_FLOW_MAX_COMPLETED_WORKFLOWS", 0), "Cap hot terminal workflows in memory before compacting to cold archive (0 disables)")
		flowsPath             = flag.String("flows", envOr("PLEXUS_FLOW_FLOWS", ""), "Optional file or directory of JSON workflow definitions to register at startup")
		showVersion           = flag.Bool("version", false, "Print version information and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("plexus-flow v%s\n", version)
		return
	}

	registry := worker.NewRegistry()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("[plexus-flow] starting node=%s raft=%s http=%s grpc=%s data=%s",
		*nodeID, *raftAddr, *httpAddr, *grpcAddr, *dataDir)

	srv, err := server.Start(ctx, server.Options{
		NodeID:                *nodeID,
		HTTPAddr:              *httpAddr,
		GRPCAddr:              *grpcAddr,
		RaftAddr:              *raftAddr,
		ActRaftAddr:           *actRaftAddr,
		AdvertiseRaftAddr:     *advRaftAddr,
		AdvertiseActRaftAddr:  *advActRaftAddr,
		DataDir:               *dataDir,
		Join:                  splitCSV(*joinAddrs),
		ActJoin:               splitCSV(*actJoinAddrs),
		Bootstrap:             *bootstrap,
		Registry:              registry,
		TimerInterval:         *timerInterval,
		HeartbeatInterval:     *heartbeatInterval,
		DefaultStepTimeout:    *defaultStepTimeout,
		RetentionTTL:          *retentionTTL,
		MaxCompletedWorkflows: *maxCompletedWorkflows,
	})
	if err != nil {
		log.Fatalf("[plexus-flow] failed to start server: %v", err)
	}
	defer srv.Close()

	if *flowsPath != "" {
		n, err := loadFlowDefinitions(srv, *flowsPath)
		if err != nil {
			log.Fatalf("[plexus-flow] failed to load flow definitions from %s: %v", *flowsPath, err)
		}
		log.Printf("[plexus-flow] registered %d flow definition(s) from %s", n, *flowsPath)
	}

	log.Printf("[plexus-flow] node %s ready (is_leader=%v)", *nodeID, srv.IsLeader())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh

	log.Printf("[plexus-flow] received %s, shutting down gracefully...", sig)
}

func loadFlowDefinitions(srv *server.Server, path string) (int, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	var files []string
	if info.IsDir() {
		entries, err := os.ReadDir(path)
		if err != nil {
			return 0, err
		}
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(strings.ToLower(e.Name()), ".json") {
				files = append(files, filepath.Join(path, e.Name()))
			}
		}
	} else {
		files = []string{path}
	}

	count := 0
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			return count, err
		}
		trimmed := strings.TrimSpace(string(raw))
		if strings.HasPrefix(trimmed, "[") {
			var defs []flow.WorkflowDefinition
			if err := json.Unmarshal(raw, &defs); err != nil {
				return count, fmt.Errorf("%s: %w", f, err)
			}
			for _, d := range defs {
				if err := srv.Coordinator.RegisterFlow(d); err != nil {
					return count, fmt.Errorf("%s (%s): %w", f, d.Name, err)
				}
				count++
			}
		} else {
			var def flow.WorkflowDefinition
			if err := json.Unmarshal(raw, &def); err != nil {
				return count, fmt.Errorf("%s: %w", f, err)
			}
			if err := srv.Coordinator.RegisterFlow(def); err != nil {
				return count, fmt.Errorf("%s (%s): %w", f, def.Name, err)
			}
			count++
		}
	}
	return count, nil
}

func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
