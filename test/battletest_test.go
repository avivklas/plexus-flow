package test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avivklas/plexus-flow/pkg/api"
	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/graphflow"
	"github.com/avivklas/plexus-flow/pkg/server"
	"github.com/avivklas/plexus-flow/pkg/worker"
	"github.com/avivklas/plexus-flow/pkg/workerclient"
	"github.com/avivklas/plexus/pkg/dedup"
	"github.com/avivklas/plexus/pkg/graph"
	"github.com/avivklas/plexus/pkg/store"
)

// startThreeNodeCluster boots a real 3-node plexus-flow cluster over TCP with Raft + RESP RPC multiplexing.
func startThreeNodeCluster(t *testing.T, regFactory func() *worker.Registry) []*server.Server {
	t.Helper()
	ctx := context.Background()

	var reg1 *worker.Registry
	if regFactory != nil {
		reg1 = regFactory()
	}
	n1, err := server.Start(ctx, server.Options{
		NodeID:            "node-1",
		DataDir:           t.TempDir(),
		RaftAddr:          freeAddr(t),
		ActRaftAddr:       freeAddr(t),
		GRPCAddr:          "127.0.0.1:0",
		HTTPAddr:          "127.0.0.1:0",
		Bootstrap:         true,
		Registry:          reg1,
		TimerInterval:     40 * time.Millisecond,
		HeartbeatInterval: 150 * time.Millisecond,
		Logf:              t.Logf,
	})
	if err != nil {
		t.Fatalf("start node-1: %v", err)
	}
	t.Cleanup(n1.Close)

	nodes := []*server.Server{n1}
	for i := 2; i <= 3; i++ {
		var reg *worker.Registry
		if regFactory != nil {
			reg = regFactory()
		}
		nodeID := fmt.Sprintf("node-%d", i)
		n, err := server.Start(ctx, server.Options{
			NodeID:            nodeID,
			DataDir:           t.TempDir(),
			RaftAddr:          freeAddr(t),
			ActRaftAddr:       freeAddr(t),
			GRPCAddr:          "127.0.0.1:0",
			HTTPAddr:          "127.0.0.1:0",
			Join:              []string{n1.RaftAddr},
			ActJoin:           []string{n1.ActRaftAddr},
			Bootstrap:         false,
			Registry:          reg,
			TimerInterval:     40 * time.Millisecond,
			HeartbeatInterval: 150 * time.Millisecond,
			Logf:              t.Logf,
		})
		if err != nil {
			t.Fatalf("start %s: %v", nodeID, err)
		}
		t.Cleanup(n.Close)
		nodes = append(nodes, n)
	}
	return nodes
}

// 1. Battle-Test: 3-Node Cluster Quorum Replication, Follower-Forwarded Writes, and Health Probes.
func TestBattle_ThreeNodeCluster_QuorumAndFollowerMutations(t *testing.T) {
	nodes := startThreeNodeCluster(t, func() *worker.Registry {
		r := worker.NewRegistry()
		r.Register("enrich", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
			return json.RawMessage(`{"enriched":true}`), nil
		})
		r.Register("publish", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
			return json.RawMessage(`{"published":true}`), nil
		})
		return r
	})

	// Verify /healthz and /readyz on all 3 nodes
	for _, n := range nodes {
		resp, err := http.Get("http://" + n.HTTPAddr + "/healthz")
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("healthz failed on %s: err=%v status=%v", n.HTTPAddr, err, resp)
		}
		resp.Body.Close()

		resp, err = http.Get("http://" + n.HTTPAddr + "/readyz")
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("readyz failed on %s: err=%v status=%v", n.HTTPAddr, err, resp)
		}
		resp.Body.Close()
	}

	// Pick a follower node (node-2) and issue workflow starts & signals directly through the follower's HTTP API!
	follower := nodes[1]
	if follower.IsLeader() {
		follower = nodes[2]
	}
	followerClient := api.NewClient("http://" + follower.HTTPAddr)
	ctx := context.Background()

	st, err := followerClient.GetClusterStatus(ctx)
	if err != nil {
		t.Fatalf("GetClusterStatus on follower: %v", err)
	}
	if st.IsLeader {
		t.Fatalf("expected follower IsLeader=false, got %+v", st)
	}
	if st.LeaderID == "" || st.LeaderAddr == "" {
		t.Fatalf("expected follower to report elected leader ID and address, got %+v", st)
	}

	def := flow.WorkflowDefinition{
		Name: "clustered-pipeline",
		Steps: []flow.StepDefinition{
			{Name: "step-enrich", Activity: "enrich"},
			{Name: "step-gate", Kind: flow.KindWaitSignal, Signal: "go-live", DependsOn: []string{"step-enrich"}},
			{Name: "step-publish", Activity: "publish", DependsOn: []string{"step-gate"}},
		},
	}

	const wfCount = 6
	for i := 0; i < wfCount; i++ {
		wfID := fmt.Sprintf("cluster-wf-%d", i)
		if _, err := followerClient.StartWorkflow(ctx, flowstore.StartWorkflowRequest{
			WorkflowID: wfID,
			Definition: def,
			Input:      json.RawMessage(fmt.Sprintf(`{"item":%d}`, i)),
		}); err != nil {
			t.Fatalf("StartWorkflow %s on follower failed: %v", wfID, err)
		}
	}

	// Signal each workflow via the other follower node (node-3)
	otherFollower := nodes[2]
	if otherFollower == follower {
		otherFollower = nodes[1]
	}
	otherClient := api.NewClient("http://" + otherFollower.HTTPAddr)

	for i := 0; i < wfCount; i++ {
		wfID := fmt.Sprintf("cluster-wf-%d", i)
		if err := otherClient.SignalWorkflow(ctx, wfID, "go-live", map[string]any{"approved_by": "quorum"}); err != nil {
			t.Fatalf("SignalWorkflow %s on follower failed: %v", wfID, err)
		}
	}

	// Verify all 6 workflows reach COMPLETED and are replicated across ALL 3 nodes
	for _, n := range nodes {
		for i := 0; i < wfCount; i++ {
			wfID := fmt.Sprintf("cluster-wf-%d", i)
			wf := waitStatus(t, n.Coordinator, wfID, flow.StatusCompleted)
			if string(wf.Steps["step-publish"].Output) != `{"published":true}` {
				t.Fatalf("node %s workflow %s unexpected output: %s", n.HTTPAddr, wfID, wf.Steps["step-publish"].Output)
			}
		}
	}
}

// 2. Battle-Test: Kill Active Leader Mid-Workflow & Verify Automatic Failover + Checkpoint Recovery.
func TestBattle_ThreeNodeCluster_LeaderKillMidFlightAndCheckpointFailover(t *testing.T) {
	nodes := startThreeNodeCluster(t, nil)

	// Connect a remote worker configured with all 3 cluster gRPC addresses for automatic leader failover
	addrs := []string{nodes[0].GRPCAddr, nodes[1].GRPCAddr, nodes[2].GRPCAddr}
	w := workerclient.New(addrs[0], workerclient.Options{
		WorkerID:         "ha-worker-1",
		Addresses:        addrs,
		MaxConcurrency:   4,
		ReconnectBackoff: 25 * time.Millisecond,
	})

	midFlight := make(chan struct{})
	var midFlightOnce sync.Once
	var resumedCursor atomic.Int64
	var step3Runs atomic.Int32

	w.Registry().Register("stage-1", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"staged":true}`), nil
	})
	w.Registry().Register("stage-2-checkpointed", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		info, _ := workerclient.TaskFromContext(ctx)
		if len(info.LastProgress) == 0 {
			// First execution on initial leader: checkpoint progress at cursor 750, then signal test to kill leader!
			_ = workerclient.ReportProgress(ctx, json.RawMessage(`{"cursor":750}`))
			time.Sleep(80 * time.Millisecond) // allow progress replication to quorum
			midFlightOnce.Do(func() { close(midFlight) })
			<-ctx.Done()
			return nil, ctx.Err()
		}
		var cp struct {
			Cursor int64 `json:"cursor"`
		}
		_ = json.Unmarshal(info.LastProgress, &cp)
		resumedCursor.Store(cp.Cursor)
		return json.RawMessage(`{"processed":1000}`), nil
	})
	w.Registry().Register("stage-3", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		step3Runs.Add(1)
		return json.RawMessage(`{"finalized":true}`), nil
	})

	wctx, stopWorker := context.WithCancel(context.Background())
	defer stopWorker()
	go func() { _ = w.Run(wctx) }()

	// Wait for worker to register on the initial leader
	var initialLeaderIdx = -1
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for i, n := range nodes {
			if n.IsLeader() && n.Broker.Stats().Workers > 0 {
				initialLeaderIdx = i
				break
			}
		}
		if initialLeaderIdx != -1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if initialLeaderIdx == -1 {
		t.Fatal("worker did not register on initial cluster leader")
	}

	def := flow.WorkflowDefinition{
		Name: "failover-pipeline",
		Steps: []flow.StepDefinition{
			{Name: "s1", Activity: "stage-1"},
			{Name: "s2", Activity: "stage-2-checkpointed", DependsOn: []string{"s1"}, Retries: 2},
			{Name: "s3", Activity: "stage-3", DependsOn: []string{"s2"}},
		},
	}

	if _, err := nodes[initialLeaderIdx].Coordinator.StartWorkflow(context.Background(), flowstore.StartWorkflowRequest{
		WorkflowID: "wf-leader-kill-1",
		Definition: def,
	}); err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}

	// Wait until stage-2 is actively running and has checkpointed cursor=750
	select {
	case <-midFlight:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stage-2 to reach mid-flight checkpoint")
	}

	// HARD-KILL the active leader node mid-flight!
	t.Logf("Killing active leader node-%d while wf-leader-kill-1 is mid-flight...", initialLeaderIdx+1)
	nodes[initialLeaderIdx].Close()

	// Wait for one of the surviving 2 nodes to elect a new leader and complete the workflow!
	var survivors []*server.Server
	for i, n := range nodes {
		if i != initialLeaderIdx {
			survivors = append(survivors, n)
		}
	}

	var completed *flow.WorkflowInstance
	failoverDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(failoverDeadline) {
		for _, s := range survivors {
			if wf, ok := s.Coordinator.GetWorkflow("wf-leader-kill-1"); ok && wf.Status == flow.StatusCompleted {
				completed = wf
				break
			}
		}
		if completed != nil {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if completed == nil {
		for _, s := range survivors {
			wf, _ := s.Coordinator.GetWorkflow("wf-leader-kill-1")
			t.Logf("Survivor(leader=%v) state: %+v", s.IsLeader(), wf)
		}
		t.Fatal("wf-leader-kill-1 did not complete after leader crash")
	}

	if got := resumedCursor.Load(); got != 750 {
		t.Fatalf("expected replacement leader to recover checkpoint cursor=750, got %d", got)
	}
	if step3Runs.Load() != 1 {
		t.Fatalf("expected stage-3 to run exactly once after failover, got %d", step3Runs.Load())
	}
}

// 3. Battle-Test: Leader Crash Mid-Saga Compensation — Surviving Quorum Finishes Rollback Safely.
func TestBattle_ThreeNodeCluster_LeaderCrashDuringSagaCompensation(t *testing.T) {
	nodes := startThreeNodeCluster(t, nil)

	addrs := []string{nodes[0].GRPCAddr, nodes[1].GRPCAddr, nodes[2].GRPCAddr}
	w := workerclient.New(addrs[0], workerclient.Options{
		WorkerID:         "saga-ha-worker",
		Addresses:        addrs,
		MaxConcurrency:   4,
		ReconnectBackoff: 25 * time.Millisecond,
	})

	compMidFlight := make(chan struct{})
	var compOnce sync.Once
	var (
		reserveCount atomic.Int32
		chargeCount  atomic.Int32
		refundCount  atomic.Int32
		releaseCount atomic.Int32
	)

	w.Registry().Register("reserve-stock", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		reserveCount.Add(1)
		return json.RawMessage(`{"reserved":true}`), nil
	})
	w.Registry().Register("charge-payment", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		chargeCount.Add(1)
		return json.RawMessage(`{"charged":true}`), nil
	})
	w.Registry().Register("ship-package", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		return nil, worker.NonRetryable(worker.WithCode("COURIER_DOWN", errors.New("courier unavailable")))
	})
	w.Registry().Register("refund-payment", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		refundCount.Add(1)
		return json.RawMessage(`{"refunded":true}`), nil
	})
	w.Registry().Register("release-stock", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		n := releaseCount.Add(1)
		if n == 1 {
			// Signal test to kill the leader right while the final compensation step is in flight!
			compOnce.Do(func() { close(compMidFlight) })
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return json.RawMessage(`{"released":true}`), nil
	})

	wctx, stopWorker := context.WithCancel(context.Background())
	defer stopWorker()
	go func() { _ = w.Run(wctx) }()

	var leaderIdx = -1
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for i, n := range nodes {
			if n.IsLeader() && n.Broker.Stats().Workers > 0 {
				leaderIdx = i
				break
			}
		}
		if leaderIdx != -1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leaderIdx == -1 {
		t.Fatal("worker did not connect to initial leader")
	}

	sagaDef := flow.WorkflowDefinition{
		Name: "ha-order-saga",
		Steps: []flow.StepDefinition{
			{Name: "reserve", Activity: "reserve-stock", CompensatingAction: "release-stock"},
			{Name: "charge", Activity: "charge-payment", CompensatingAction: "refund-payment", DependsOn: []string{"reserve"}},
			{Name: "ship", Activity: "ship-package", DependsOn: []string{"charge"}},
		},
	}

	if _, err := nodes[leaderIdx].Coordinator.StartWorkflow(context.Background(), flowstore.StartWorkflowRequest{
		WorkflowID: "wf-saga-crash-1",
		Definition: sagaDef,
	}); err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}

	select {
	case <-compMidFlight:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for release-stock compensation to start")
	}

	// Kill the leader mid-compensation!
	t.Logf("Killing leader node-%d mid-Saga rollback...", leaderIdx+1)
	nodes[leaderIdx].Close()

	var survivors []*server.Server
	for i, n := range nodes {
		if i != leaderIdx {
			survivors = append(survivors, n)
		}
	}

	var compensated *flow.WorkflowInstance
	failoverDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(failoverDeadline) {
		for _, s := range survivors {
			if wf, ok := s.Coordinator.GetWorkflow("wf-saga-crash-1"); ok && wf.Status == flow.StatusCompensated {
				compensated = wf
				break
			}
		}
		if compensated != nil {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if compensated == nil {
		t.Fatal("saga did not finish compensating after leader crash")
	}
	if refundCount.Load() != 1 {
		t.Fatalf("expected already-completed refund-payment compensation NOT to re-run after failover, got %d", refundCount.Load())
	}
	if compensated.Steps["reserve"].Status != flow.StepStatusCompensated || compensated.Steps["charge"].Status != flow.StepStatusCompensated {
		t.Fatalf("expected both reserve and charge to be COMPENSATED, got reserve=%s charge=%s",
			compensated.Steps["reserve"].Status, compensated.Steps["charge"].Status)
	}
}

// 4. Battle-Test: High-Concurrency Worker Chaos (Random Worker Kills, Reconnects, and Zombie Attempt Fencing).
func TestBattle_WorkerChaos_MassDisconnectsAndZombieFencing(t *testing.T) {
	e := newRemoteEnv(t)

	const (
		numWorkflows = 30
		numWorkers   = 6
	)

	var completedSteps atomic.Int32
	registerFn := func(r *worker.Registry) {
		r.Register("chaos-step-1", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
			select {
			case <-time.After(15 * time.Millisecond):
				completedSteps.Add(1)
				return json.RawMessage(`{"s1":true}`), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
		r.Register("chaos-step-2", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
			select {
			case <-time.After(15 * time.Millisecond):
				completedSteps.Add(1)
				return json.RawMessage(`{"s2":true}`), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
	}

	var stopsMu sync.Mutex
	stops := make([]func(), numWorkers)
	for i := 0; i < numWorkers; i++ {
		stops[i] = e.startWorker(fmt.Sprintf("chaos-w-%d", i), 3, registerFn)
	}

	// Chaos monkey: kill and respawn 3 workers while workflows are actively executing
	chaosDone := make(chan struct{})
	go func() {
		defer close(chaosDone)
		for round := 0; round < 3; round++ {
			time.Sleep(40 * time.Millisecond)
			victim := round % (numWorkers - 2) // always keep at least 2 steady workers alive
			stopsMu.Lock()
			stopFn := stops[victim]
			stopsMu.Unlock()
			stopFn()
			time.Sleep(20 * time.Millisecond)
			newStop := e.startWorker(fmt.Sprintf("chaos-w-%d-reborn-%d", victim, round), 3, registerFn)
			stopsMu.Lock()
			stops[victim] = newStop
			stopsMu.Unlock()
		}
	}()

	def := flow.WorkflowDefinition{
		Name: "chaos-pipeline",
		Steps: []flow.StepDefinition{
			{Name: "step-1", Activity: "chaos-step-1", Retries: 3},
			{Name: "step-2", Activity: "chaos-step-2", DependsOn: []string{"step-1"}, Retries: 3},
		},
	}

	var startWg sync.WaitGroup
	for i := 0; i < numWorkflows; i++ {
		startWg.Add(1)
		go func(idx int) {
			defer startWg.Done()
			e.start(fmt.Sprintf("wf-chaos-%d", idx), def, map[string]string{"batch": "chaos"})
		}(i)
	}
	startWg.Wait()

	// Concurrently inject stale zombie completions/failures for Attempt=999 or RunID="stale-run" to verify fencing under load
	for i := 0; i < 10; i++ {
		wfID := fmt.Sprintf("wf-chaos-%d", i)
		_, _ = e.coord.Upstream().Apply(context.Background(), flowstore.CmdCompleteStep, flowstore.CompleteStepRequest{
			WorkflowID: wfID,
			RunID:      "zombie-run-epoch",
			StepName:   "step-1",
			Attempt:    99,
			Output:     json.RawMessage(`{"corrupted":true}`),
		})
	}

	<-chaosDone

	for i := 0; i < numWorkflows; i++ {
		wfID := fmt.Sprintf("wf-chaos-%d", i)
		wf := e.wait(wfID, flow.StatusCompleted)
		if string(wf.Steps["step-1"].Output) == `{"corrupted":true}` {
			t.Fatalf("zombie completion was not fenced out on %s", wfID)
		}
		if string(wf.Steps["step-2"].Output) != `{"s2":true}` {
			t.Fatalf("unexpected step-2 output on %s: %s", wfID, wf.Steps["step-2"].Output)
		}
	}
}

// 5. Battle-Test: High-Concurrency Parallel DAG, Conditional Branching, Saga Rollback, Signals, and Search Storm.
func TestBattle_HighConcurrency_ParallelDAGAndSagaStorm(t *testing.T) {
	reg := worker.NewRegistry()

	var (
		ingestCount   atomic.Int32
		branchACount  atomic.Int32
		branchBCount  atomic.Int32
		joinCount     atomic.Int32
		rollbackCount atomic.Int32
	)

	reg.Register("ingest", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		ingestCount.Add(1)
		return json.RawMessage(`{"rows":1000}`), nil
	})
	reg.Register("undo-ingest", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		rollbackCount.Add(1)
		return json.RawMessage(`{"purged":true}`), nil
	})
	reg.Register("branch-a", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		branchACount.Add(1)
		return json.RawMessage(`{"a_ok":true}`), nil
	})
	reg.Register("undo-a", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		rollbackCount.Add(1)
		return json.RawMessage(`{"a_undone":true}`), nil
	})
	reg.Register("branch-b", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		branchBCount.Add(1)
		return json.RawMessage(`{"b_ok":true}`), nil
	})
	reg.Register("join-commit", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		var p struct {
			FailCommit bool `json:"fail_commit"`
		}
		_ = json.Unmarshal(in, &p)
		if p.FailCommit {
			return nil, worker.NonRetryable(worker.WithCode("COMMIT_REJECTED", errors.New("downstream sink rejected batch")))
		}
		joinCount.Add(1)
		return json.RawMessage(`{"committed":true}`), nil
	})

	sc, err := graphflow.NewEmbeddedShardedCoordinator(4, reg)
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()

	def := flow.WorkflowDefinition{
		Name: "heavy-data-pipeline",
		Steps: []flow.StepDefinition{
			{Name: "ingest", Activity: "ingest", CompensatingAction: "undo-ingest"},
			{
				Name:               "branch-a",
				Activity:           "branch-a",
				CompensatingAction: "undo-a",
				DependsOn:          []string{"ingest"},
			},
			{
				Name:      "branch-b",
				Activity:  "branch-b",
				DependsOn: []string{"ingest"},
				When: &flow.Condition{
					Source: "input",
					Field:  "skip_b",
					Op:     "ne",
					Value:  json.RawMessage(`true`),
				},
			},
			{
				Name:      "join",
				Activity:  "join-commit",
				DependsOn: []string{"branch-a", "branch-b"},
			},
		},
	}

	const totalWorkflows = 80
	// Subscribe a live watcher across all shards while the storm runs
	watchCh, cancelWatch := sc.Watch(func(wf *flow.WorkflowInstance) bool { return wf.IsTerminal() }, 256)
	defer cancelWatch()
	var watchedTerminals atomic.Int32
	go func() {
		for range watchCh {
			watchedTerminals.Add(1)
		}
	}()

	// Concurrently query SearchWorkflows while mutations are flying
	stopSearch := make(chan struct{})
	var searchWg sync.WaitGroup
	searchWg.Add(1)
	go func() {
		defer searchWg.Done()
		for {
			select {
			case <-stopSearch:
				return
			default:
				_ = sc.SearchWorkflows(flowstore.SearchQuery{Query: "pipeline", Limit: 25})
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Launch all 80 workflows concurrently across 8 goroutines:
	// - Even workflows succeed (half with skip_b=true, half with skip_b=false)
	// - Every 4th workflow (i%4 == 3) fails on "join" and triggers LIFO Saga compensation!
	var wg sync.WaitGroup
	for i := 0; i < totalWorkflows; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			failCommit := idx%4 == 3
			skipB := idx%2 == 1
			input, _ := json.Marshal(map[string]any{
				"batch_id":    idx,
				"fail_commit": failCommit,
				"skip_b":      skipB,
			})
			wfID := fmt.Sprintf("storm-wf-%03d", idx)
			if _, err := sc.StartWorkflow(context.Background(), flowstore.StartWorkflowRequest{
				WorkflowID: wfID,
				Definition: def,
				Input:      input,
				Metadata:   map[string]string{"pipeline": "heavy-data"},
			}); err != nil {
				t.Errorf("StartWorkflow %s: %v", wfID, err)
			}
		}(i)
	}
	wg.Wait()

	// Wait for all 80 workflows to hit their expected terminal status
	deadline := time.Now().Add(15 * time.Second)
	for i := 0; i < totalWorkflows; i++ {
		wfID := fmt.Sprintf("storm-wf-%03d", i)
		want := flow.StatusCompleted
		if i%4 == 3 {
			want = flow.StatusCompensated
		}
		for {
			wf, ok := sc.GetWorkflow(wfID)
			if ok && wf.Status == want {
				if i%2 == 1 && wf.Steps["branch-b"].Status != flow.StepStatusSkipped {
					t.Fatalf("%s expected branch-b SKIPPED, got %s", wfID, wf.Steps["branch-b"].Status)
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("workflow %s did not reach %s (got %v)", wfID, want, wf)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	close(stopSearch)
	searchWg.Wait()

	// Verify faceted search counts across all 4 shards
	res := sc.SearchWorkflows(flowstore.SearchQuery{Limit: 100})
	if res.Total != totalWorkflows {
		t.Fatalf("expected SearchWorkflows Total=%d, got %d", totalWorkflows, res.Total)
	}
	if ingestCount.Load() != totalWorkflows {
		t.Fatalf("expected ingest to run %d times, got %d", totalWorkflows, ingestCount.Load())
	}
	// 20 workflows failed on join -> each compensated branch-a and ingest (20 * 2 = 40 rollbacks)
	if rollbackCount.Load() != 40 {
		t.Fatalf("expected 40 compensating actions to run, got %d", rollbackCount.Load())
	}
}

// 6. Battle-Test: Consensus-Level Exactly-Once Deduplication Across Replay & Snapshot/Restore.
func TestBattle_Deduplication_IdempotencyUnderReplayAndSnapshotRestore(t *testing.T) {
	actStore := worker.NewActivityStore()
	dedupStore := dedup.NewMemoryStore()
	actStore.SetDedupStore(dedupStore)
	graph.AttachDedupInterceptor(actStore, dedupStore)

	var dispatched atomic.Int32
	actStore.SetListeners(
		func(task worker.ActivityTask) { dispatched.Add(1) },
		nil,
	)

	task := worker.ActivityTask{
		WorkflowID: "wf-eos-1",
		RunID:      "run-1",
		StepName:   "transform-step",
		Activity:   "heavy-transform",
		Attempt:    0,
	}

	buildCmd := func(idempKey string) *store.Command {
		cmd, err := store.NewCommand(worker.CmdDispatchActivity, task)
		if err != nil {
			t.Fatal(err)
		}
		cmd.SetIdempotencyKey(idempKey)
		return cmd
	}

	// Replay the exact same deterministic Raft Graph command 20 times concurrently
	const key = "workflow-raft:1:42:0"
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := actStore.Router().Execute(context.Background(), buildCmd(key))
			if err != nil {
				t.Errorf("execute failed: %v", err)
			}
		}()
	}
	wg.Wait()
	time.Sleep(30 * time.Millisecond)

	if dispatched.Load() != 1 {
		t.Fatalf("expected activity dispatched exactly once before snapshot, got %d", dispatched.Load())
	}

	// Snapshot ActivityStore (which includes DedupStore state) and restore into a brand-new instance
	snap, err := actStore.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restoredAct := worker.NewActivityStore()
	restoredDedup := dedup.NewMemoryStore()
	restoredAct.SetDedupStore(restoredDedup)
	graph.AttachDedupInterceptor(restoredAct, restoredDedup)
	restoredAct.SetListeners(
		func(task worker.ActivityTask) { dispatched.Add(1) },
		nil,
	)
	if err := restoredAct.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Replay the command 10 more times after snapshot restore -> must still be deduplicated!
	for i := 0; i < 10; i++ {
		if _, err := restoredAct.Router().Execute(context.Background(), buildCmd(key)); err != nil {
			t.Fatalf("post-restore execute failed: %v", err)
		}
	}
	time.Sleep(30 * time.Millisecond)

	if dispatched.Load() != 1 {
		t.Fatalf("expected activity to remain dispatched only 1 time after snapshot restore + replay, got %d", dispatched.Load())
	}
}
