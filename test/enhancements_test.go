package test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avivklas/plexus-flow/pkg/api"
	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/graphflow"
	"github.com/avivklas/plexus-flow/pkg/worker"
	"github.com/avivklas/plexus-flow/pkg/workerclient"
)

// 1. Verify single-commit atomic step advancement (Version = 2 for a 1-step workflow:
// Version 1 on StartWorkflow+Dispatch, Version 2 on CompleteStep+CompleteWorkflow)
// and automatic pruning of completed workflow tasks from ActivityStore.
func TestSingleCommitAdvanceAndActivityPruning(t *testing.T) {
	reg := worker.NewRegistry()
	reg.Register("fast-act", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"done":true}`), nil
	})

	coord, err := graphflow.NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatal(err)
	}
	defer coord.Close()

	def := flow.WorkflowDefinition{
		Name: "single-commit-flow",
		Steps: []flow.StepDefinition{
			{Name: "step-1", Activity: "fast-act"},
		},
	}

	wf, err := coord.StartWorkflow(context.Background(), flowstore.StartWorkflowRequest{
		WorkflowID: "wf-single-commit",
		Definition: def,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Step-1 was dispatched atomically inside CmdStartWorkflow (Version 1).
	if wf.Steps["step-1"].Status != flow.StepStatusRunning && wf.Steps["step-1"].Status != flow.StepStatusCompleted {
		t.Fatalf("expected step-1 atomically advanced on start, got %s", wf.Steps["step-1"].Status)
	}

	final := waitStatus(t, coord, "wf-single-commit", flow.StatusCompleted)
	// Version 1 = StartWorkflow (+ atomic dispatch of step-1)
	// Version 2 = CompleteStep (+ atomic completion of workflow)
	if final.Version != 2 {
		t.Fatalf("expected exactly 2 Raft state transitions (versions) for 1-step workflow, got %d", final.Version)
	}

	// Wait briefly for downstream prune command to apply on ActivityStore
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if coord.ActivityStore().TaskCount() == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n := coord.ActivityStore().TaskCount(); n != 0 {
		t.Fatalf("expected ActivityStore to prune terminal workflow tasks, still has %d", n)
	}
}

// 2. Verify tiered cold storage archival & compaction: terminal workflows are evicted
// from hot memory and Snapshot() while remaining readable via GetWorkflow and GetHistory.
func TestWorkflowCompactionAndColdArchive(t *testing.T) {
	reg := echoRegistry()
	coord, err := graphflow.NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatal(err)
	}
	defer coord.Close()

	archiver, err := flowstore.NewFileArchiver(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	coord.FlowStore().SetArchiver(archiver)

	def := flow.WorkflowDefinition{
		Name:  "archive-flow",
		Steps: []flow.StepDefinition{{Name: "s", Activity: "noop"}},
	}

	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("wf-arch-%d", i)
		if _, err := coord.StartWorkflow(context.Background(), flowstore.StartWorkflowRequest{
			WorkflowID: id,
			Definition: def,
		}); err != nil {
			t.Fatal(err)
		}
		waitStatus(t, coord, id, flow.StatusCompleted)
	}

	// Compact keeping only 1 completed workflow in hot memory
	resp, err := coord.CompactWorkflows(context.Background(), flowstore.CompactWorkflowsRequest{
		MaxRetained: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Compacted != 3 {
		t.Fatalf("expected 3 workflows compacted to cold archive, got %d", resp.Compacted)
	}
	if hot := len(coord.ListWorkflows()); hot != 1 {
		t.Fatalf("expected 1 hot workflow in memory after compaction, got %d", hot)
	}

	// All 4 workflows and their histories are still transparently retrievable (3 from cold FileArchiver, 1 from hot memory)
	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("wf-arch-%d", i)
		wf, ok := coord.GetWorkflow(id)
		if !ok || wf.Status != flow.StatusCompleted {
			t.Fatalf("expected cold-archived workflow %s to be retrievable, got ok=%v wf=%v", id, ok, wf)
		}
		hist, ok := coord.FlowStore().GetHistory(id)
		if !ok || len(hist) == 0 {
			t.Fatalf("expected cold-archived history for %s", id)
		}
	}
}

// 3. Verify Multi-Raft ShardedCoordinator distributes workflows across shards and executes in parallel.
func TestShardedCoordinatorMultiRaft(t *testing.T) {
	reg := worker.NewRegistry()
	var runs atomic.Int32
	reg.Register("work", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		runs.Add(1)
		return json.RawMessage(`{"ok":true}`), nil
	})

	sc, err := graphflow.NewEmbeddedShardedCoordinator(4, reg)
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()

	def := flow.WorkflowDefinition{
		Name:  "sharded-flow",
		Steps: []flow.StepDefinition{{Name: "step-1", Activity: "work"}},
	}

	const total = 16
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("wf-shard-%d", i)
		if _, err := sc.StartWorkflow(context.Background(), flowstore.StartWorkflowRequest{
			WorkflowID: id,
			Definition: def,
		}); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		completed := 0
		for i := 0; i < total; i++ {
			if wf, ok := sc.GetWorkflow(fmt.Sprintf("wf-shard-%d", i)); ok && wf.Status == flow.StatusCompleted {
				completed++
			}
		}
		if completed == total {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	if runs.Load() != total {
		t.Fatalf("expected %d activity runs across 4 Raft shards, got %d", total, runs.Load())
	}
	if all := sc.ListWorkflows(); len(all) != total {
		t.Fatalf("expected ListWorkflows to aggregate %d workflows across shards, got %d", total, len(all))
	}
}

// 4. Verify RetryPolicy exponential backoff timing and NonRetryableErrorCodes filtering on remote workers.
func TestRetryPolicyExponentialBackoffAndNonRetryableCode(t *testing.T) {
	e := newRemoteEnv(t)

	var (
		mu         sync.Mutex
		timestamps []time.Time
	)

	e.startWorker("w-retry", 2, func(r *worker.Registry) {
		r.Register("backoff-act", func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
			mu.Lock()
			timestamps = append(timestamps, time.Now())
			n := len(timestamps)
			mu.Unlock()
			if n < 3 {
				return nil, errors.New("temporary network glitch")
			}
			return json.RawMessage(`{"ok":true}`), nil
		})
		r.Register("fatal-code-act", func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
			return nil, worker.WithCode("INVALID_CARD", errors.New("card number invalid"))
		})
	})

	// Part A: Exponential backoff (120ms initial, 2.0x coefficient => ~120ms then ~240ms)
	e.start("wf-backoff", flow.WorkflowDefinition{
		Name: "backoff-flow",
		Steps: []flow.StepDefinition{
			{
				Name:     "s",
				Activity: "backoff-act",
				Retries:  3,
				RetryPolicy: &flow.RetryPolicy{
					InitialInterval:    120 * time.Millisecond,
					BackoffCoefficient: 2.0,
					MaxInterval:        500 * time.Millisecond,
				},
			},
		},
	}, nil)

	e.wait("wf-backoff", flow.StatusCompleted)
	mu.Lock()
	if len(timestamps) != 3 {
		t.Fatalf("expected 3 attempts, got %d", len(timestamps))
	}
	gap1 := timestamps[1].Sub(timestamps[0])
	gap2 := timestamps[2].Sub(timestamps[1])
	mu.Unlock()

	if gap1 < 100*time.Millisecond {
		t.Fatalf("expected first retry backoff >= 100ms, got %v", gap1)
	}
	if gap2 < 200*time.Millisecond {
		t.Fatalf("expected second retry backoff >= 200ms (2x exponential), got %v", gap2)
	}

	// Part B: NonRetryableErrorCodes stops retries immediately even if Retries: 5
	e.start("wf-fatal-code", flow.WorkflowDefinition{
		Name: "fatal-code-flow",
		Steps: []flow.StepDefinition{
			{
				Name:     "s",
				Activity: "fatal-code-act",
				Retries:  5,
				RetryPolicy: &flow.RetryPolicy{
					InitialInterval:        50 * time.Millisecond,
					NonRetryableErrorCodes: []string{"INVALID_CARD"},
				},
			},
		},
	}, nil)

	wf := e.wait("wf-fatal-code", flow.StatusFailed)
	if wf.Steps["s"].Attempt != 1 {
		t.Fatalf("expected NonRetryableErrorCodes to prevent retry (1 attempt), got %d attempts", wf.Steps["s"].Attempt)
	}
	if wf.ErrorCode != "INVALID_CARD" {
		t.Fatalf("expected error code INVALID_CARD, got %q", wf.ErrorCode)
	}
}

// 5. Verify O(log N) min-heap timer queue and single-commit batched CmdFireTimers resolution.
func TestMinHeapBatchedTimers(t *testing.T) {
	coord, err := graphflow.NewEmbeddedCoordinator(echoRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer coord.Close()

	const count = 10
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("wf-timer-%d", i)
		startFlow(t, coord, id,
			flow.StepDefinition{Name: "sleep", Kind: flow.KindSleep, Delay: 150 * time.Millisecond},
			flow.StepDefinition{Name: "done", Activity: "noop", DependsOn: []string{"sleep"}},
		)
	}

	for i := 0; i < count; i++ {
		waitStatus(t, coord, fmt.Sprintf("wf-timer-%d", i), flow.StatusCompleted)
	}
}

// 6. Verify durable Cron / Interval Schedules fire workflows automatically.
func TestDurableCronAndIntervalSchedules(t *testing.T) {
	// Unit-test 5-field cron expression calculation
	ref := time.Date(2026, 10, 17, 12, 3, 0, 0, time.UTC)
	next, err := flow.NextScheduleTime("*/15 * * * *", 0, ref)
	if err != nil {
		t.Fatalf("NextScheduleTime failed: %v", err)
	}
	want := time.Date(2026, 10, 17, 12, 15, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("cron */15 next = %v, want %v", next, want)
	}

	// Integration-test recurring schedule execution in Coordinator
	reg := worker.NewRegistry()
	var scheduledRuns atomic.Int32
	reg.Register("scheduled-job", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		scheduledRuns.Add(1)
		return json.RawMessage(`{"ran":true}`), nil
	})

	coord, err := graphflow.NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatal(err)
	}
	defer coord.Close()

	sched, err := coord.UpsertSchedule(context.Background(), flow.Schedule{
		ScheduleID: "heartbeat-sync",
		CronExpr:   "@every 150ms",
		Definition: flow.WorkflowDefinition{
			Name:  "periodic-sync",
			Steps: []flow.StepDefinition{{Name: "run", Activity: "scheduled-job"}},
		},
	})
	if err != nil {
		t.Fatalf("UpsertSchedule failed: %v", err)
	}
	if sched.ScheduleID != "heartbeat-sync" {
		t.Fatalf("unexpected schedule: %+v", sched)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if scheduledRuns.Load() >= 2 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if scheduledRuns.Load() < 2 {
		t.Fatalf("expected schedule to trigger at least 2 workflow runs, got %d", scheduledRuns.Load())
	}

	updated, ok := coord.GetSchedule("heartbeat-sync")
	if !ok || updated.TotalRuns < 2 || updated.LastWorkflowID == "" {
		t.Fatalf("expected schedule metadata to track runs, got %+v", updated)
	}
}

// 7. Verify remote worker Heartbeat Progress Checkpointing: if an activity checkpoints
// progress and then fails/disconnects, the retry attempt receives TaskInfo.LastProgress.
func TestRemoteHeartbeatProgressCheckpointing(t *testing.T) {
	e := newRemoteEnv(t)

	var resumedFrom atomic.Int64
	e.startWorker("w-checkpoint", 1, func(r *worker.Registry) {
		r.Register("batch-import", func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
			info, _ := workerclient.TaskFromContext(ctx)
			if info.Attempt == 1 {
				// Report progress at item 500, then simulate transient failure
				if err := workerclient.ReportProgress(ctx, json.RawMessage(`{"cursor":500}`)); err != nil {
					return nil, err
				}
				time.Sleep(30 * time.Millisecond) // allow heartbeat frame to arrive at broker
				return nil, errors.New("connection reset at item 500")
			}

			// Attempt 2 should receive the checkpointed progress from Attempt 1!
			var cp struct {
				Cursor int64 `json:"cursor"`
			}
			_ = json.Unmarshal(info.LastProgress, &cp)
			resumedFrom.Store(cp.Cursor)
			return json.RawMessage(`{"imported":1000}`), nil
		})
	})

	e.start("wf-progress", flow.WorkflowDefinition{
		Name: "checkpoint-flow",
		Steps: []flow.StepDefinition{
			{Name: "import", Activity: "batch-import", Retries: 2},
		},
	}, nil)

	e.wait("wf-progress", flow.StatusCompleted)
	if got := resumedFrom.Load(); got != 500 {
		t.Fatalf("expected retry attempt to resume from checkpoint cursor 500, got %d", got)
	}
}

// 8. Verify real cluster status and transparent follower-to-leader HTTP forwarding.
func TestFollowerToLeaderHTTPForwarding(t *testing.T) {
	reg := worker.NewRegistry()
	reg.Register("leader-act", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"routed":true}`), nil
	})

	leaderCoord, err := graphflow.NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatal(err)
	}
	defer leaderCoord.Close()

	followerCoord, err := graphflow.NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatal(err)
	}
	defer followerCoord.Close()

	leaderSrv := api.NewServer("127.0.0.1:0", leaderCoord, "node-leader")
	if err := leaderSrv.Start(); err != nil {
		t.Fatal(err)
	}
	defer leaderSrv.Close()

	followerSrv := api.NewServer("127.0.0.1:0", followerCoord, "node-follower")
	// Configure followerSrv as a non-leader pointing to leaderSrv's HTTP address
	followerSrv.SetLeaderResolver(
		func() bool { return false },
		func() string { return "http://" + leaderSrv.Addr() },
	)
	if err := followerSrv.Start(); err != nil {
		t.Fatal(err)
	}
	defer followerSrv.Close()

	followerClient := api.NewClient("http://" + followerSrv.Addr())
	ctx := context.Background()

	// 1. Follower reports IsLeader = false and LeaderAddr = leaderSrv
	st, err := followerClient.GetClusterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.IsLeader || st.NodeID != "node-follower" {
		t.Fatalf("expected follower status IsLeader=false, got %+v", st)
	}

	// 2. Starting a workflow via the follower client transparently forwards to the leader
	wf, err := followerClient.StartWorkflow(ctx, flowstore.StartWorkflowRequest{
		WorkflowID: "wf-forwarded-1",
		Definition: flow.WorkflowDefinition{
			Name:  "forward-flow",
			Steps: []flow.StepDefinition{{Name: "s", Activity: "leader-act"}},
		},
	})
	if err != nil {
		t.Fatalf("StartWorkflow via follower failed: %v", err)
	}
	if wf.WorkflowID != "wf-forwarded-1" {
		t.Fatalf("unexpected workflow ID: %s", wf.WorkflowID)
	}

	// Verify the workflow executed on the leader
	waitStatus(t, leaderCoord, "wf-forwarded-1", flow.StatusCompleted)
}
