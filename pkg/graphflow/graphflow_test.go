package graphflow

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/worker"
	"github.com/avivklas/plexus/pkg/dedup"
	"github.com/avivklas/plexus/pkg/store"
)

func TestCoordinatorSequentialWorkflow(t *testing.T) {
	reg := worker.NewRegistry()

	var (
		step1Executed atomic.Int32
		step2Executed atomic.Int32
	)

	reg.Register("step-1-act", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		step1Executed.Add(1)
		return json.RawMessage(`{"step1": "done"}`), nil
	})

	reg.Register("step-2-act", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		step2Executed.Add(1)
		return json.RawMessage(`{"step2": "done"}`), nil
	})

	coord, err := NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatalf("NewEmbeddedCoordinator failed: %v", err)
	}
	defer coord.Close()

	def := flow.WorkflowDefinition{
		Name: "two-step-pipeline",
		Steps: []flow.StepDefinition{
			{Name: "step-1", Activity: "step-1-act"},
			{Name: "step-2", Activity: "step-2-act"},
		},
	}

	ctx := context.Background()
	_, err = coord.StartWorkflow(ctx, flowstore.StartWorkflowRequest{
		WorkflowID: "wf-seq-test-1",
		Definition: def,
		Input:      json.RawMessage(`{"hello": "world"}`),
	})
	if err != nil {
		t.Fatalf("StartWorkflow failed: %v", err)
	}

	// Wait for workflow to finish
	deadline := time.Now().Add(5 * time.Second)
	var finalWf *flow.WorkflowInstance
	for time.Now().Before(deadline) {
		wf, exists := coord.GetWorkflow("wf-seq-test-1")
		if exists && wf.Status == flow.StatusCompleted {
			finalWf = wf
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if finalWf == nil {
		wf, _ := coord.GetWorkflow("wf-seq-test-1")
		t.Fatalf("workflow did not complete in time, status: %v", wf.Status)
	}

	if step1Executed.Load() != 1 {
		t.Fatalf("expected step 1 to execute once, got %d", step1Executed.Load())
	}
	if step2Executed.Load() != 1 {
		t.Fatalf("expected step 2 to execute once, got %d", step2Executed.Load())
	}
}

func TestCoordinatorExactlyOnceSemantics(t *testing.T) {
	reg := worker.NewRegistry()
	var actRuns atomic.Int32

	reg.Register("idempotent-act", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		actRuns.Add(1)
		return json.RawMessage(`{"status": "ok"}`), nil
	})

	coord, err := NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatalf("NewEmbeddedCoordinator failed: %v", err)
	}
	defer coord.Close()

	def := flow.WorkflowDefinition{
		Name: "single-step-pipeline",
		Steps: []flow.StepDefinition{
			{Name: "step-1", Activity: "idempotent-act"},
		},
	}

	ctx := context.Background()
	_, err = coord.StartWorkflow(ctx, flowstore.StartWorkflowRequest{
		WorkflowID: "wf-eos-1",
		Definition: def,
	})
	if err != nil {
		t.Fatalf("StartWorkflow failed: %v", err)
	}

	// Wait for step to run once
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if actRuns.Load() == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if actRuns.Load() != 1 {
		t.Fatalf("expected activity to run once, got %d", actRuns.Load())
	}

	// Find the idempotency key saved in downstream DedupStore
	snap, err := coord.DedupStore().Snapshot()
	if err != nil {
		t.Fatalf("DedupStore.Snapshot failed: %v", err)
	}
	var records map[string]dedup.Record
	if err := json.Unmarshal(snap, &records); err != nil {
		t.Fatalf("unmarshal dedup records: %v", err)
	}

	var recordedKey string
	for k := range records {
		recordedKey = k
		break
	}
	if recordedKey == "" {
		t.Fatalf("expected idempotency key recorded in downstream dedup store")
	}

	// Duplicate dispatch attack: Re-apply duplicate activity dispatch command 5 times!
	duplicateCmd, err := store.NewCommand(worker.CmdDispatchActivity, worker.ActivityTask{
		WorkflowID: "wf-eos-1",
		StepName:   "step-1",
		Activity:   "idempotent-act",
	})
	if err != nil {
		t.Fatalf("NewCommand failed: %v", err)
	}
	duplicateCmd.SetIdempotencyKey(recordedKey)

	for i := 0; i < 5; i++ {
		res, err := coord.cfg.Downstream.ApplyCommand(ctx, duplicateCmd)
		if err != nil {
			t.Fatalf("ApplyCommand duplicate failed: %v", err)
		}
		if res != "DISPATCHED" {
			t.Errorf("expected cached response 'DISPATCHED', got %v", res)
		}
	}

	// Allow some time to ensure no extra executions happened
	time.Sleep(100 * time.Millisecond)

	// Verify actRuns is STILL strictly 1!
	if actRuns.Load() != 1 {
		t.Fatalf("EOS VIOLATED: expected activity to execute exactly 1 time, but executed %d times", actRuns.Load())
	}
}

func TestCoordinatorSagaRollback(t *testing.T) {
	reg := worker.NewRegistry()

	var (
		reserveRun atomic.Int32
		chargeRun  atomic.Int32
		releaseRun atomic.Int32
		refundRun  atomic.Int32
	)

	reg.Register("reserve-inventory", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		reserveRun.Add(1)
		return json.RawMessage(`{"reserved": true}`), nil
	})

	reg.Register("release-inventory", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		releaseRun.Add(1)
		return json.RawMessage(`{"released": true}`), nil
	})

	reg.Register("charge-card", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		chargeRun.Add(1)
		return nil, errors.New("insufficient credit limit")
	})

	reg.Register("refund-card", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		refundRun.Add(1)
		return json.RawMessage(`{"refunded": true}`), nil
	})

	coord, err := NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatalf("NewEmbeddedCoordinator failed: %v", err)
	}
	defer coord.Close()

	def := flow.WorkflowDefinition{
		Name: "order-saga",
		Steps: []flow.StepDefinition{
			{
				Name:               "step-reserve",
				Activity:           "reserve-inventory",
				CompensatingAction: "release-inventory",
			},
			{
				Name:               "step-charge",
				Activity:           "charge-card",
				CompensatingAction: "refund-card",
				Retries:            0,
			},
		},
	}

	ctx := context.Background()
	_, err = coord.StartWorkflow(ctx, flowstore.StartWorkflowRequest{
		WorkflowID: "wf-saga-order-1",
		Definition: def,
	})
	if err != nil {
		t.Fatalf("StartWorkflow failed: %v", err)
	}

	// Wait for compensation to complete
	deadline := time.Now().Add(5 * time.Second)
	var finalWf *flow.WorkflowInstance
	for time.Now().Before(deadline) {
		wf, exists := coord.GetWorkflow("wf-saga-order-1")
		if exists && wf.Status == flow.StatusCompensated {
			finalWf = wf
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if finalWf == nil {
		wf, _ := coord.GetWorkflow("wf-saga-order-1")
		t.Fatalf("workflow did not reach COMPENSATED in time, status: %v", wf.Status)
	}

	if reserveRun.Load() != 1 {
		t.Fatalf("expected reserve to run once, got %d", reserveRun.Load())
	}
	if chargeRun.Load() != 1 {
		t.Fatalf("expected charge to run once, got %d", chargeRun.Load())
	}
	if releaseRun.Load() != 1 {
		t.Fatalf("expected release-inventory to be compensated once, got %d", releaseRun.Load())
	}
	if refundRun.Load() != 0 {
		t.Fatalf("expected refund-card not to run since charge-card failed, got %d", refundRun.Load())
	}
}
