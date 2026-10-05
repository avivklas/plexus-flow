package test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avivklas/plexus/pkg/store"
	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/graphflow"
	"github.com/avivklas/plexus-flow/pkg/worker"
)

// Order processing saga definition helper
func createOrderSagaDefinition() flow.WorkflowDefinition {
	return flow.WorkflowDefinition{
		Name: "order-saga",
		Steps: []flow.StepDefinition{
			{
				Name:               "reserve-inventory",
				Activity:           "reserve-inventory",
				CompensatingAction: "release-inventory",
				Retries:            0,
			},
			{
				Name:               "charge-card",
				Activity:           "charge-card",
				CompensatingAction: "refund-card",
				Retries:            0,
			},
			{
				Name:               "ship-item",
				Activity:           "ship-item",
				CompensatingAction: "cancel-shipment",
				Retries:            0,
			},
		},
	}
}

func TestSagaOrderProcessingSuccess(t *testing.T) {
	reg := worker.NewRegistry()

	var (
		resInv  atomic.Int32
		chgCard atomic.Int32
		shipItm atomic.Int32
		relInv  atomic.Int32
		refCard atomic.Int32
		cncShip atomic.Int32
	)

	reg.Register("reserve-inventory", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		resInv.Add(1)
		return json.RawMessage(`{"reserved": true, "sku": "ITEM-101"}`), nil
	})
	reg.Register("charge-card", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		chgCard.Add(1)
		return json.RawMessage(`{"charged": true, "amount": 99.99}`), nil
	})
	reg.Register("ship-item", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		shipItm.Add(1)
		return json.RawMessage(`{"shipped": true, "tracking": "TRK-9901"}`), nil
	})

	reg.Register("release-inventory", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		relInv.Add(1)
		return json.RawMessage(`{"released": true}`), nil
	})
	reg.Register("refund-card", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		refCard.Add(1)
		return json.RawMessage(`{"refunded": true}`), nil
	})
	reg.Register("cancel-shipment", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		cncShip.Add(1)
		return json.RawMessage(`{"cancelled": true}`), nil
	})

	coord, err := graphflow.NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatalf("NewEmbeddedCoordinator failed: %v", err)
	}
	defer coord.Close()

	ctx := context.Background()
	_, err = coord.StartWorkflow(ctx, flowstore.StartWorkflowRequest{
		WorkflowID: "order-success-1",
		Definition: createOrderSagaDefinition(),
		Input:      json.RawMessage(`{"order_id": "ORD-001"}`),
	})
	if err != nil {
		t.Fatalf("StartWorkflow failed: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var finalWf *flow.WorkflowInstance
	for time.Now().Before(deadline) {
		wf, exists := coord.GetWorkflow("order-success-1")
		if exists && wf.Status == flow.StatusCompleted {
			finalWf = wf
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if finalWf == nil {
		wf, _ := coord.GetWorkflow("order-success-1")
		t.Fatalf("workflow did not complete, status: %v", wf.Status)
	}

	if resInv.Load() != 1 || chgCard.Load() != 1 || shipItm.Load() != 1 {
		t.Fatalf("expected all 3 steps to run once, got: res=%d, chg=%d, ship=%d",
			resInv.Load(), chgCard.Load(), shipItm.Load())
	}
	if relInv.Load() != 0 || refCard.Load() != 0 || cncShip.Load() != 0 {
		t.Fatalf("expected no compensations to run on success")
	}
}

func TestSagaOrderProcessingRollbackStep3Failure(t *testing.T) {
	reg := worker.NewRegistry()

	var (
		resInv   atomic.Int32
		chgCard  atomic.Int32
		shipItm  atomic.Int32
		relInv   atomic.Int32
		refCard  atomic.Int32
		cncShip  atomic.Int32
		orderLog []string
		orderMu  sync.Mutex
	)

	logAction := func(action string) {
		orderMu.Lock()
		defer orderMu.Unlock()
		orderLog = append(orderLog, action)
	}

	reg.Register("reserve-inventory", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		resInv.Add(1)
		logAction("reserve-inventory")
		return json.RawMessage(`{"reserved": true}`), nil
	})
	reg.Register("charge-card", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		chgCard.Add(1)
		logAction("charge-card")
		return json.RawMessage(`{"charged": true}`), nil
	})
	reg.Register("ship-item", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		shipItm.Add(1)
		logAction("ship-item-fail")
		return nil, errors.New("carrier warehouse closed")
	})

	reg.Register("release-inventory", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		relInv.Add(1)
		logAction("release-inventory")
		return json.RawMessage(`{"released": true}`), nil
	})
	reg.Register("refund-card", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		refCard.Add(1)
		logAction("refund-card")
		return json.RawMessage(`{"refunded": true}`), nil
	})
	reg.Register("cancel-shipment", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		cncShip.Add(1)
		logAction("cancel-shipment")
		return json.RawMessage(`{"cancelled": true}`), nil
	})

	coord, err := graphflow.NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatalf("NewEmbeddedCoordinator failed: %v", err)
	}
	defer coord.Close()

	ctx := context.Background()
	_, err = coord.StartWorkflow(ctx, flowstore.StartWorkflowRequest{
		WorkflowID: "order-fail-step3",
		Definition: createOrderSagaDefinition(),
		Input:      json.RawMessage(`{"order_id": "ORD-002"}`),
	})
	if err != nil {
		t.Fatalf("StartWorkflow failed: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var finalWf *flow.WorkflowInstance
	for time.Now().Before(deadline) {
		wf, exists := coord.GetWorkflow("order-fail-step3")
		if exists && wf.Status == flow.StatusCompensated {
			finalWf = wf
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if finalWf == nil {
		wf, _ := coord.GetWorkflow("order-fail-step3")
		t.Fatalf("workflow did not reach COMPENSATED, status: %v, error: %s", wf.Status, wf.Error)
	}

	if resInv.Load() != 1 || chgCard.Load() != 1 || shipItm.Load() != 1 {
		t.Fatalf("expected 3 steps to attempt execution, got: res=%d, chg=%d, ship=%d",
			resInv.Load(), chgCard.Load(), shipItm.Load())
	}

	// Verify reverse-order compensation!
	// Completed: reserve -> charge. Failed: ship.
	// Reverse compensation order MUST BE: refund-card -> release-inventory!
	if refCard.Load() != 1 || relInv.Load() != 1 {
		t.Fatalf("expected refund-card and release-inventory to run, got: ref=%d, rel=%d",
			refCard.Load(), relInv.Load())
	}
	if cncShip.Load() != 0 {
		t.Fatalf("shipment did not complete so should not be compensated")
	}

	orderMu.Lock()
	defer orderMu.Unlock()
	// Check log for exact reverse order
	// Must contain refund-card before release-inventory
	refIdx := -1
	relIdx := -1
	for idx, a := range orderLog {
		if a == "refund-card" {
			refIdx = idx
		}
		if a == "release-inventory" && refIdx != -1 {
			relIdx = idx
		}
	}
	if refIdx == -1 || relIdx == -1 || refIdx >= relIdx {
		t.Fatalf("expected reverse-order compensation (refund-card before release-inventory), got log: %v", orderLog)
	}
}

func TestDAGForkJoinWorkflow(t *testing.T) {
	reg := worker.NewRegistry()

	var (
		actStart   atomic.Int32
		actBranchA atomic.Int32
		actBranchB atomic.Int32
		actJoin    atomic.Int32
	)

	reg.Register("act-start", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		actStart.Add(1)
		return json.RawMessage(`{"val": 10}`), nil
	})
	reg.Register("act-a", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		actBranchA.Add(1)
		return json.RawMessage(`{"a": 20}`), nil
	})
	reg.Register("act-b", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		actBranchB.Add(1)
		return json.RawMessage(`{"b": 30}`), nil
	})
	reg.Register("act-join", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		// Join must only run after BOTH branchA and branchB have completed!
		if actBranchA.Load() == 0 || actBranchB.Load() == 0 {
			return nil, errors.New("join executed before branches completed")
		}
		actJoin.Add(1)
		return json.RawMessage(`{"total": 50}`), nil
	})

	coord, err := graphflow.NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatalf("NewEmbeddedCoordinator failed: %v", err)
	}
	defer coord.Close()

	def := flow.WorkflowDefinition{
		Name: "dag-fork-join",
		Steps: []flow.StepDefinition{
			{Name: "start", Activity: "act-start"},
			{Name: "branchA", Activity: "act-a", DependsOn: []string{"start"}},
			{Name: "branchB", Activity: "act-b", DependsOn: []string{"start"}},
			{Name: "join", Activity: "act-join", DependsOn: []string{"branchA", "branchB"}},
		},
	}

	ctx := context.Background()
	_, err = coord.StartWorkflow(ctx, flowstore.StartWorkflowRequest{
		WorkflowID: "wf-dag-join-1",
		Definition: def,
	})
	if err != nil {
		t.Fatalf("StartWorkflow failed: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var finalWf *flow.WorkflowInstance
	for time.Now().Before(deadline) {
		wf, exists := coord.GetWorkflow("wf-dag-join-1")
		if exists && wf.Status == flow.StatusCompleted {
			finalWf = wf
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if finalWf == nil {
		wf, _ := coord.GetWorkflow("wf-dag-join-1")
		t.Fatalf("DAG workflow did not complete, status: %v", wf.Status)
	}

	if actStart.Load() != 1 || actBranchA.Load() != 1 || actBranchB.Load() != 1 || actJoin.Load() != 1 {
		t.Fatalf("expected all nodes to run once: start=%d, a=%d, b=%d, join=%d",
			actStart.Load(), actBranchA.Load(), actBranchB.Load(), actJoin.Load())
	}
}

func TestWorkflowSnapshotAndRecovery(t *testing.T) {
	st := flowstore.New()
	ctx := context.Background()

	def := flow.WorkflowDefinition{
		Name: "recovery-flow",
		Steps: []flow.StepDefinition{
			{Name: "step-1", Activity: "act-1", CompensatingAction: "undo-1"},
			{Name: "step-2", Activity: "act-2", CompensatingAction: "undo-2"},
		},
	}

	// Start workflow
	startCmd, _ := store.NewCommand(flowstore.CmdStartWorkflow, flowstore.StartWorkflowRequest{
		WorkflowID: "wf-rec-1",
		Definition: def,
		Input:      json.RawMessage(`{"amount": 100}`),
	})
	st.Router().Execute(ctx, startCmd)

	// Complete step 1
	compCmd, _ := store.NewCommand(flowstore.CmdCompleteStep, flowstore.CompleteStepRequest{
		WorkflowID: "wf-rec-1",
		StepName:   "step-1",
		Output:     json.RawMessage(`{"result": "step-1-ok"}`),
	})
	st.Router().Execute(ctx, compCmd)

	// Take snapshot mid-execution
	snap, err := st.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	// Restore into a completely fresh Store (simulating crash recovery)
	freshStore := flowstore.New()
	if err := freshStore.Restore(snap); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	wf, ok := freshStore.GetWorkflow("wf-rec-1")
	if !ok || wf == nil {
		t.Fatalf("expected workflow restored")
	}
	if wf.Status != flow.StatusRunning {
		t.Fatalf("expected status RUNNING, got %v", wf.Status)
	}
	if wf.Steps["step-1"].Status != flow.StepStatusCompleted {
		t.Fatalf("expected step-1 COMPLETED, got %v", wf.Steps["step-1"].Status)
	}
	if wf.Steps["step-2"].Status != flow.StepStatusPending {
		t.Fatalf("expected step-2 PENDING, got %v", wf.Steps["step-2"].Status)
	}

	// Verify next ready steps on restored instance
	ready := wf.NextReadySteps()
	if len(ready) != 1 || ready[0].Name != "step-2" {
		t.Fatalf("expected step-2 to be ready to execute after recovery")
	}

	// Complete step 2 on restored store
	comp2Cmd, _ := store.NewCommand(flowstore.CmdCompleteStep, flowstore.CompleteStepRequest{
		WorkflowID: "wf-rec-1",
		StepName:   "step-2",
		Output:     json.RawMessage(`{"result": "step-2-ok"}`),
	})
	res, err := freshStore.Router().Execute(ctx, comp2Cmd)
	if err != nil {
		t.Fatalf("Execute step-2 completion failed: %v", err)
	}
	finalWf := res.(*flow.WorkflowInstance)
	if finalWf.Status != flow.StatusCompleted {
		t.Fatalf("expected workflow to finish as COMPLETED, got %v", finalWf.Status)
	}
}
