package flowstore

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/avivklas/plexus/pkg/store"
	"github.com/avivklas/plexus-flow/pkg/flow"
)

func TestFlowStoreLifecycle(t *testing.T) {
	st := New()
	ctx := context.Background()

	def := flow.WorkflowDefinition{
		Name: "order-flow",
		Steps: []flow.StepDefinition{
			{Name: "reserve", Activity: "reserve-inv", CompensatingAction: "release-inv"},
			{Name: "charge", Activity: "charge-card", CompensatingAction: "refund-card"},
			{Name: "ship", Activity: "ship-item", CompensatingAction: "cancel-ship"},
		},
	}

	// 1. Start Workflow
	startCmd, err := store.NewCommand(CmdStartWorkflow, StartWorkflowRequest{
		WorkflowID: "order-100",
		Definition: def,
		Input:      json.RawMessage(`{"item": "book", "price": 20}`),
	})
	if err != nil {
		t.Fatalf("NewCommand failed: %v", err)
	}

	res, err := st.Router().Execute(ctx, startCmd)
	if err != nil {
		t.Fatalf("Execute CmdStartWorkflow failed: %v", err)
	}
	wf, ok := res.(*flow.WorkflowInstance)
	if !ok || wf.Status != flow.StatusRunning {
		t.Fatalf("expected running workflow, got %v", wf)
	}

	// 2. Dispatch Step 1
	dispCmd, _ := store.NewCommand(CmdDispatchStep, DispatchStepRequest{
		WorkflowID: "order-100",
		StepName:   "reserve",
	})
	_, err = st.Router().Execute(ctx, dispCmd)
	if err != nil {
		t.Fatalf("Execute CmdDispatchStep failed: %v", err)
	}
	inst, _ := st.GetWorkflow("order-100")
	if inst.Steps["reserve"].Status != flow.StepStatusRunning || inst.Steps["reserve"].Attempt != 1 {
		t.Fatalf("expected step running with attempt 1, got %v", inst.Steps["reserve"])
	}

	// 3. Complete Step 1
	compCmd, _ := store.NewCommand(CmdCompleteStep, CompleteStepRequest{
		WorkflowID: "order-100",
		StepName:   "reserve",
		Output:     json.RawMessage(`{"reserved": true}`),
	})
	_, err = st.Router().Execute(ctx, compCmd)
	if err != nil {
		t.Fatalf("Execute CmdCompleteStep failed: %v", err)
	}

	// 4. Complete Step 2
	comp2Cmd, _ := store.NewCommand(CmdCompleteStep, CompleteStepRequest{
		WorkflowID: "order-100",
		StepName:   "charge",
		Output:     json.RawMessage(`{"charged": true}`),
	})
	st.Router().Execute(ctx, comp2Cmd)

	// 5. Complete Step 3 -> Workflow completes!
	comp3Cmd, _ := store.NewCommand(CmdCompleteStep, CompleteStepRequest{
		WorkflowID: "order-100",
		StepName:   "ship",
		Output:     json.RawMessage(`{"tracking": "TRK-123"}`),
	})
	res3, err := st.Router().Execute(ctx, comp3Cmd)
	if err != nil {
		t.Fatalf("Execute CmdCompleteStep step 3 failed: %v", err)
	}
	finalWf := res3.(*flow.WorkflowInstance)
	if finalWf.Status != flow.StatusCompleted {
		t.Fatalf("expected workflow COMPLETED, got %v", finalWf.Status)
	}
	if finalWf.CompletedAt == nil {
		t.Fatalf("expected CompletedAt to be set")
	}
}

func TestFlowStoreSagaRollback(t *testing.T) {
	st := New()
	ctx := context.Background()

	def := flow.WorkflowDefinition{
		Name: "order-flow",
		Steps: []flow.StepDefinition{
			{Name: "reserve", Activity: "reserve-inv", CompensatingAction: "release-inv"},
			{Name: "charge", Activity: "charge-card", CompensatingAction: "refund-card"},
			{Name: "ship", Activity: "ship-item", CompensatingAction: "cancel-ship"},
		},
	}

	startCmd, _ := store.NewCommand(CmdStartWorkflow, StartWorkflowRequest{
		WorkflowID: "order-200",
		Definition: def,
	})
	st.Router().Execute(ctx, startCmd)

	// Complete step 1
	compCmd, _ := store.NewCommand(CmdCompleteStep, CompleteStepRequest{
		WorkflowID: "order-200",
		StepName:   "reserve",
	})
	st.Router().Execute(ctx, compCmd)

	// Complete step 2
	comp2Cmd, _ := store.NewCommand(CmdCompleteStep, CompleteStepRequest{
		WorkflowID: "order-200",
		StepName:   "charge",
	})
	st.Router().Execute(ctx, comp2Cmd)

	// Step 3 fails permanently!
	failCmd, _ := store.NewCommand(CmdFailStep, FailStepRequest{
		WorkflowID: "order-200",
		StepName:   "ship",
		Error:      "out of stock",
		Retryable:  false,
	})
	res, err := st.Router().Execute(ctx, failCmd)
	if err != nil {
		t.Fatalf("Execute CmdFailStep failed: %v", err)
	}
	wf := res.(*flow.WorkflowInstance)
	if wf.Status != flow.StatusCompensating {
		t.Fatalf("expected status COMPENSATING, got %s", wf.Status)
	}

	// Next step to compensate should be 'charge' (latest completed)
	nextComp := wf.NextCompensatingStep()
	if nextComp == nil || nextComp.Name != "charge" {
		t.Fatalf("expected 'charge' next to compensate, got %v", nextComp)
	}

	// Complete compensation of 'charge'
	cCompCmd, _ := store.NewCommand(CmdCompleteCompensationStep, CompleteCompensationStepRequest{
		WorkflowID: "order-200",
		StepName:   "charge",
	})
	resComp, _ := st.Router().Execute(ctx, cCompCmd)
	wf = resComp.(*flow.WorkflowInstance)
	if wf.Status != flow.StatusCompensating {
		t.Fatalf("expected status still COMPENSATING, got %s", wf.Status)
	}

	// Next step to compensate should be 'reserve'
	nextComp = wf.NextCompensatingStep()
	if nextComp == nil || nextComp.Name != "reserve" {
		t.Fatalf("expected 'reserve' next to compensate, got %v", nextComp)
	}

	// Complete compensation of 'reserve' -> now all are compensated!
	cComp2Cmd, _ := store.NewCommand(CmdCompleteCompensationStep, CompleteCompensationStepRequest{
		WorkflowID: "order-200",
		StepName:   "reserve",
	})
	resComp2, _ := st.Router().Execute(ctx, cComp2Cmd)
	wf = resComp2.(*flow.WorkflowInstance)
	if wf.Status != flow.StatusCompensated {
		t.Fatalf("expected status COMPENSATED, got %s", wf.Status)
	}
}

func TestFlowStoreSnapshotAndRestore(t *testing.T) {
	st := New()
	ctx := context.Background()

	def := flow.WorkflowDefinition{
		Name: "test-snap",
		Steps: []flow.StepDefinition{
			{Name: "step-1", Activity: "act-1"},
		},
	}

	startCmd, _ := store.NewCommand(CmdStartWorkflow, StartWorkflowRequest{
		WorkflowID: "wf-snap-1",
		Definition: def,
	})
	st.Router().Execute(ctx, startCmd)

	// Take snapshot
	snap, err := st.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	// Restore into a new store
	st2 := New()
	if err := st2.Restore(snap); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	wf, ok := st2.GetWorkflow("wf-snap-1")
	if !ok || wf == nil {
		t.Fatalf("expected restored workflow wf-snap-1")
	}
	if wf.DefinitionName != "test-snap" || wf.Status != flow.StatusRunning {
		t.Fatalf("unexpected restored state: %+v", wf)
	}
}
