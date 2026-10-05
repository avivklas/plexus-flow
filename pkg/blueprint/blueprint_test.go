package blueprint

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/graphflow"
	"github.com/avivklas/plexus-flow/pkg/worker"
)

func TestBlueprintRegistryDefaults(t *testing.T) {
	reg := NewRegistry()
	list := reg.List()
	if len(list) < 2 {
		t.Fatalf("expected at least 2 default blueprints, got %d", len(list))
	}

	ecom, ok := reg.Get("order-fulfillment-saga")
	if !ok {
		t.Fatalf("expected order-fulfillment-saga blueprint to exist")
	}

	if err := ecom.Definition.Validate(); err != nil {
		t.Fatalf("order-fulfillment-saga DAG is invalid: %v", err)
	}

	order, err := ecom.Definition.TopologicalOrder()
	if err != nil {
		t.Fatalf("failed to calculate topological order: %v", err)
	}
	if len(order) != 4 {
		t.Fatalf("expected 4 steps in topological order, got %d", len(order))
	}
}

func TestBlueprintExecutionHappyPath(t *testing.T) {
	workerReg := worker.NewRegistry()
	RegisterBlueprintActivities(workerReg)

	coord, err := graphflow.NewEmbeddedCoordinator(workerReg)
	if err != nil {
		t.Fatalf("init embedded coordinator: %v", err)
	}
	defer coord.Close()

	bpReg := NewRegistry()
	bp, ok := bpReg.Get("order-fulfillment-saga")
	if !ok {
		t.Fatalf("blueprint not found")
	}

	ctx := context.Background()
	wf, err := coord.StartWorkflow(ctx, flowstore.StartWorkflowRequest{
		WorkflowID: "test-order-happy-1",
		Definition: bp.Definition,
		Input:      json.RawMessage(`{"order_id": "test-order-happy-1", "total_amount": 99.00}`),
	})
	if err != nil {
		t.Fatalf("start workflow: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		inst, exists := coord.GetWorkflow(wf.WorkflowID)
		if exists && inst.IsTerminal() {
			if inst.Status != "COMPLETED" {
				t.Fatalf("expected status COMPLETED, got %s (err: %s)", inst.Status, inst.Error)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for happy path saga execution")
}

func TestBlueprintExecutionSagaRollback(t *testing.T) {
	workerReg := worker.NewRegistry()
	RegisterBlueprintActivities(workerReg)

	coord, err := graphflow.NewEmbeddedCoordinator(workerReg)
	if err != nil {
		t.Fatalf("init embedded coordinator: %v", err)
	}
	defer coord.Close()

	bpReg := NewRegistry()
	bp, ok := bpReg.Get("order-fulfillment-saga")
	if !ok {
		t.Fatalf("blueprint not found")
	}

	ctx := context.Background()
	wf, err := coord.StartWorkflow(ctx, flowstore.StartWorkflowRequest{
		WorkflowID: "test-order-fail-payment",
		Definition: bp.Definition,
		Input:      json.RawMessage(`{"order_id": "test-order-fail-payment", "simulate_fail": "charge-payment"}`),
	})
	if err != nil {
		t.Fatalf("start workflow: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		inst, exists := coord.GetWorkflow(wf.WorkflowID)
		if exists && inst.IsTerminal() {
			if inst.Status != "COMPENSATED" {
				t.Fatalf("expected status COMPENSATED, got %s (err: %s)", inst.Status, inst.Error)
			}
			// Verify that reserve-inventory was compensated
			step := inst.Steps["reserve-inventory"]
			if step.Status != "COMPENSATED" {
				t.Fatalf("expected reserve-inventory to be COMPENSATED, got %s", step.Status)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for saga rollback execution")
}
