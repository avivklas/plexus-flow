package test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/graphflow"
	"github.com/avivklas/plexus-flow/pkg/worker"
)

func waitStatus(t *testing.T, c *graphflow.Coordinator, id string, want flow.Status) *flow.WorkflowInstance {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if wf, ok := c.GetWorkflow(id); ok && wf.Status == want {
			return wf
		}
		time.Sleep(20 * time.Millisecond)
	}
	wf, _ := c.GetWorkflow(id)
	t.Fatalf("workflow %s did not reach %s, got %s (%s)", id, want, wf.Status, wf.Error)
	return nil
}

// Cancelling a running workflow compensates what already completed, and a
// result reported late by the cancelled step changes nothing.
func TestCancelCompensatesCompletedSteps(t *testing.T) {
	reg := worker.NewRegistry()
	release := make(chan struct{})
	var undoA, undoB atomic.Int32
	running := make(chan struct{})

	reg.Register("act-a", func(context.Context, json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	reg.Register("act-b", func(context.Context, json.RawMessage) (json.RawMessage, error) {
		close(running)
		<-release // ignores cancellation, like a worker that never learns about it
		return json.RawMessage(`{}`), nil
	})
	reg.Register("undo-a", func(context.Context, json.RawMessage) (json.RawMessage, error) {
		undoA.Add(1)
		return json.RawMessage(`{}`), nil
	})
	reg.Register("undo-b", func(context.Context, json.RawMessage) (json.RawMessage, error) {
		undoB.Add(1)
		return json.RawMessage(`{}`), nil
	})

	coord, err := graphflow.NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatal(err)
	}
	defer coord.Close()

	_, err = coord.StartWorkflow(context.Background(), flowstore.StartWorkflowRequest{
		WorkflowID: "cancel-me",
		Definition: flow.WorkflowDefinition{Name: "cancel-flow", Steps: []flow.StepDefinition{
			{Name: "a", Activity: "act-a", CompensatingAction: "undo-a"},
			{Name: "b", Activity: "act-b", CompensatingAction: "undo-b", DependsOn: []string{"a"}, Timeout: 10 * time.Second},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	<-running
	if err := coord.CancelWorkflow(context.Background(), "cancel-me", "no longer needed"); err != nil {
		t.Fatal(err)
	}

	wf := waitStatus(t, coord, "cancel-me", flow.StatusCompensated)
	if undoA.Load() != 1 || undoB.Load() != 0 {
		t.Fatalf("expected undo-a once and undo-b never, got a=%d b=%d", undoA.Load(), undoB.Load())
	}

	close(release) // the cancelled step now reports success
	time.Sleep(200 * time.Millisecond)
	wf, _ = coord.GetWorkflow("cancel-me")
	if wf.Status != flow.StatusCompensated || wf.Steps["b"].Status != flow.StepStatusSkipped {
		t.Fatalf("late result changed the workflow: status=%s step b=%s", wf.Status, wf.Steps["b"].Status)
	}
	if undoB.Load() != 0 {
		t.Fatalf("undo-b must not run for a skipped step")
	}
}
