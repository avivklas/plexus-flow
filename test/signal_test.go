package test

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/graphflow"
	"github.com/avivklas/plexus-flow/pkg/worker"
)

func echoRegistry() *worker.Registry {
	reg := worker.NewRegistry()
	reg.Register("noop", func(context.Context, json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	return reg
}

func startFlow(t *testing.T, c *graphflow.Coordinator, id string, steps ...flow.StepDefinition) {
	t.Helper()
	if _, err := c.StartWorkflow(context.Background(), flowstore.StartWorkflowRequest{
		WorkflowID: id, Definition: flow.WorkflowDefinition{Name: "signal-flow", Steps: steps},
	}); err != nil {
		t.Fatal(err)
	}
}

// A step can wait for an external signal; the payload becomes its output and
// is kept for later steps.
func TestWaitSignalDeliversPayloadDownstream(t *testing.T) {
	reg := worker.NewRegistry()
	reg.Register("after", func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	})
	coord, err := graphflow.NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatal(err)
	}
	defer coord.Close()

	startFlow(t, coord, "wait-1",
		flow.StepDefinition{Name: "upload", Kind: flow.KindWaitSignal, Signal: "uploaded"},
		flow.StepDefinition{Name: "process", Activity: "after", DependsOn: []string{"upload"}},
	)
	time.Sleep(150 * time.Millisecond)
	wf, _ := coord.GetWorkflow("wait-1")
	if wf.Status != flow.StatusRunning || wf.Steps["upload"].Status != flow.StepStatusRunning || wf.Steps["process"].Status != flow.StepStatusPending {
		t.Fatalf("expected the flow parked on upload, got %s / %s", wf.Status, wf.Steps["upload"].Status)
	}

	if err := coord.SignalWorkflow(context.Background(), "wait-1", "uploaded", json.RawMessage(`{"key":"abc"}`)); err != nil {
		t.Fatal(err)
	}
	wf = waitStatus(t, coord, "wait-1", flow.StatusCompleted)
	if string(wf.Steps["upload"].Output) != `{"key":"abc"}` {
		t.Fatalf("signal payload not kept as step output: %s", wf.Steps["upload"].Output)
	}
}

// A signal that arrives before the step waits for it is not lost.
func TestSignalBeforeWaitIsBuffered(t *testing.T) {
	reg := worker.NewRegistry()
	release := make(chan struct{})
	reg.Register("slow", func(context.Context, json.RawMessage) (json.RawMessage, error) {
		<-release
		return json.RawMessage(`{}`), nil
	})
	coord, err := graphflow.NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatal(err)
	}
	defer coord.Close()

	startFlow(t, coord, "early",
		flow.StepDefinition{Name: "slow", Activity: "slow"},
		flow.StepDefinition{Name: "wait", Kind: flow.KindWaitSignal, Signal: "go", DependsOn: []string{"slow"}},
		flow.StepDefinition{Name: "done", Activity: "slow", DependsOn: []string{"wait"}},
	)
	if err := coord.SignalWorkflow(context.Background(), "early", "go", json.RawMessage(`1`)); err != nil {
		t.Fatal(err)
	}
	close(release)
	waitStatus(t, coord, "early", flow.StatusCompleted)
}

func TestSignalNobodyWaitsForIsRejected(t *testing.T) {
	coord, err := graphflow.NewEmbeddedCoordinator(echoRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer coord.Close()
	startFlow(t, coord, "typo", flow.StepDefinition{Name: "w", Kind: flow.KindWaitSignal, Signal: "real"})

	err = coord.SignalWorkflow(context.Background(), "typo", "fake", nil)
	if err == nil || !strings.Contains(err.Error(), flow.ErrUnknownSignal.Error()) {
		t.Fatalf("expected an unknown-signal error, got %v", err)
	}
}

func TestSleepStepWaitsItsDelay(t *testing.T) {
	coord, err := graphflow.NewEmbeddedCoordinator(echoRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer coord.Close()

	started := time.Now()
	startFlow(t, coord, "nap",
		flow.StepDefinition{Name: "nap", Kind: flow.KindSleep, Delay: 300 * time.Millisecond},
		flow.StepDefinition{Name: "after", Activity: "noop", DependsOn: []string{"nap"}},
	)
	waitStatus(t, coord, "nap", flow.StatusCompleted)
	if elapsed := time.Since(started); elapsed < 300*time.Millisecond {
		t.Fatalf("finished after %s, before the sleep was over", elapsed)
	}
}

// Missing the signal in time fails the step with a code and rolls back what came before.
func TestWaitSignalTimeoutCompensates(t *testing.T) {
	reg := worker.NewRegistry()
	var undone atomic.Int32
	reg.Register("do", func(context.Context, json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	reg.Register("undo", func(context.Context, json.RawMessage) (json.RawMessage, error) {
		undone.Add(1)
		return json.RawMessage(`{}`), nil
	})
	coord, err := graphflow.NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatal(err)
	}
	defer coord.Close()

	startFlow(t, coord, "late",
		flow.StepDefinition{Name: "reserve", Activity: "do", CompensatingAction: "undo"},
		flow.StepDefinition{Name: "wait", Kind: flow.KindWaitSignal, Signal: "paid", Timeout: 200 * time.Millisecond, DependsOn: []string{"reserve"}},
	)
	wf := waitStatus(t, coord, "late", flow.StatusCompensated)
	if wf.ErrorCode != flow.CodeSignalTimeout || undone.Load() != 1 {
		t.Fatalf("code=%q undone=%d", wf.ErrorCode, undone.Load())
	}
	if wf.Steps["wait"].Status != flow.StepStatusFailed {
		t.Fatalf("wait step is %s", wf.Steps["wait"].Status)
	}
}

// Cancelling a workflow parked on a wait compensates and never fires its timer.
func TestCancelWhileWaiting(t *testing.T) {
	reg := worker.NewRegistry()
	var undone atomic.Int32
	reg.Register("do", func(context.Context, json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	reg.Register("undo", func(context.Context, json.RawMessage) (json.RawMessage, error) {
		undone.Add(1)
		return json.RawMessage(`{}`), nil
	})
	coord, err := graphflow.NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatal(err)
	}
	defer coord.Close()

	startFlow(t, coord, "park",
		flow.StepDefinition{Name: "reserve", Activity: "do", CompensatingAction: "undo"},
		flow.StepDefinition{Name: "wait", Kind: flow.KindWaitSignal, Signal: "x", Timeout: 400 * time.Millisecond, DependsOn: []string{"reserve"}},
	)
	time.Sleep(150 * time.Millisecond)
	if err := coord.CancelWorkflow(context.Background(), "park", "changed my mind"); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, coord, "park", flow.StatusCompensated)
	time.Sleep(500 * time.Millisecond)
	wf, _ := coord.GetWorkflow("park")
	if wf.ErrorCode != "" || wf.Steps["wait"].Status != flow.StepStatusSkipped || undone.Load() != 1 {
		t.Fatalf("timer fired after cancel: code=%q wait=%s undone=%d", wf.ErrorCode, wf.Steps["wait"].Status, undone.Load())
	}
}

func TestSignalToFinishedWorkflowIsIgnored(t *testing.T) {
	coord, err := graphflow.NewEmbeddedCoordinator(echoRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer coord.Close()
	startFlow(t, coord, "done", flow.StepDefinition{Name: "w", Kind: flow.KindWaitSignal, Signal: "s"})
	if err := coord.SignalWorkflow(context.Background(), "done", "s", nil); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, coord, "done", flow.StatusCompleted)
	if err := coord.SignalWorkflow(context.Background(), "done", "s", nil); err != nil {
		t.Fatalf("a duplicate signal should be harmless, got %v", err)
	}
}

func TestStepKindValidation(t *testing.T) {
	for name, def := range map[string]flow.StepDefinition{
		"wait without signal": {Name: "a", Kind: flow.KindWaitSignal},
		"sleep without delay": {Name: "a", Kind: flow.KindSleep},
		"wait compensation":   {Name: "a", Kind: flow.KindWaitSignal, Signal: "s", CompensatingAction: "x"},
		"unknown kind":        {Name: "a", Kind: "teleport", Activity: "x"},
	} {
		d := flow.WorkflowDefinition{Name: "v", Steps: []flow.StepDefinition{def}}
		if err := d.Validate(); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}
