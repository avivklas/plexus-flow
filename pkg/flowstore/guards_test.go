package flowstore

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus/pkg/store"
)

func exec[T any](t *testing.T, st *Store, typ store.CommandType, req T) any {
	t.Helper()
	cmd, err := store.NewCommand(typ, req)
	if err != nil {
		t.Fatalf("NewCommand %s: %v", typ, err)
	}
	res, err := st.Router().Execute(context.Background(), cmd)
	if err != nil {
		t.Fatalf("Execute %s: %v", typ, err)
	}
	return res
}

func twoStepFlow() flow.WorkflowDefinition {
	return flow.WorkflowDefinition{
		Name: "two-step",
		Steps: []flow.StepDefinition{
			{Name: "a", Activity: "act-a", CompensatingAction: "undo-a"},
			{Name: "b", Activity: "act-b", CompensatingAction: "undo-b", DependsOn: []string{"a"}},
		},
	}
}

func start(t *testing.T, st *Store, id string, def flow.WorkflowDefinition) {
	t.Helper()
	exec(t, st, CmdStartWorkflow, StartWorkflowRequest{WorkflowID: id, Definition: def})
}

func dispatch(t *testing.T, st *Store, id, step string) {
	t.Helper()
	exec(t, st, CmdDispatchStep, DispatchStepRequest{WorkflowID: id, StepName: step})
}

func complete(t *testing.T, st *Store, id, step string) {
	t.Helper()
	exec(t, st, CmdCompleteStep, CompleteStepRequest{WorkflowID: id, StepName: step, Output: json.RawMessage(`{"ok":true}`)})
}

func TestLateCompletionAfterCancelIsIgnored(t *testing.T) {
	st := New()
	start(t, st, "wf", twoStepFlow())
	dispatch(t, st, "wf", "a")
	complete(t, st, "wf", "a")
	dispatch(t, st, "wf", "b")

	exec(t, st, CmdCancelWorkflow, CancelWorkflowRequest{WorkflowID: "wf", Reason: "user"})
	wf, _ := st.GetWorkflow("wf")
	if wf.Status != flow.StatusCompensating {
		t.Fatalf("cancel with a completed compensable step must compensate, got %s", wf.Status)
	}

	// the worker running step b did not notice the cancellation and reports success
	complete(t, st, "wf", "b")
	wf, _ = st.GetWorkflow("wf")
	if wf.Status != flow.StatusCompensating {
		t.Fatalf("late completion must not change the workflow status, got %s", wf.Status)
	}
	if got := wf.Steps["b"].Status; got != flow.StepStatusSkipped {
		t.Fatalf("a cancelled step must stay SKIPPED, got %s", got)
	}
}

func TestResultsOnTerminalWorkflowAreIgnored(t *testing.T) {
	st := New()
	start(t, st, "wf", flow.WorkflowDefinition{Name: "one", Steps: []flow.StepDefinition{{Name: "a", Activity: "act-a"}}})
	dispatch(t, st, "wf", "a")
	exec(t, st, CmdCancelWorkflow, CancelWorkflowRequest{WorkflowID: "wf", Reason: "user"})

	wf, _ := st.GetWorkflow("wf")
	if wf.Status != flow.StatusCancelled {
		t.Fatalf("expected CANCELLED, got %s", wf.Status)
	}
	complete(t, st, "wf", "a")
	exec(t, st, CmdFailStep, FailStepRequest{WorkflowID: "wf", StepName: "a", Error: "late"})

	wf, _ = st.GetWorkflow("wf")
	if wf.Status != flow.StatusCancelled || wf.Steps["a"].Status != flow.StepStatusSkipped {
		t.Fatalf("terminal workflow must not change, got workflow=%s step=%s", wf.Status, wf.Steps["a"].Status)
	}
}

func TestRetryableFailureConsumesRetriesNotAttempts(t *testing.T) {
	st := New()
	start(t, st, "wf", flow.WorkflowDefinition{Name: "retry", Steps: []flow.StepDefinition{{Name: "a", Activity: "act-a", Retries: 2}}})

	// Retries: 2 means three attempts in total.
	for attempt := 1; attempt <= 3; attempt++ {
		dispatch(t, st, "wf", "a")
		exec(t, st, CmdFailStep, FailStepRequest{WorkflowID: "wf", StepName: "a", Error: "boom", Retryable: true})
		wf, _ := st.GetWorkflow("wf")
		want := flow.StepStatusPending
		if attempt == 3 {
			want = flow.StepStatusFailed
		}
		if got := wf.Steps["a"].Status; got != want {
			t.Fatalf("after attempt %d: step status %s, want %s", attempt, got, want)
		}
	}
	wf, _ := st.GetWorkflow("wf")
	if wf.Status != flow.StatusFailed {
		t.Fatalf("workflow should fail after the last attempt, got %s", wf.Status)
	}
}

func TestNonRetryableFailureIgnoresRetries(t *testing.T) {
	st := New()
	start(t, st, "wf", flow.WorkflowDefinition{Name: "retry", Steps: []flow.StepDefinition{{Name: "a", Activity: "act-a", Retries: 5}}})
	dispatch(t, st, "wf", "a")
	exec(t, st, CmdFailStep, FailStepRequest{WorkflowID: "wf", StepName: "a", Error: "bad input", Retryable: false})
	wf, _ := st.GetWorkflow("wf")
	if wf.Status != flow.StatusFailed {
		t.Fatalf("non-retryable failure must fail the workflow, got %s", wf.Status)
	}
}

func TestFailedCompensationIsTerminal(t *testing.T) {
	st := New()
	start(t, st, "wf", twoStepFlow())
	dispatch(t, st, "wf", "a")
	complete(t, st, "wf", "a")
	dispatch(t, st, "wf", "b")
	exec(t, st, CmdFailStep, FailStepRequest{WorkflowID: "wf", StepName: "b", Error: "boom"})
	wf, _ := st.GetWorkflow("wf")
	if wf.Status != flow.StatusCompensating {
		t.Fatalf("expected COMPENSATING, got %s", wf.Status)
	}

	exec(t, st, CmdStartCompensationStep, StartCompensationStepRequest{WorkflowID: "wf", StepName: "a"})
	exec(t, st, CmdFailCompensationStep, FailCompensationStepRequest{WorkflowID: "wf", StepName: "a", Error: "undo failed"})
	wf, _ = st.GetWorkflow("wf")
	if wf.Status != flow.StatusCompensationFailed || !wf.IsTerminal() {
		t.Fatalf("a failed compensation must end the workflow as COMPENSATION_FAILED, got %s", wf.Status)
	}
	if wf.Steps["a"].Status != flow.StepStatusCompensationFailed {
		t.Fatalf("step a should be COMPENSATION_FAILED, got %s", wf.Steps["a"].Status)
	}
}

// Concurrent commits can each see a step as ready; only the first dispatch may start it.
func TestDispatchIsIdempotent(t *testing.T) {
	st := New()
	start(t, st, "wf", twoStepFlow())
	dispatch(t, st, "wf", "a")

	cmd, _ := store.NewCommand(CmdDispatchStep, DispatchStepRequest{WorkflowID: "wf", StepName: "a"})
	if _, err := st.Router().Execute(context.Background(), cmd); !errors.Is(err, ErrStepNotPending) {
		t.Fatalf("second dispatch: got err %v, want ErrStepNotPending", err)
	}
	wf, _ := st.GetWorkflow("wf")
	if got := wf.Steps["a"].Attempt; got != 1 {
		t.Fatalf("attempt = %d after duplicate dispatch, want 1", got)
	}
}

// A duplicate dispatch must not reopen a signal wait that already consumed its buffered signal.
func TestDuplicateDispatchDoesNotReopenSignalWait(t *testing.T) {
	st := New()
	def := flow.WorkflowDefinition{Name: "gate", Steps: []flow.StepDefinition{
		{Name: "wait", Kind: flow.KindWaitSignal, Signal: "approval"},
		{Name: "after", Activity: "act", DependsOn: []string{"wait"}},
	}}
	start(t, st, "wf", def)
	exec(t, st, CmdSignalWorkflow, SignalWorkflowRequest{WorkflowID: "wf", SignalName: "approval", Payload: json.RawMessage(`{"ok":true}`)})
	dispatch(t, st, "wf", "wait") // consumes the buffered signal and completes

	cmd, _ := store.NewCommand(CmdDispatchStep, DispatchStepRequest{WorkflowID: "wf", StepName: "wait"})
	_, _ = st.Router().Execute(context.Background(), cmd)

	wf, _ := st.GetWorkflow("wf")
	if s := wf.Steps["wait"]; s.Status != flow.StepStatusCompleted || s.Attempt != 1 {
		t.Fatalf("wait step = %s (attempt %d), want COMPLETED (attempt 1)", s.Status, s.Attempt)
	}
}
