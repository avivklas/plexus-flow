package flowstore

import (
	"context"
	"testing"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus/pkg/store"
)

func recv(t *testing.T, ch <-chan *flow.WorkflowInstance) *flow.WorkflowInstance {
	t.Helper()
	select {
	case wf, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed unexpectedly")
		}
		return wf
	case <-time.After(time.Second):
		t.Fatal("no change delivered")
	}
	return nil
}

func TestWatchDeliversEveryChangeWithIncreasingVersions(t *testing.T) {
	st := New()
	ch, cancel := st.Watch(func(wf *flow.WorkflowInstance) bool { return wf.WorkflowID == "wf" }, 16)
	defer cancel()

	start(t, st, "wf", twoStepFlow())
	dispatch(t, st, "wf", "a")
	complete(t, st, "wf", "a")

	var versions []uint64
	for range 3 {
		versions = append(versions, recv(t, ch).Version)
	}
	if versions[0] != 1 || versions[1] != 2 || versions[2] != 3 {
		t.Fatalf("versions = %v, want [1 2 3]", versions)
	}
	if wf, _ := st.GetWorkflow("wf"); wf.Version != 3 {
		t.Fatalf("stored version = %d, want 3", wf.Version)
	}
}

func TestWatchIgnoresOtherWorkflowsAndNoOps(t *testing.T) {
	st := New()
	ch, cancel := st.Watch(func(wf *flow.WorkflowInstance) bool { return wf.WorkflowID == "mine" }, 16)
	defer cancel()

	start(t, st, "other", twoStepFlow())
	start(t, st, "mine", twoStepFlow())
	recv(t, ch) // start of "mine"

	// A rejected duplicate dispatch changes nothing, so nothing is published.
	dispatch(t, st, "mine", "a")
	recv(t, ch)
	cmd, _ := store.NewCommand(CmdDispatchStep, DispatchStepRequest{WorkflowID: "mine", StepName: "a"})
	_, _ = st.Router().Execute(context.Background(), cmd)

	select {
	case wf := <-ch:
		t.Fatalf("unexpected change published: version %d", wf.Version)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSlowWatcherIsDroppedNotBlocking(t *testing.T) {
	st := New()
	ch, cancel := st.Watch(nil, 1)
	defer cancel()

	start(t, st, "wf", twoStepFlow()) // fills the buffer
	dispatch(t, st, "wf", "a")        // does not fit: the watcher is dropped, the command still applies

	<-ch
	if _, open := <-ch; open {
		t.Fatal("slow watcher should have been closed")
	}
	if wf, _ := st.GetWorkflow("wf"); wf.Steps["a"].Status != flow.StepStatusRunning {
		t.Fatal("state machine must not be held back by a slow watcher")
	}
}

func TestWatchSummaries(t *testing.T) {
	st := New()
	ch, cancel := st.WatchSummaries(func(wf *flow.WorkflowInstance) bool { return wf.IsTerminal() }, 16)
	defer cancel()

	start(t, st, "wf", twoStepFlow())
	dispatch(t, st, "wf", "a")
	complete(t, st, "wf", "a")
	dispatch(t, st, "wf", "b")
	complete(t, st, "wf", "b")

	select {
	case sum := <-ch:
		if sum.WorkflowID != "wf" || sum.Status != flow.StatusCompleted || sum.EndedAt == nil {
			t.Fatalf("unexpected summary %+v", sum)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal summary not delivered")
	}
}
