package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus/pkg/store"
)

type mockApplier struct {
	mu       sync.Mutex
	applied  []store.CommandType
	requests []any
}

func (m *mockApplier) Apply(ctx context.Context, cmdType store.CommandType, data any) (any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applied = append(m.applied, cmdType)
	m.requests = append(m.requests, data)
	return "OK", nil
}

func TestWorkerExecutionAndRetry(t *testing.T) {
	reg := NewRegistry()
	var attempts atomic.Int32

	reg.Register("flakey-task", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		att := attempts.Add(1)
		if att < 3 {
			return nil, errors.New("network glitch")
		}
		return json.RawMessage(`{"success": true}`), nil
	})

	applier := &mockApplier{}
	exec := NewExecutor(reg, applier)
	defer exec.Close()

	task := ActivityTask{
		WorkflowID: "wf-retry",
		RunID:      "run-1",
		StepName:   "step-retry",
		Activity:   "flakey-task",
		Retries:    3,
		Timeout:    2 * time.Second,
	}

	exec.ExecuteActivity(task)

	// Wait for execution to finish
	deadline := time.Now().Add(2 * time.Second)
	for {
		applier.mu.Lock()
		count := len(applier.applied)
		applier.mu.Unlock()
		if count > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	applier.mu.Lock()
	defer applier.mu.Unlock()

	if len(applier.applied) != 1 {
		t.Fatalf("expected 1 report, got %d", len(applier.applied))
	}
	if applier.applied[0] != flowstore.CmdCompleteStep {
		t.Fatalf("expected CmdCompleteStep, got %v", applier.applied[0])
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
	}
}

func TestWorkerTimeout(t *testing.T) {
	reg := NewRegistry()
	reg.Register("slow-task", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return json.RawMessage(`{"done": true}`), nil
		}
	})

	applier := &mockApplier{}
	exec := NewExecutor(reg, applier)
	defer exec.Close()

	task := ActivityTask{
		WorkflowID: "wf-timeout",
		RunID:      "run-1",
		StepName:   "step-slow",
		Activity:   "slow-task",
		Retries:    0,
		Timeout:    50 * time.Millisecond, // very short timeout
	}

	exec.ExecuteActivity(task)

	deadline := time.Now().Add(1 * time.Second)
	for {
		applier.mu.Lock()
		count := len(applier.applied)
		applier.mu.Unlock()
		if count > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	applier.mu.Lock()
	defer applier.mu.Unlock()

	if len(applier.applied) != 1 {
		t.Fatalf("expected 1 report, got %d", len(applier.applied))
	}
	if applier.applied[0] != flowstore.CmdFailStep {
		t.Fatalf("expected CmdFailStep on timeout, got %v", applier.applied[0])
	}
}

func TestWorkerCompensation(t *testing.T) {
	reg := NewRegistry()
	var compensated atomic.Bool

	reg.Register("undo-something", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		compensated.Store(true)
		return json.RawMessage(`{"undone": true}`), nil
	})

	applier := &mockApplier{}
	exec := NewExecutor(reg, applier)
	defer exec.Close()

	task := CompensationTask{
		WorkflowID: "wf-comp",
		RunID:      "run-1",
		StepName:   "step-1",
		Activity:   "undo-something",
		Timeout:    1 * time.Second,
	}

	exec.ExecuteCompensation(task)

	deadline := time.Now().Add(1 * time.Second)
	for {
		applier.mu.Lock()
		count := len(applier.applied)
		applier.mu.Unlock()
		if count > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	applier.mu.Lock()
	defer applier.mu.Unlock()

	if len(applier.applied) != 1 {
		t.Fatalf("expected 1 report, got %d", len(applier.applied))
	}
	if applier.applied[0] != flowstore.CmdCompleteCompensationStep {
		t.Fatalf("expected CmdCompleteCompensationStep, got %v", applier.applied[0])
	}
	if !compensated.Load() {
		t.Fatalf("expected compensated to be true")
	}
}

func TestActivityStoreDispatch(t *testing.T) {
	st := NewActivityStore()
	var dispatchedTask ActivityTask
	var wg sync.WaitGroup
	wg.Add(1)

	st.SetListeners(func(task ActivityTask) {
		dispatchedTask = task
		wg.Done()
	}, nil)

	cmd, err := store.NewCommand(CmdDispatchActivity, ActivityTask{
		WorkflowID: "wf-10",
		StepName:   "step-a",
		Activity:   "act-a",
	})
	if err != nil {
		t.Fatalf("NewCommand failed: %v", err)
	}

	res, err := st.Router().Execute(context.Background(), cmd)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if res != "DISPATCHED" {
		t.Fatalf("expected 'DISPATCHED', got %v", res)
	}

	wg.Wait()
	if dispatchedTask.WorkflowID != "wf-10" || dispatchedTask.StepName != "step-a" {
		t.Fatalf("unexpected dispatched task: %+v", dispatchedTask)
	}
}
