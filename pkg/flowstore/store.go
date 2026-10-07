package flowstore

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus/pkg/store"
)

// Ensure Store implements store.Store interface.
var _ store.Store = (*Store)(nil)

// Store is a Raft-replicated state machine store for workflow lifecycle management.
type Store struct {
	store.BaseStore
	mu        sync.RWMutex
	workflows map[string]*flow.WorkflowInstance
	mutator   store.Mutator
}

// New creates an initialized Flow Store.
func New() *Store {
	s := &Store{
		BaseStore: store.NewBaseStore(),
		workflows: make(map[string]*flow.WorkflowInstance),
	}
	s.registerHandlers()
	return s
}

// ID implements store.Store.
func (s *Store) ID() store.StoreID {
	return "flowstore"
}

// AttachMutator attaches a mutator handle for cluster proposals.
func (s *Store) AttachMutator(m store.Mutator) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mutator = m
}

// Mutator returns the attached mutator.
func (s *Store) Mutator() store.Mutator {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mutator
}

func (s *Store) registerHandlers() {
	r := s.Router()

	// 1. Start Workflow
	store.HandleTyped(r, CmdStartWorkflow, func(ctx context.Context, req StartWorkflowRequest) (*flow.WorkflowInstance, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		if _, exists := s.workflows[req.WorkflowID]; exists {
			return nil, flow.ErrWorkflowAlreadyExist
		}

		runID := req.RunID
		if runID == "" {
			runID = fmt.Sprintf("run-%d", time.Now().UTC().UnixNano())
		}

		inst, err := flow.NewWorkflowInstance(req.WorkflowID, runID, req.Definition, req.Input, req.Metadata)
		if err != nil {
			return nil, err
		}

		s.workflows[req.WorkflowID] = inst
		return inst.Clone(), nil
	})

	// 2. Dispatch Step
	store.HandleTyped(r, CmdDispatchStep, func(ctx context.Context, req DispatchStepRequest) (*flow.StepExecution, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		wf, exists := s.workflows[req.WorkflowID]
		if !exists {
			return nil, flow.ErrWorkflowNotFound
		}

		step, exists := wf.Steps[req.StepName]
		if !exists {
			return nil, flow.ErrStepNotFound
		}

		now := req.At.UTC()
		if req.At.IsZero() {
			now = time.Now().UTC()
		}
		step.Status = flow.StepStatusRunning
		step.Attempt++
		step.StartedAt = &now
		wf.UpdatedAt = now
		wf.RecordEvent("STEP_DISPATCHED", req.StepName, fmt.Sprintf("Step %s dispatched (attempt %d)", req.StepName, step.Attempt), nil)

		switch step.Kind {
		case flow.KindSleep:
			wake := now.Add(step.Delay)
			step.WakeAt = &wake
		case flow.KindWaitSignal:
			if step.Timeout > 0 {
				wake := now.Add(step.Timeout)
				step.WakeAt = &wake
			}
			if queue := wf.Signals[step.Signal]; len(queue) > 0 {
				wf.Signals[step.Signal] = queue[1:]
				completeStepLocked(wf, step, queue[0], now)
			}
		}

		return step, nil
	})

	// 3. Complete Step
	store.HandleTyped(r, CmdCompleteStep, func(ctx context.Context, req CompleteStepRequest) (*flow.WorkflowInstance, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		wf, exists := s.workflows[req.WorkflowID]
		if !exists {
			return nil, flow.ErrWorkflowNotFound
		}

		step, exists := wf.Steps[req.StepName]
		if !exists {
			return nil, flow.ErrStepNotFound
		}

		if step.Status == flow.StepStatusCompleted {
			// Idempotent: already completed
			return wf.Clone(), nil
		}
		if wf.IsTerminal() || step.Status == flow.StepStatusSkipped {
			// A worker reporting after the workflow ended or the step was
			// cancelled must not resurrect it.
			return wf.Clone(), nil
		}

		completeStepLocked(wf, step, req.Output, time.Now().UTC())
		return wf.Clone(), nil
	})

	// 4. Fail Step
	store.HandleTyped(r, CmdFailStep, func(ctx context.Context, req FailStepRequest) (*flow.WorkflowInstance, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		wf, exists := s.workflows[req.WorkflowID]
		if !exists {
			return nil, flow.ErrWorkflowNotFound
		}

		step, exists := wf.Steps[req.StepName]
		if !exists {
			return nil, flow.ErrStepNotFound
		}

		if wf.IsTerminal() || step.Status == flow.StepStatusSkipped || step.Status == flow.StepStatusCompleted {
			// Late or duplicate report for a step that no longer runs.
			return wf.Clone(), nil
		}

		failStepLocked(wf, step, req.Error, req.Code, req.Retryable, time.Now().UTC())
		return wf.Clone(), nil
	})

	// 5. Trigger Compensation
	store.HandleTyped(r, CmdTriggerCompensation, func(ctx context.Context, req TriggerCompensationRequest) (*flow.WorkflowInstance, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		wf, exists := s.workflows[req.WorkflowID]
		if !exists {
			return nil, flow.ErrWorkflowNotFound
		}

		now := time.Now().UTC()
		wf.Status = flow.StatusCompensating
		wf.Error = req.Reason
		wf.UpdatedAt = now
		wf.RecordEvent("COMPENSATION_TRIGGERED", "", fmt.Sprintf("Compensation manually triggered: %s", req.Reason), nil)

		return wf.Clone(), nil
	})

	// 6. Start Compensation Step
	store.HandleTyped(r, CmdStartCompensationStep, func(ctx context.Context, req StartCompensationStepRequest) (*flow.StepExecution, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		wf, exists := s.workflows[req.WorkflowID]
		if !exists {
			return nil, flow.ErrWorkflowNotFound
		}

		step, exists := wf.Steps[req.StepName]
		if !exists {
			return nil, flow.ErrStepNotFound
		}

		now := time.Now().UTC()
		step.Status = flow.StepStatusCompensating
		wf.UpdatedAt = now
		wf.RecordEvent("COMPENSATION_STEP_STARTED", req.StepName, fmt.Sprintf("Compensating action %s started", step.CompensatingAction), nil)

		return step, nil
	})

	// 7. Complete Compensation Step
	store.HandleTyped(r, CmdCompleteCompensationStep, func(ctx context.Context, req CompleteCompensationStepRequest) (*flow.WorkflowInstance, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		wf, exists := s.workflows[req.WorkflowID]
		if !exists {
			return nil, flow.ErrWorkflowNotFound
		}

		step, exists := wf.Steps[req.StepName]
		if !exists {
			return nil, flow.ErrStepNotFound
		}

		if wf.IsTerminal() || step.Status == flow.StepStatusCompensated {
			return wf.Clone(), nil
		}

		now := time.Now().UTC()
		step.Status = flow.StepStatusCompensated
		step.CompensatedAt = &now
		wf.UpdatedAt = now
		wf.RecordEvent("COMPENSATION_STEP_COMPLETED", req.StepName, fmt.Sprintf("Compensating action %s completed", step.CompensatingAction), req.Output)

		// Check if all compensations are now finished
		if wf.NextCompensatingStep() == nil {
			wf.Status = flow.StatusCompensated
			wf.CompletedAt = &now
			wf.RecordEvent("WORKFLOW_COMPENSATED", "", "Workflow fully compensated and rolled back", nil)
		}

		return wf.Clone(), nil
	})

	// 8. Fail Compensation Step
	store.HandleTyped(r, CmdFailCompensationStep, func(ctx context.Context, req FailCompensationStepRequest) (*flow.WorkflowInstance, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		wf, exists := s.workflows[req.WorkflowID]
		if !exists {
			return nil, flow.ErrWorkflowNotFound
		}

		step, exists := wf.Steps[req.StepName]
		if !exists {
			return nil, flow.ErrStepNotFound
		}
		if wf.IsTerminal() {
			return wf.Clone(), nil
		}

		now := time.Now().UTC()
		step.Status = flow.StepStatusCompensationFailed
		step.Error = req.Error
		step.ErrorCode = req.Code
		wf.Status = flow.StatusCompensationFailed
		wf.CompletedAt = &now
		wf.Error = fmt.Sprintf("compensation failed on step %s: %s", req.StepName, req.Error)
		wf.ErrorCode = req.Code
		wf.UpdatedAt = now
		wf.RecordEvent("COMPENSATION_STEP_FAILED", req.StepName, fmt.Sprintf("Compensating action failed: %s", req.Error), nil)
		wf.RecordEvent("WORKFLOW_COMPENSATION_FAILED", req.StepName, "Workflow could not be fully rolled back", nil)

		return wf.Clone(), nil
	})

	// 9. Complete Workflow
	store.HandleTyped(r, CmdCompleteWorkflow, func(ctx context.Context, req CompleteWorkflowRequest) (*flow.WorkflowInstance, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		wf, exists := s.workflows[req.WorkflowID]
		if !exists {
			return nil, flow.ErrWorkflowNotFound
		}

		now := time.Now().UTC()
		wf.Status = flow.StatusCompleted
		wf.CompletedAt = &now
		wf.Output = req.Output
		wf.UpdatedAt = now
		wf.RecordEvent("WORKFLOW_COMPLETED", "", "Workflow explicitly marked completed", req.Output)

		return wf.Clone(), nil
	})

	// 10. Cancel Workflow
	store.HandleTyped(r, CmdCancelWorkflow, func(ctx context.Context, req CancelWorkflowRequest) (*flow.WorkflowInstance, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		wf, exists := s.workflows[req.WorkflowID]
		if !exists {
			return nil, flow.ErrWorkflowNotFound
		}

		if wf.IsTerminal() {
			return wf.Clone(), nil
		}

		now := time.Now().UTC()
		wf.Error = fmt.Sprintf("cancelled: %s", req.Reason)
		wf.UpdatedAt = now

		// Mark any pending or running steps as skipped
		for _, step := range wf.Steps {
			if step.Status == flow.StepStatusPending || step.Status == flow.StepStatusRunning {
				step.Status = flow.StepStatusSkipped
			}
		}

		// If any completed steps need compensation, rollback
		if wf.NextCompensatingStep() != nil {
			wf.Status = flow.StatusCompensating
			wf.RecordEvent("WORKFLOW_CANCELLED", "", fmt.Sprintf("Workflow cancelled (%s), initiating compensation", req.Reason), nil)
		} else {
			wf.Status = flow.StatusCancelled
			wf.CompletedAt = &now
			wf.RecordEvent("WORKFLOW_CANCELLED", "", fmt.Sprintf("Workflow cancelled: %s", req.Reason), nil)
		}

		return wf.Clone(), nil
	})

	// 11. Signal Workflow
	store.HandleTyped(r, CmdSignalWorkflow, func(ctx context.Context, req SignalWorkflowRequest) (*flow.WorkflowInstance, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		wf, exists := s.workflows[req.WorkflowID]
		if !exists {
			return nil, flow.ErrWorkflowNotFound
		}

		if !wf.DefinesSignal(req.SignalName) {
			return nil, fmt.Errorf("%w: %q", flow.ErrUnknownSignal, req.SignalName)
		}
		if wf.IsTerminal() {
			return wf.Clone(), nil
		}

		now := time.Now().UTC()
		wf.UpdatedAt = now
		wf.RecordEvent("WORKFLOW_SIGNALED", "", fmt.Sprintf("Signal received: %s", req.SignalName), req.Payload)

		if wf.Status == flow.StatusRunning {
			for _, name := range wf.StepOrder {
				step := wf.Steps[name]
				if step.Kind == flow.KindWaitSignal && step.Signal == req.SignalName && step.Status == flow.StepStatusRunning {
					completeStepLocked(wf, step, req.Payload, now)
					return wf.Clone(), nil
				}
			}
		}
		// Nobody is waiting yet: keep it for the step that will.
		if wf.Signals == nil {
			wf.Signals = make(map[string][]json.RawMessage)
		}
		wf.Signals[req.SignalName] = append(wf.Signals[req.SignalName], req.Payload)

		return wf.Clone(), nil
	})

	// 12. Fire Timer
	store.HandleTyped(r, CmdFireTimer, func(ctx context.Context, req FireTimerRequest) (*flow.WorkflowInstance, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		wf, exists := s.workflows[req.WorkflowID]
		if !exists {
			return nil, flow.ErrWorkflowNotFound
		}
		step, exists := wf.Steps[req.StepName]
		if !exists {
			return nil, flow.ErrStepNotFound
		}
		if wf.Status != flow.StatusRunning || step.Status != flow.StepStatusRunning ||
			step.WakeAt == nil || step.WakeAt.After(req.At) {
			return wf.Clone(), nil // already resolved, or not due
		}

		switch step.Kind {
		case flow.KindSleep:
			completeStepLocked(wf, step, nil, req.At)
		case flow.KindWaitSignal:
			failStepLocked(wf, step, fmt.Sprintf("timed out waiting for signal %q", step.Signal), flow.CodeSignalTimeout, false, req.At)
		}
		return wf.Clone(), nil
	})
}

// completeStepLocked marks a step completed and the workflow completed when it was the last one.
func completeStepLocked(wf *flow.WorkflowInstance, step *flow.StepExecution, output json.RawMessage, now time.Time) {
	step.Status = flow.StepStatusCompleted
	step.Output = output
	step.WakeAt = nil
	step.CompletedAt = &now
	wf.UpdatedAt = now
	wf.RecordEvent("STEP_COMPLETED", step.Name, fmt.Sprintf("Step %s completed", step.Name), output)

	if wf.AllStepsCompleted() {
		wf.Status = flow.StatusCompleted
		wf.CompletedAt = &now
		wf.Output = output
		wf.RecordEvent("WORKFLOW_COMPLETED", "", "Workflow completed successfully", output)
	}
}

// failStepLocked records a failed attempt: it schedules a retry, or fails the
// step for good and starts compensation (or fails the workflow).
func failStepLocked(wf *flow.WorkflowInstance, step *flow.StepExecution, message, code string, retryable bool, now time.Time) {
	step.Error = message
	step.ErrorCode = code
	step.WakeAt = nil
	wf.UpdatedAt = now

	// Retries is the number of attempts after the first.
	if retryable && step.Attempt <= step.MaxRetries {
		step.Status = flow.StepStatusPending
		wf.RecordEvent("STEP_RETRY_SCHEDULED", step.Name, fmt.Sprintf("Step %s failed (attempt %d/%d), scheduling retry: %s", step.Name, step.Attempt, step.MaxRetries, message), nil)
		return
	}

	step.Status = flow.StepStatusFailed
	wf.Error = fmt.Sprintf("step %s failed: %s", step.Name, message)
	wf.ErrorCode = code
	wf.RecordEvent("STEP_FAILED", step.Name, fmt.Sprintf("Step %s failed permanently: %s", step.Name, message), nil)

	// Engine-resolved steps still waiting have nobody to cancel them, so stop them here.
	for _, other := range wf.Steps {
		if !other.IsActivity() && other.Status == flow.StepStatusRunning {
			other.Status = flow.StepStatusSkipped
			other.WakeAt = nil
		}
	}

	if wf.NextCompensatingStep() != nil {
		wf.Status = flow.StatusCompensating
		wf.RecordEvent("COMPENSATION_TRIGGERED", step.Name, "Saga compensation triggered due to step failure", nil)
	} else {
		wf.Status = flow.StatusFailed
		wf.CompletedAt = &now
		wf.RecordEvent("WORKFLOW_FAILED", step.Name, "Workflow failed with no compensating actions", nil)
	}
}

// Wait describes a signal-wait step that has not finished yet.
type Wait struct {
	Step   string
	Signal string
	Status flow.StepStatus
	// Buffered is how many signals of that name already arrived and are unconsumed.
	Buffered int
}

// Waits lists the unfinished signal-wait steps of a running workflow, without
// copying it. It lets an application decide whether a signal is still due.
func (s *Store) Waits(workflowID string) []Wait {
	s.mu.RLock()
	defer s.mu.RUnlock()
	wf, ok := s.workflows[workflowID]
	if !ok || wf.Status != flow.StatusRunning {
		return nil
	}
	var out []Wait
	for _, name := range wf.StepOrder {
		step := wf.Steps[name]
		if step.Kind != flow.KindWaitSignal {
			continue
		}
		if step.Status == flow.StepStatusPending || step.Status == flow.StepStatusRunning {
			out = append(out, Wait{Step: name, Signal: step.Signal, Status: step.Status, Buffered: len(wf.Signals[step.Signal])})
		}
	}
	return out
}

// TimerRef identifies a step whose timer has expired.
type TimerRef struct {
	WorkflowID string
	RunID      string
	StepName   string
}

// DueTimers lists running sleep and signal-wait steps whose wake time is not after now.
func (s *Store) DueTimers(now time.Time) []TimerRef {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var due []TimerRef
	for _, wf := range s.workflows {
		if wf.Status != flow.StatusRunning {
			continue
		}
		for _, step := range wf.Steps {
			if step.Status == flow.StepStatusRunning && step.WakeAt != nil && !step.WakeAt.After(now) {
				due = append(due, TimerRef{WorkflowID: wf.WorkflowID, RunID: wf.RunID, StepName: step.Name})
			}
		}
	}
	return due
}

// GetWorkflow retrieves a workflow instance by ID.
func (s *Store) GetWorkflow(workflowID string) (*flow.WorkflowInstance, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	wf, exists := s.workflows[workflowID]
	if !exists {
		return nil, false
	}
	return wf.Clone(), true
}

// StepState returns the workflow and step status without copying the workflow.
func (s *Store) StepState(workflowID, stepName string) (flow.Status, flow.StepStatus, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	wf, ok := s.workflows[workflowID]
	if !ok {
		return "", "", false
	}
	step, ok := wf.Steps[stepName]
	if !ok {
		return wf.Status, "", false
	}
	return wf.Status, step.Status, true
}

// ListWorkflows retrieves all workflow instances.
func (s *Store) ListWorkflows() []*flow.WorkflowInstance {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := make([]*flow.WorkflowInstance, 0, len(s.workflows))
	for _, wf := range s.workflows {
		list = append(list, wf.Clone())
	}
	return list
}

// GetHistory retrieves the audit event log of a workflow.
func (s *Store) GetHistory(workflowID string) ([]flow.Event, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	wf, exists := s.workflows[workflowID]
	if !exists {
		return nil, false
	}
	history := make([]flow.Event, len(wf.History))
	copy(history, wf.History)
	return history, true
}

// Snapshot serializes the complete in-memory state of the store.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.workflows)
}

// Restore resets store state from snapshot data.
func (s *Store) Restore(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var restored map[string]*flow.WorkflowInstance
	if err := json.Unmarshal(data, &restored); err != nil {
		return fmt.Errorf("restore flow store: %w", err)
	}

	if restored == nil {
		restored = make(map[string]*flow.WorkflowInstance)
	}
	s.workflows = restored
	return nil
}
