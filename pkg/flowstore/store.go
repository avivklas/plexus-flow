package flowstore

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
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
	mu          sync.RWMutex
	workflows   map[string]*flow.WorkflowInstance
	schedules   map[string]*flow.Schedule
	timers      *timerHeap
	archiver    Archiver
	autoAdvance bool
	mutator     store.Mutator
	watch       watchers
}

// New creates an initialized Flow Store.
func New() *Store {
	s := &Store{
		BaseStore: store.NewBaseStore(),
		workflows: make(map[string]*flow.WorkflowInstance),
		schedules: make(map[string]*flow.Schedule),
		timers:    newTimerHeap(),
	}
	s.registerHandlers()
	return s
}

// ID implements store.Store.
func (s *Store) ID() store.StoreID {
	return "flowstore"
}

// SetAutoAdvance configures whether workflow mutations atomically advance ready steps
// and compensating steps inside the same Raft commit (eliminating secondary CmdDispatchStep commits).
func (s *Store) SetAutoAdvance(enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.autoAdvance = enabled
}

// AutoAdvance reports whether atomic step advancement is enabled.
func (s *Store) AutoAdvance() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.autoAdvance
}

// SetArchiver attaches a cold storage archiver for compacted terminal workflows.
func (s *Store) SetArchiver(a Archiver) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.archiver = a
}

// Archiver returns the attached cold storage archiver, if any.
func (s *Store) Archiver() Archiver {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.archiver
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
	handle(s, r, CmdStartWorkflow, func(ctx context.Context, req StartWorkflowRequest) (*flow.WorkflowInstance, error) {
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
		if s.autoAdvance {
			s.advanceWorkflowLocked(inst, inst.CreatedAt)
		}
		return inst.Clone(), nil
	})

	// 2. Dispatch Step
	handle(s, r, CmdDispatchStep, func(ctx context.Context, req DispatchStepRequest) (*flow.StepExecution, error) {
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
		if err := s.dispatchStepLocked(wf, step, now); err != nil {
			return nil, err
		}
		return step, nil
	})

	// 3. Complete Step
	handle(s, r, CmdCompleteStep, func(ctx context.Context, req CompleteStepRequest) (*flow.WorkflowInstance, error) {
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

		now := time.Now().UTC()
		s.completeStepLocked(wf, step, req.Output, now)
		if s.autoAdvance {
			s.advanceWorkflowLocked(wf, now)
		}
		return wf.Clone(), nil
	})

	// 4. Fail Step
	handle(s, r, CmdFailStep, func(ctx context.Context, req FailStepRequest) (*flow.WorkflowInstance, error) {
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

		now := time.Now().UTC()
		s.failStepLocked(wf, step, req.Error, req.Code, req.Retryable, now)
		if s.autoAdvance {
			s.advanceWorkflowLocked(wf, now)
		}
		return wf.Clone(), nil
	})

	// 5. Trigger Compensation
	handle(s, r, CmdTriggerCompensation, func(ctx context.Context, req TriggerCompensationRequest) (*flow.WorkflowInstance, error) {
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
		if s.autoAdvance {
			s.advanceWorkflowLocked(wf, now)
		}

		return wf.Clone(), nil
	})

	// 6. Start Compensation Step
	handle(s, r, CmdStartCompensationStep, func(ctx context.Context, req StartCompensationStepRequest) (*flow.StepExecution, error) {
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

		s.startCompensationStepLocked(wf, step, time.Now().UTC())
		return step, nil
	})

	// 7. Complete Compensation Step
	handle(s, r, CmdCompleteCompensationStep, func(ctx context.Context, req CompleteCompensationStepRequest) (*flow.WorkflowInstance, error) {
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
			s.timers.removeWorkflow(wf.WorkflowID)
			wf.RecordEvent("WORKFLOW_COMPENSATED", "", "Workflow fully compensated and rolled back", nil)
		} else if s.autoAdvance {
			s.advanceWorkflowLocked(wf, now)
		}

		return wf.Clone(), nil
	})

	// 8. Fail Compensation Step
	handle(s, r, CmdFailCompensationStep, func(ctx context.Context, req FailCompensationStepRequest) (*flow.WorkflowInstance, error) {
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
		retryable := req.Retryable && !step.RetryPolicy.IsNonRetryableCode(req.Code)
		if retryable && step.CompensationAttempt <= step.MaxRetries {
			// Reset back to COMPLETED so NextCompensatingStep picks it up for retry
			step.Status = flow.StepStatusCompleted
			step.Error = req.Error
			step.ErrorCode = req.Code
			wf.UpdatedAt = now
			wf.RecordEvent("COMPENSATION_RETRY_SCHEDULED", req.StepName, fmt.Sprintf("Compensating action %s failed (attempt %d/%d), scheduling retry: %s", step.CompensatingAction, step.CompensationAttempt, step.MaxRetries, req.Error), nil)
			if s.autoAdvance {
				s.advanceWorkflowLocked(wf, now)
			}
			return wf.Clone(), nil
		}

		step.Status = flow.StepStatusCompensationFailed
		step.Error = req.Error
		step.ErrorCode = req.Code
		wf.Status = flow.StatusCompensationFailed
		wf.CompletedAt = &now
		wf.Error = fmt.Sprintf("compensation failed on step %s: %s", req.StepName, req.Error)
		wf.ErrorCode = req.Code
		wf.UpdatedAt = now
		s.timers.removeWorkflow(wf.WorkflowID)
		wf.RecordEvent("COMPENSATION_STEP_FAILED", req.StepName, fmt.Sprintf("Compensating action failed: %s", req.Error), nil)
		wf.RecordEvent("WORKFLOW_COMPENSATION_FAILED", req.StepName, "Workflow could not be fully rolled back", nil)

		return wf.Clone(), nil
	})

	// 9. Complete Workflow
	handle(s, r, CmdCompleteWorkflow, func(ctx context.Context, req CompleteWorkflowRequest) (*flow.WorkflowInstance, error) {
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
		s.timers.removeWorkflow(wf.WorkflowID)
		wf.RecordEvent("WORKFLOW_COMPLETED", "", "Workflow explicitly marked completed", req.Output)

		return wf.Clone(), nil
	})

	// 10. Cancel Workflow
	handle(s, r, CmdCancelWorkflow, func(ctx context.Context, req CancelWorkflowRequest) (*flow.WorkflowInstance, error) {
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

		// Mark any pending or running steps as skipped and clear all timers
		for _, step := range wf.Steps {
			if step.Status == flow.StepStatusPending || step.Status == flow.StepStatusRunning {
				step.Status = flow.StepStatusSkipped
				step.WakeAt = nil
			}
		}
		s.timers.removeWorkflow(wf.WorkflowID)

		// If any completed steps need compensation, rollback
		if wf.NextCompensatingStep() != nil {
			wf.Status = flow.StatusCompensating
			wf.RecordEvent("WORKFLOW_CANCELLED", "", fmt.Sprintf("Workflow cancelled (%s), initiating compensation", req.Reason), nil)
			if s.autoAdvance {
				s.advanceWorkflowLocked(wf, now)
			}
		} else {
			wf.Status = flow.StatusCancelled
			wf.CompletedAt = &now
			wf.RecordEvent("WORKFLOW_CANCELLED", "", fmt.Sprintf("Workflow cancelled: %s", req.Reason), nil)
		}

		return wf.Clone(), nil
	})

	// 11. Signal Workflow
	handle(s, r, CmdSignalWorkflow, func(ctx context.Context, req SignalWorkflowRequest) (*flow.WorkflowInstance, error) {
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
					s.completeStepLocked(wf, step, req.Payload, now)
					if s.autoAdvance {
						s.advanceWorkflowLocked(wf, now)
					}
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

	// 12. Fire Single Timer
	handle(s, r, CmdFireTimer, func(ctx context.Context, req FireTimerRequest) (*flow.WorkflowInstance, error) {
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
		s.fireStepTimerLocked(wf, step, req.At)
		return wf.Clone(), nil
	})

	// 13. Fire Batched Timers
	store.HandleTyped(r, CmdFireTimers, func(ctx context.Context, req FireTimersRequest) ([]string, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		changedSet := make(map[string]bool, len(req.Timers))
		var changedIDs []string
		for _, ref := range req.Timers {
			wf, ok := s.workflows[ref.WorkflowID]
			if !ok {
				s.timers.remove(ref.WorkflowID, ref.StepName)
				continue
			}
			step, ok := wf.Steps[ref.StepName]
			if !ok {
				s.timers.remove(ref.WorkflowID, ref.StepName)
				continue
			}
			beforeEvents := len(wf.History)
			beforeStatus := wf.Status
			s.fireStepTimerLocked(wf, step, req.At)
			if len(wf.History) != beforeEvents || wf.Status != beforeStatus {
				if !changedSet[wf.WorkflowID] {
					changedSet[wf.WorkflowID] = true
					changedIDs = append(changedIDs, wf.WorkflowID)
				}
			}
		}
		for _, id := range changedIDs {
			if wf, ok := s.workflows[id]; ok {
				wf.Version++
				s.publishLocked(wf)
			}
		}
		return changedIDs, nil
	})

	// 14. Update Step Progress (Heartbeat Checkpointing)
	store.HandleTyped(r, CmdUpdateStepProgress, func(ctx context.Context, req UpdateStepProgressRequest) (bool, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		wf, exists := s.workflows[req.WorkflowID]
		if !exists || wf.IsTerminal() {
			return false, nil
		}
		step, exists := wf.Steps[req.StepName]
		if !exists {
			return false, nil
		}
		step.Progress = append(json.RawMessage(nil), req.Progress...)
		wf.UpdatedAt = time.Now().UTC()
		return true, nil
	})

	// 15. Compact Terminal Workflows (Cold Storage Archival / Eviction)
	store.HandleTyped(r, CmdCompactWorkflows, func(ctx context.Context, req CompactWorkflowsRequest) (CompactWorkflowsResponse, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		var terminals []*flow.WorkflowInstance
		for _, wf := range s.workflows {
			if wf.IsTerminal() {
				terminals = append(terminals, wf)
			}
		}
		// Sort oldest completed first
		sort.Slice(terminals, func(i, j int) bool {
			ti := terminals[i].UpdatedAt
			if terminals[i].CompletedAt != nil {
				ti = *terminals[i].CompletedAt
			}
			tj := terminals[j].UpdatedAt
			if terminals[j].CompletedAt != nil {
				tj = *terminals[j].CompletedAt
			}
			if !ti.Equal(tj) {
				return ti.Before(tj)
			}
			return terminals[i].WorkflowID < terminals[j].WorkflowID
		})

		evictSet := make(map[string]*flow.WorkflowInstance)
		if !req.Before.IsZero() {
			for _, wf := range terminals {
				ended := wf.UpdatedAt
				if wf.CompletedAt != nil {
					ended = *wf.CompletedAt
				}
				if !ended.After(req.Before) {
					evictSet[wf.WorkflowID] = wf
				}
			}
		}
		if req.MaxRetained > 0 {
			remaining := len(terminals) - len(evictSet)
			for _, wf := range terminals {
				if remaining <= req.MaxRetained {
					break
				}
				if _, already := evictSet[wf.WorkflowID]; !already {
					evictSet[wf.WorkflowID] = wf
					remaining--
				}
			}
		}

		if len(evictSet) == 0 {
			return CompactWorkflowsResponse{}, nil
		}

		toArchive := make([]*flow.WorkflowInstance, 0, len(evictSet))
		ids := make([]string, 0, len(evictSet))
		for id, wf := range evictSet {
			ids = append(ids, id)
			toArchive = append(toArchive, wf)
		}
		sort.Strings(ids)

		if s.archiver != nil {
			if err := s.archiver.ArchiveWorkflows(toArchive); err != nil {
				return CompactWorkflowsResponse{}, fmt.Errorf("archive workflows: %w", err)
			}
		}
		for _, id := range ids {
			delete(s.workflows, id)
			s.timers.removeWorkflow(id)
		}

		return CompactWorkflowsResponse{
			Compacted:   len(ids),
			WorkflowIDs: ids,
		}, nil
	})

	// 16. Upsert Schedule
	store.HandleTyped(r, CmdUpsertSchedule, func(ctx context.Context, req UpsertScheduleRequest) (*flow.Schedule, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		sched := req.Schedule
		if err := sched.Validate(); err != nil {
			return nil, err
		}
		now := req.Now.UTC()
		if req.Now.IsZero() {
			now = time.Now().UTC()
		}
		next, err := flow.NextScheduleTime(sched.CronExpr, sched.Interval, now)
		if err != nil {
			return nil, err
		}
		if existing, ok := s.schedules[sched.ScheduleID]; ok {
			sched.CreatedAt = existing.CreatedAt
			sched.TotalRuns = existing.TotalRuns
			sched.LastRunAt = existing.LastRunAt
			sched.LastWorkflowID = existing.LastWorkflowID
		} else {
			sched.CreatedAt = now
		}
		sched.UpdatedAt = now
		if sched.NextFireAt.IsZero() || sched.NextFireAt.Before(now) {
			sched.NextFireAt = next
		}
		cp := sched.Clone()
		s.schedules[sched.ScheduleID] = cp
		return cp.Clone(), nil
	})

	// 17. Delete Schedule
	store.HandleTyped(r, CmdDeleteSchedule, func(ctx context.Context, req DeleteScheduleRequest) (bool, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		if _, exists := s.schedules[req.ScheduleID]; !exists {
			return false, flow.ErrScheduleNotFound
		}
		delete(s.schedules, req.ScheduleID)
		return true, nil
	})

	// 18. Trigger Schedule
	store.HandleTyped(r, CmdTriggerSchedule, func(ctx context.Context, req TriggerScheduleRequest) (*flow.WorkflowInstance, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		sched, exists := s.schedules[req.ScheduleID]
		if !exists {
			return nil, flow.ErrScheduleNotFound
		}
		now := req.At.UTC()
		if req.At.IsZero() {
			now = time.Now().UTC()
		}
		if sched.Paused || sched.NextFireAt.After(now) {
			return nil, nil
		}

		wfID := fmt.Sprintf("%s-%d", sched.ScheduleID, sched.NextFireAt.UnixNano())
		nextFire, err := flow.NextScheduleTime(sched.CronExpr, sched.Interval, now)
		if err != nil {
			return nil, err
		}
		sched.NextFireAt = nextFire
		sched.LastRunAt = &now
		sched.LastWorkflowID = wfID
		sched.TotalRuns++
		sched.UpdatedAt = now

		if existing, dup := s.workflows[wfID]; dup {
			return existing.Clone(), nil
		}

		runID := fmt.Sprintf("run-%d", now.UnixNano())
		inst, err := flow.NewWorkflowInstance(wfID, runID, sched.Definition, sched.Input, sched.Metadata)
		if err != nil {
			return nil, err
		}
		s.workflows[wfID] = inst
		if s.autoAdvance {
			s.advanceWorkflowLocked(inst, now)
		}
		inst.Version++
		s.publishLocked(inst)
		return inst.Clone(), nil
	})
}

// dispatchStepLocked transitions a pending step to RUNNING and sets up sleep or signal-wait timers.
func (s *Store) dispatchStepLocked(wf *flow.WorkflowInstance, step *flow.StepExecution, now time.Time) error {
	// Dispatch is idempotent. Concurrent commits can each see the same step as
	// ready; only the first may start it. A second dispatch would re-run an
	// activity, or reopen a signal wait that already consumed its signal.
	// Retries are unaffected: a retryable failure puts the step back to PENDING.
	if wf.Status != flow.StatusRunning || step.Status != flow.StepStatusPending {
		return fmt.Errorf("%w: step %s is %s", ErrStepNotPending, step.Name, step.Status)
	}
	s.timers.remove(wf.WorkflowID, step.Name)
	step.WakeAt = nil
	step.Status = flow.StepStatusRunning
	step.Attempt++
	step.StartedAt = &now
	step.DispatchedVersion = wf.Version + 1
	wf.UpdatedAt = now
	wf.RecordEvent("STEP_DISPATCHED", step.Name, fmt.Sprintf("Step %s dispatched (attempt %d)", step.Name, step.Attempt), nil)

	switch step.Kind {
	case flow.KindSleep:
		wake := now.Add(step.Delay)
		step.WakeAt = &wake
		s.timers.set(wf.WorkflowID, wf.RunID, step.Name, &wake)
	case flow.KindWaitSignal:
		if len(wf.Signals[step.Signal]) > 0 {
			payload := wf.Signals[step.Signal][0]
			wf.Signals[step.Signal] = wf.Signals[step.Signal][1:]
			s.completeStepLocked(wf, step, payload, now)
			return nil
		}
		if step.Timeout > 0 {
			wake := now.Add(step.Timeout)
			step.WakeAt = &wake
			s.timers.set(wf.WorkflowID, wf.RunID, step.Name, &wake)
		}
	}
	return nil
}

func (s *Store) startCompensationStepLocked(wf *flow.WorkflowInstance, step *flow.StepExecution, now time.Time) {
	step.Status = flow.StepStatusCompensating
	step.CompensationAttempt++
	step.DispatchedVersion = wf.Version + 1
	wf.UpdatedAt = now
	wf.RecordEvent("COMPENSATION_STEP_STARTED", step.Name, fmt.Sprintf("Compensating action %s started", step.CompensatingAction), nil)
}

// advanceWorkflowLocked atomically dispatches ready steps (or starts the next compensating step)
// inside the current state transition when autoAdvance is enabled.
func (s *Store) advanceWorkflowLocked(wf *flow.WorkflowInstance, now time.Time) {
	switch wf.Status {
	case flow.StatusRunning:
		for wf.Status == flow.StatusRunning {
			ready := wf.NextReadySteps()
			if len(ready) == 0 {
				break
			}
			resolvedInline := false
			for _, step := range ready {
				_ = s.dispatchStepLocked(wf, step, now)
				if step.Status == flow.StepStatusCompleted {
					resolvedInline = true
				}
			}
			if !resolvedInline {
				break
			}
		}
	case flow.StatusCompensating:
		// If a step is already compensating, wait for it to finish before starting the next.
		for _, st := range wf.Steps {
			if st.Status == flow.StepStatusCompensating {
				return
			}
		}
		if nextComp := wf.NextCompensatingStep(); nextComp != nil {
			s.startCompensationStepLocked(wf, nextComp, now)
		}
	}
}

func (s *Store) fireStepTimerLocked(wf *flow.WorkflowInstance, step *flow.StepExecution, at time.Time) {
	if wf.Status != flow.StatusRunning || step.WakeAt == nil || step.WakeAt.After(at) {
		return // already resolved, or not due
	}
	s.timers.remove(wf.WorkflowID, step.Name)

	// Retry backoff timer on a PENDING step
	if step.Status == flow.StepStatusPending {
		step.WakeAt = nil
		wf.UpdatedAt = at
		wf.RecordEvent("STEP_RETRY_READY", step.Name, fmt.Sprintf("Step %s retry backoff elapsed", step.Name), nil)
		if s.autoAdvance {
			s.advanceWorkflowLocked(wf, at)
		}
		return
	}

	if step.Status != flow.StepStatusRunning {
		return
	}

	switch step.Kind {
	case flow.KindSleep:
		s.completeStepLocked(wf, step, nil, at)
		if s.autoAdvance {
			s.advanceWorkflowLocked(wf, at)
		}
	case flow.KindWaitSignal:
		s.failStepLocked(wf, step, fmt.Sprintf("timed out waiting for signal %q", step.Signal), flow.CodeSignalTimeout, false, at)
		if s.autoAdvance {
			s.advanceWorkflowLocked(wf, at)
		}
	}
}

// completeStepLocked marks a step completed and the workflow completed when it was the last one.
func (s *Store) completeStepLocked(wf *flow.WorkflowInstance, step *flow.StepExecution, output json.RawMessage, now time.Time) {
	s.timers.remove(wf.WorkflowID, step.Name)
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
		s.timers.removeWorkflow(wf.WorkflowID)
		wf.RecordEvent("WORKFLOW_COMPLETED", "", "Workflow completed successfully", output)
	}
}

// failStepLocked records a failed attempt: it schedules a retry (with optional exponential backoff),
// or fails the step for good and starts compensation (or fails the workflow).
func (s *Store) failStepLocked(wf *flow.WorkflowInstance, step *flow.StepExecution, message, code string, retryable bool, now time.Time) {
	s.timers.remove(wf.WorkflowID, step.Name)
	step.Error = message
	step.ErrorCode = code
	step.WakeAt = nil
	wf.UpdatedAt = now

	if step.RetryPolicy.IsNonRetryableCode(code) {
		retryable = false
	}

	// Retries is the number of attempts after the first.
	if retryable && step.Attempt <= step.MaxRetries {
		step.Status = flow.StepStatusPending
		if delay := step.RetryPolicy.NextDelay(step.Attempt); delay > 0 {
			wake := now.Add(delay)
			step.WakeAt = &wake
			s.timers.set(wf.WorkflowID, wf.RunID, step.Name, &wake)
		}
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
			s.timers.remove(wf.WorkflowID, other.Name)
		}
	}

	if wf.NextCompensatingStep() != nil {
		wf.Status = flow.StatusCompensating
		wf.RecordEvent("COMPENSATION_TRIGGERED", step.Name, "Saga compensation triggered due to step failure", nil)
	} else {
		wf.Status = flow.StatusFailed
		wf.CompletedAt = &now
		s.timers.removeWorkflow(wf.WorkflowID)
		wf.RecordEvent("WORKFLOW_FAILED", step.Name, "Workflow failed with no compensating actions", nil)
	}
}

// UpdateStepProgressLocal updates a running step's checkpointed progress in memory.
func (s *Store) UpdateStepProgressLocal(workflowID, stepName string, progress json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	wf, ok := s.workflows[workflowID]
	if !ok || wf.IsTerminal() {
		return
	}
	if step, ok := wf.Steps[stepName]; ok {
		step.Progress = append(json.RawMessage(nil), progress...)
	}
}

// StepProgress returns the last checkpointed progress for a step, if any.
func (s *Store) StepProgress(workflowID, stepName string) json.RawMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	wf, ok := s.workflows[workflowID]
	if !ok {
		return nil
	}
	step, ok := wf.Steps[stepName]
	if !ok || len(step.Progress) == 0 {
		return nil
	}
	return append(json.RawMessage(nil), step.Progress...)
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
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
	StepName   string `json:"step_name"`
}

// DueTimers lists running sleep/signal-wait steps and retry-backoff steps whose wake time is not after now.
// Uses the O(log N) min-heap instead of scanning all workflows.
func (s *Store) DueTimers(now time.Time) []TimerRef {
	s.mu.RLock()
	defer s.mu.RUnlock()
	candidates := s.timers.due(now)
	if len(candidates) == 0 {
		return nil
	}
	var due []TimerRef
	for _, c := range candidates {
		wf, ok := s.workflows[c.WorkflowID]
		if !ok || wf.Status != flow.StatusRunning {
			continue
		}
		step, ok := wf.Steps[c.StepName]
		if !ok || step.WakeAt == nil || step.WakeAt.After(now) {
			continue
		}
		if step.Status == flow.StepStatusRunning || step.Status == flow.StepStatusPending {
			due = append(due, c)
		}
	}
	return due
}

// DueSchedules lists active schedules whose NextFireAt is not after now.
func (s *Store) DueSchedules(now time.Time) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var due []string
	for id, sched := range s.schedules {
		if !sched.Paused && !sched.NextFireAt.IsZero() && !sched.NextFireAt.After(now) {
			due = append(due, id)
		}
	}
	sort.Strings(due)
	return due
}

// GetSchedule retrieves a schedule by ID.
func (s *Store) GetSchedule(scheduleID string) (*flow.Schedule, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sched, ok := s.schedules[scheduleID]
	if !ok {
		return nil, false
	}
	return sched.Clone(), true
}

// ListSchedules returns all registered schedules sorted by ID.
func (s *Store) ListSchedules() []*flow.Schedule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*flow.Schedule, 0, len(s.schedules))
	for _, sched := range s.schedules {
		out = append(out, sched.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ScheduleID < out[j].ScheduleID })
	return out
}

// GetWorkflow retrieves a workflow instance by ID (checking hot memory first, then cold Archiver if configured).
func (s *Store) GetWorkflow(workflowID string) (*flow.WorkflowInstance, bool) {
	s.mu.RLock()
	wf, exists := s.workflows[workflowID]
	arch := s.archiver
	s.mu.RUnlock()
	if exists {
		return wf.Clone(), true
	}
	if arch != nil {
		if archived, ok, err := arch.LoadWorkflow(workflowID); err == nil && ok {
			return archived, true
		}
	}
	return nil, false
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

// ListWorkflows retrieves all hot workflow instances.
func (s *Store) ListWorkflows() []*flow.WorkflowInstance {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := make([]*flow.WorkflowInstance, 0, len(s.workflows))
	for _, wf := range s.workflows {
		list = append(list, wf.Clone())
	}
	return list
}

// GetHistory retrieves the audit event log of a workflow (checking hot memory first, then cold Archiver).
func (s *Store) GetHistory(workflowID string) ([]flow.Event, bool) {
	s.mu.RLock()
	wf, exists := s.workflows[workflowID]
	arch := s.archiver
	s.mu.RUnlock()
	if exists {
		history := make([]flow.Event, len(wf.History))
		copy(history, wf.History)
		return history, true
	}
	if arch != nil {
		if archived, ok, err := arch.LoadWorkflow(workflowID); err == nil && ok {
			return archived.History, true
		}
	}
	return nil, false
}

type snapshotEnvelope struct {
	Workflows map[string]*flow.WorkflowInstance `json:"workflows"`
	Schedules map[string]*flow.Schedule         `json:"schedules,omitempty"`
}

// Snapshot serializes the complete in-memory state of the store.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.schedules) == 0 {
		return json.Marshal(s.workflows)
	}
	return json.Marshal(snapshotEnvelope{
		Workflows: s.workflows,
		Schedules: s.schedules,
	})
}

// Restore resets store state from snapshot data and rebuilds the timer min-heap.
func (s *Store) Restore(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var env snapshotEnvelope
	if err := json.Unmarshal(data, &env); err == nil && (env.Workflows != nil || env.Schedules != nil) {
		s.workflows = env.Workflows
		s.schedules = env.Schedules
	} else {
		var restored map[string]*flow.WorkflowInstance
		if err := json.Unmarshal(data, &restored); err != nil {
			return fmt.Errorf("restore flow store: %w", err)
		}
		s.workflows = restored
		s.schedules = make(map[string]*flow.Schedule)
	}

	if s.workflows == nil {
		s.workflows = make(map[string]*flow.WorkflowInstance)
	}
	if s.schedules == nil {
		s.schedules = make(map[string]*flow.Schedule)
	}

	// Rebuild timer min-heap from restored active workflows
	s.timers = newTimerHeap()
	for _, wf := range s.workflows {
		if wf.Status != flow.StatusRunning {
			continue
		}
		for _, step := range wf.Steps {
			if (step.Status == flow.StepStatusRunning || step.Status == flow.StepStatusPending) && step.WakeAt != nil {
				s.timers.set(wf.WorkflowID, wf.RunID, step.Name, step.WakeAt)
			}
		}
	}
	return nil
}
