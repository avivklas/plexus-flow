package flow

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// Status represents the lifecycle status of a workflow instance.
type Status string

const (
	StatusPending      Status = "PENDING"
	StatusRunning      Status = "RUNNING"
	StatusCompleted    Status = "COMPLETED"
	StatusFailed       Status = "FAILED"
	StatusCompensating Status = "COMPENSATING"
	StatusCompensated  Status = "COMPENSATED"
	StatusCancelled    Status = "CANCELLED"
	// StatusCompensationFailed is terminal: a compensating action failed, so the
	// effects of some completed steps could not be undone and need attention.
	StatusCompensationFailed Status = "COMPENSATION_FAILED"
)

// StepStatus represents the status of an individual step execution.
type StepStatus string

const (
	StepStatusPending      StepStatus = "PENDING"
	StepStatusRunning      StepStatus = "RUNNING"
	StepStatusCompleted    StepStatus = "COMPLETED"
	StepStatusFailed       StepStatus = "FAILED"
	StepStatusCompensating StepStatus = "COMPENSATING"
	StepStatusCompensated  StepStatus = "COMPENSATED"
	StepStatusSkipped      StepStatus = "SKIPPED"
	// StepStatusCompensationFailed marks a step whose compensating action failed.
	StepStatusCompensationFailed StepStatus = "COMPENSATION_FAILED"
)

// StepKind says what a step does when it becomes ready.
type StepKind string

const (
	// KindActivity runs an activity on a worker. It is the default.
	KindActivity StepKind = "activity"
	// KindWaitSignal parks the step until the workflow receives the named
	// signal; the signal payload becomes the step output. Timeout, if set,
	// fails the step with CodeSignalTimeout.
	KindWaitSignal StepKind = "wait_signal"
	// KindSleep completes the step after Delay.
	KindSleep StepKind = "sleep"
)

// Failure codes the engine itself produces.
const (
	CodeSignalTimeout = "SIGNAL_TIMEOUT"
)

// RetryPolicy configures exponential backoff and error-code filtering for step retries.
type RetryPolicy struct {
	// InitialInterval is the delay before the first retry. Zero means immediate retry.
	InitialInterval time.Duration `json:"initial_interval,omitempty"`
	// BackoffCoefficient multiplies the delay after each retry (defaults to 2.0 when InitialInterval > 0).
	BackoffCoefficient float64 `json:"backoff_coefficient,omitempty"`
	// MaxInterval caps the backoff delay (zero means uncapped).
	MaxInterval time.Duration `json:"max_interval,omitempty"`
	// NonRetryableErrorCodes lists error codes that fail the step immediately without retrying.
	NonRetryableErrorCodes []string `json:"non_retryable_error_codes,omitempty"`
}

// IsNonRetryableCode reports whether code is explicitly listed as non-retryable.
func (p *RetryPolicy) IsNonRetryableCode(code string) bool {
	if p == nil || code == "" {
		return false
	}
	for _, c := range p.NonRetryableErrorCodes {
		if c == code {
			return true
		}
	}
	return false
}

// NextDelay computes the backoff delay after the given 1-based attempt number.
func (p *RetryPolicy) NextDelay(attempt int) time.Duration {
	if p == nil || p.InitialInterval <= 0 {
		return 0
	}
	coeff := p.BackoffCoefficient
	if coeff <= 0 {
		coeff = 2.0
	}
	delay := float64(p.InitialInterval)
	for i := 1; i < attempt; i++ {
		delay *= coeff
		if p.MaxInterval > 0 && time.Duration(delay) >= p.MaxInterval {
			return p.MaxInterval
		}
	}
	d := time.Duration(delay)
	if p.MaxInterval > 0 && d > p.MaxInterval {
		return p.MaxInterval
	}
	return d
}

// StepDefinition defines a single activity or task within a workflow.
type StepDefinition struct {
	Name string `json:"name"`
	// Kind defaults to KindActivity.
	Kind     StepKind `json:"kind,omitempty"`
	Activity string   `json:"activity,omitempty"`
	// Signal names the signal a KindWaitSignal step waits for.
	Signal string `json:"signal,omitempty"`
	// Delay is how long a KindSleep step sleeps.
	Delay              time.Duration   `json:"delay,omitempty"`
	Input              json.RawMessage `json:"input,omitempty"`
	Retries            int             `json:"retries,omitempty"`
	RetryPolicy        *RetryPolicy    `json:"retry_policy,omitempty"`
	Timeout            time.Duration   `json:"timeout,omitempty"`
	CompensatingAction string          `json:"compensating_action,omitempty"`
	DependsOn          []string        `json:"depends_on,omitempty"`
	// When is an optional condition evaluated when dependencies finish; if false, the step is skipped.
	When *Condition `json:"when,omitempty"`
}

// WorkflowDefinition defines the static blueprint for a workflow DAG or sequential pipeline.
type WorkflowDefinition struct {
	Name  string           `json:"name"`
	Steps []StepDefinition `json:"steps"`
}

// StepExecution records the runtime state and results of a step execution.
type StepExecution struct {
	Name     string   `json:"name"`
	Kind     StepKind `json:"kind,omitempty"`
	Activity string   `json:"activity,omitempty"`
	Signal   string   `json:"signal,omitempty"`
	// Delay is how long a sleep step sleeps.
	Delay time.Duration `json:"delay,omitempty"`
	// WakeAt is when a waiting, sleeping, or backoff-retrying step is due to be resolved by the engine.
	WakeAt      *time.Time      `json:"wake_at,omitempty"`
	Status      StepStatus      `json:"status"`
	Input       json.RawMessage `json:"input,omitempty"`
	Output      json.RawMessage `json:"output,omitempty"`
	// Progress holds the latest heartbeat checkpoint reported by a running activity.
	Progress            json.RawMessage `json:"progress,omitempty"`
	Error               string          `json:"error,omitempty"`
	ErrorCode           string          `json:"error_code,omitempty"`
	Attempt             int             `json:"attempt"`
	CompensationAttempt int             `json:"compensation_attempt,omitempty"`
	MaxRetries          int             `json:"max_retries"`
	RetryPolicy         *RetryPolicy    `json:"retry_policy,omitempty"`
	Timeout             time.Duration   `json:"timeout,omitempty"`
	CompensatingAction  string          `json:"compensating_action,omitempty"`
	DependsOn           []string        `json:"depends_on,omitempty"`
	When                *Condition      `json:"when,omitempty"`
	StartedAt           *time.Time      `json:"started_at,omitempty"`
	CompletedAt         *time.Time      `json:"completed_at,omitempty"`
	CompensatedAt       *time.Time      `json:"compensated_at,omitempty"`
	DispatchedVersion   uint64          `json:"dispatched_version,omitempty"`
}

// Event records a state transition or milestone in the workflow audit log.
type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	StepName  string          `json:"step_name,omitempty"`
	Message   string          `json:"message,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// WorkflowInstance is the stateful runtime instance of a workflow.
type WorkflowInstance struct {
	WorkflowID     string             `json:"workflow_id"`
	RunID          string             `json:"run_id"`
	DefinitionName string             `json:"definition_name"`
	Definition     WorkflowDefinition `json:"definition"`
	Status         Status             `json:"status"`
	Input          json.RawMessage    `json:"input,omitempty"`
	Output         json.RawMessage    `json:"output,omitempty"`
	Error          string             `json:"error,omitempty"`
	// ErrorCode is the machine-readable code of the failure that ended the workflow, if the worker supplied one.
	ErrorCode string                    `json:"error_code,omitempty"`
	Steps     map[string]*StepExecution `json:"steps"`
	// Signals buffers payloads that arrived before a step waited for them.
	Signals     map[string][]json.RawMessage `json:"signals,omitempty"`
	StepOrder   []string                     `json:"step_order"`
	History     []Event                      `json:"history,omitempty"`
	CreatedAt   time.Time                    `json:"created_at"`
	UpdatedAt   time.Time                    `json:"updated_at"`
	CompletedAt *time.Time                   `json:"completed_at,omitempty"`
	Metadata    map[string]string            `json:"metadata,omitempty"`
	// Version increases by one with every change to the instance. It is
	// identical on every replica, so watchers can skip changes they have seen.
	Version uint64 `json:"version"`
}

// NewWorkflowInstance creates a new initialized workflow instance from a definition.
func NewWorkflowInstance(workflowID, runID string, def WorkflowDefinition, input json.RawMessage, meta map[string]string) (*WorkflowInstance, error) {
	return NewWorkflowInstanceAt(workflowID, runID, def, input, meta, time.Now().UTC())
}

// NewWorkflowInstanceAt creates a new initialized workflow instance with an explicit deterministic timestamp.
func NewWorkflowInstanceAt(workflowID, runID string, def WorkflowDefinition, input json.RawMessage, meta map[string]string, now time.Time) (*WorkflowInstance, error) {
	if err := def.Validate(); err != nil {
		return nil, fmt.Errorf("invalid workflow definition: %w", err)
	}

	normDef := def.Normalized()
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}

	steps := make(map[string]*StepExecution, len(normDef.Steps))
	stepOrder := make([]string, 0, len(normDef.Steps))

	for _, s := range normDef.Steps {
		stepOrder = append(stepOrder, s.Name)
		stepInput := s.Input
		if len(stepInput) == 0 {
			stepInput = input
		}

		var rp *RetryPolicy
		if s.RetryPolicy != nil {
			cp := *s.RetryPolicy
			if len(s.RetryPolicy.NonRetryableErrorCodes) > 0 {
				cp.NonRetryableErrorCodes = append([]string(nil), s.RetryPolicy.NonRetryableErrorCodes...)
			}
			rp = &cp
		}

		var cond *Condition
		if s.When != nil {
			cp := *s.When
			if len(s.When.Value) > 0 {
				cp.Value = append(json.RawMessage(nil), s.When.Value...)
			}
			cond = &cp
		}

		steps[s.Name] = &StepExecution{
			Name:               s.Name,
			Kind:               s.Kind,
			Activity:           s.Activity,
			Signal:             s.Signal,
			Delay:              s.Delay,
			Status:             StepStatusPending,
			Input:              stepInput,
			MaxRetries:         s.Retries,
			RetryPolicy:        rp,
			Timeout:            s.Timeout,
			CompensatingAction: s.CompensatingAction,
			DependsOn:          append([]string(nil), s.DependsOn...),
			When:               cond,
		}
	}

	if meta == nil {
		meta = make(map[string]string)
	}

	inst := &WorkflowInstance{
		WorkflowID:     workflowID,
		RunID:          runID,
		DefinitionName: normDef.Name,
		Definition:     normDef,
		Status:         StatusRunning,
		Input:          input,
		Steps:          steps,
		StepOrder:      stepOrder,
		History:        make([]Event, 0),
		CreatedAt:      now,
		UpdatedAt:      now,
		Metadata:       meta,
	}

	inst.RecordEventAt("WORKFLOW_STARTED", "", "Workflow started", input, now)
	return inst, nil
}

// RecordEvent appends an audit event to the instance history using the workflow's UpdatedAt (or current UTC time).
func (w *WorkflowInstance) RecordEvent(eventType, stepName, message string, payload any) {
	ts := w.UpdatedAt
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	w.RecordEventAt(eventType, stepName, message, payload, ts)
}

// RecordEventAt appends an audit event with an explicit deterministic timestamp.
func (w *WorkflowInstance) RecordEventAt(eventType, stepName, message string, payload any, at time.Time) {
	if at.IsZero() {
		at = w.UpdatedAt
		if at.IsZero() {
			at = time.Now().UTC()
		}
	} else {
		at = at.UTC()
	}
	var rawPayload json.RawMessage
	if payload != nil {
		if b, ok := payload.([]byte); ok {
			rawPayload = b
		} else if rm, ok := payload.(json.RawMessage); ok {
			rawPayload = rm
		} else if b, err := json.Marshal(payload); err == nil {
			rawPayload = b
		}
	}

	seq := len(w.History) + 1
	eventID := fmt.Sprintf("evt-%d-%d", at.UnixNano(), seq)
	w.History = append(w.History, Event{
		ID:        eventID,
		Type:      eventType,
		Timestamp: at,
		StepName:  stepName,
		Message:   message,
		Payload:   rawPayload,
	})
	w.UpdatedAt = at
}

// NextReadySteps returns all steps that are currently pending (and not parked on retry backoff)
// and have all their dependencies resolved (COMPLETED or SKIPPED).
func (w *WorkflowInstance) NextReadySteps() []*StepExecution {
	if w.Status != StatusRunning {
		return nil
	}

	var ready []*StepExecution
	for _, stepName := range w.StepOrder {
		step := w.Steps[stepName]
		if step == nil || step.Status != StepStatusPending || step.WakeAt != nil {
			continue
		}

		// Check if all dependencies are resolved (COMPLETED or SKIPPED)
		depsSatisfied := true
		for _, dep := range step.DependsOn {
			depStep, exists := w.Steps[dep]
			if !exists || (depStep.Status != StepStatusCompleted && depStep.Status != StepStatusSkipped) {
				depsSatisfied = false
				break
			}
		}

		if depsSatisfied {
			ready = append(ready, step)
		}
	}
	return ready
}

// ShouldSkipStep reports whether a ready pending step should be skipped because:
// 1. It has a When condition that evaluates to false, OR
// 2. It has dependencies, ALL of its dependencies were SKIPPED, and it has no explicit When condition overriding that.
func (w *WorkflowInstance) ShouldSkipStep(step *StepExecution) (bool, string) {
	if step.When != nil {
		if !step.When.Evaluate(w) {
			return true, fmt.Sprintf("Step %s skipped (when condition on %q evaluated to false)", step.Name, step.When.Field)
		}
		return false, ""
	}
	if len(step.DependsOn) > 0 {
		allDepsSkipped := true
		for _, dep := range step.DependsOn {
			if ds := w.Steps[dep]; ds == nil || ds.Status != StepStatusSkipped {
				allDepsSkipped = false
				break
			}
		}
		if allDepsSkipped {
			return true, fmt.Sprintf("Step %s skipped (all upstream dependencies skipped)", step.Name)
		}
	}
	return false, ""
}

// NextCompensatingStep returns the next completed step that requires compensation (in reverse order of completion).
func (w *WorkflowInstance) NextCompensatingStep() *StepExecution {
	if w.IsTerminal() {
		return nil
	}

	var candidates []*StepExecution
	for _, stepName := range w.StepOrder {
		step := w.Steps[stepName]
		if step == nil {
			continue
		}
		// A step needs compensation if it completed, has a compensating action, and hasn't been compensated yet.
		if step.Status == StepStatusCompleted && step.CompensatingAction != "" && step.CompensatedAt == nil {
			candidates = append(candidates, step)
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	// Sort candidates in reverse order of completion (LIFO)
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].CompletedAt != nil && candidates[j].CompletedAt != nil {
			return candidates[i].CompletedAt.After(*candidates[j].CompletedAt)
		}
		// Fallback to reverse definition order
		return i > j
	})

	return candidates[0]
}

// DefinesSignal reports whether any step of the workflow waits for the named signal.
func (w *WorkflowInstance) DefinesSignal(name string) bool {
	for _, s := range w.Steps {
		if s.Kind == KindWaitSignal && s.Signal == name {
			return true
		}
	}
	return false
}

// IsActivity reports whether the step runs on a worker.
func (s *StepExecution) IsActivity() bool { return s.Kind == "" || s.Kind == KindActivity }

// IsActivity reports whether the step runs on a worker.
func (s StepDefinition) IsActivity() bool { return s.Kind == "" || s.Kind == KindActivity }

// IsTerminal returns true if the workflow is in a terminal status.
func (w *WorkflowInstance) IsTerminal() bool {
	return w.Status == StatusCompleted ||
		w.Status == StatusFailed ||
		w.Status == StatusCompensated ||
		w.Status == StatusCancelled ||
		w.Status == StatusCompensationFailed
}

// AllStepsCompleted returns true if every step in the workflow has resolved (COMPLETED or SKIPPED via When condition).
func (w *WorkflowInstance) AllStepsCompleted() bool {
	if len(w.Steps) == 0 {
		return false
	}
	for _, step := range w.Steps {
		if step.Status != StepStatusCompleted && step.Status != StepStatusSkipped {
			return false
		}
	}
	return true
}

// DescendantsOf returns the names of all steps that transitively depend on rootStep (in topological order).
func (w *WorkflowInstance) DescendantsOf(rootStep string) []string {
	desc := make(map[string]bool)
	changed := true
	for changed {
		changed = false
		for _, name := range w.StepOrder {
			if desc[name] {
				continue
			}
			st := w.Steps[name]
			if st == nil {
				continue
			}
			for _, dep := range st.DependsOn {
				if dep == rootStep || desc[dep] {
					desc[name] = true
					changed = true
					break
				}
			}
		}
	}
	var out []string
	for _, name := range w.StepOrder {
		if desc[name] {
			out = append(out, name)
		}
	}
	return out
}

// AllCompensationsCompleted returns true if every step that required compensation has completed compensation.
func (w *WorkflowInstance) AllCompensationsCompleted() bool {
	for _, step := range w.Steps {
		if step.CompensatingAction != "" && step.CompletedAt != nil && step.Status != StepStatusCompensated {
			return false
		}
	}
	return true
}

// Clone creates a deep copy of the WorkflowInstance for thread-safe reads.
func (w *WorkflowInstance) Clone() *WorkflowInstance {
	if w == nil {
		return nil
	}
	b, err := json.Marshal(w)
	if err != nil {
		return nil
	}
	var clone WorkflowInstance
	_ = json.Unmarshal(b, &clone)
	return &clone
}

// Common errors
var (
	ErrWorkflowNotFound     = errors.New("workflow instance not found")
	ErrWorkflowAlreadyExist = errors.New("workflow instance already exists")
	ErrWorkflowTerminal     = errors.New("workflow instance is in terminal state")
	ErrStepNotFound         = errors.New("step not found in workflow")
	ErrInvalidStepState     = errors.New("invalid step state transition")
	ErrUnknownSignal        = errors.New("workflow has no step waiting for that signal")
)
