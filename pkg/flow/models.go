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
	Timeout            time.Duration   `json:"timeout,omitempty"`
	CompensatingAction string          `json:"compensating_action,omitempty"`
	DependsOn          []string        `json:"depends_on,omitempty"`
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
	// WakeAt is when a waiting or sleeping step is due to be resolved by the engine.
	WakeAt             *time.Time      `json:"wake_at,omitempty"`
	Status             StepStatus      `json:"status"`
	Input              json.RawMessage `json:"input,omitempty"`
	Output             json.RawMessage `json:"output,omitempty"`
	Error              string          `json:"error,omitempty"`
	ErrorCode          string          `json:"error_code,omitempty"`
	Attempt            int             `json:"attempt"`
	MaxRetries         int             `json:"max_retries"`
	Timeout            time.Duration   `json:"timeout,omitempty"`
	CompensatingAction string          `json:"compensating_action,omitempty"`
	DependsOn          []string        `json:"depends_on,omitempty"`
	StartedAt          *time.Time      `json:"started_at,omitempty"`
	CompletedAt        *time.Time      `json:"completed_at,omitempty"`
	CompensatedAt      *time.Time      `json:"compensated_at,omitempty"`
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
}

// NewWorkflowInstance creates a new initialized workflow instance from a definition.
func NewWorkflowInstance(workflowID, runID string, def WorkflowDefinition, input json.RawMessage, meta map[string]string) (*WorkflowInstance, error) {
	if err := def.Validate(); err != nil {
		return nil, fmt.Errorf("invalid workflow definition: %w", err)
	}

	normDef := def.Normalized()
	now := time.Now().UTC()

	steps := make(map[string]*StepExecution, len(normDef.Steps))
	stepOrder := make([]string, 0, len(normDef.Steps))

	for _, s := range normDef.Steps {
		stepOrder = append(stepOrder, s.Name)
		stepInput := s.Input
		if len(stepInput) == 0 {
			stepInput = input
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
			Timeout:            s.Timeout,
			CompensatingAction: s.CompensatingAction,
			DependsOn:          append([]string(nil), s.DependsOn...),
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

	inst.RecordEvent("WORKFLOW_STARTED", "", "Workflow started", input)
	return inst, nil
}

// RecordEvent appends an audit event to the instance history.
func (w *WorkflowInstance) RecordEvent(eventType, stepName, message string, payload any) {
	var rawPayload json.RawMessage
	if payload != nil {
		if b, ok := payload.([]byte); ok {
			rawPayload = b
		} else if b, err := json.Marshal(payload); err == nil {
			rawPayload = b
		}
	}

	eventID := fmt.Sprintf("evt-%d-%d", time.Now().UTC().UnixNano(), len(w.History)+1)
	w.History = append(w.History, Event{
		ID:        eventID,
		Type:      eventType,
		Timestamp: time.Now().UTC(),
		StepName:  stepName,
		Message:   message,
		Payload:   rawPayload,
	})
	w.UpdatedAt = time.Now().UTC()
}

// NextReadySteps returns all steps that are currently pending and have all their dependencies satisfied.
func (w *WorkflowInstance) NextReadySteps() []*StepExecution {
	if w.Status != StatusRunning {
		return nil
	}

	var ready []*StepExecution
	for _, stepName := range w.StepOrder {
		step := w.Steps[stepName]
		if step == nil || step.Status != StepStatusPending {
			continue
		}

		// Check if all dependencies are completed
		depsSatisfied := true
		for _, dep := range step.DependsOn {
			depStep, exists := w.Steps[dep]
			if !exists || depStep.Status != StepStatusCompleted {
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

// AllStepsCompleted returns true if every step in the workflow has status COMPLETED.
func (w *WorkflowInstance) AllStepsCompleted() bool {
	if len(w.Steps) == 0 {
		return false
	}
	for _, step := range w.Steps {
		if step.Status != StepStatusCompleted {
			return false
		}
	}
	return true
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
