package flowstore

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus/pkg/store"
)

// ErrStepNotPending rejects dispatching a step that has already started or finished.
var ErrStepNotPending = errors.New("step is not pending")

// Flow Store Command Types
const (
	CmdStartWorkflow            store.CommandType = "flow.workflow.start"
	CmdDispatchStep             store.CommandType = "flow.step.dispatch"
	CmdCompleteStep             store.CommandType = "flow.step.complete"
	CmdFailStep                 store.CommandType = "flow.step.fail"
	CmdTriggerCompensation      store.CommandType = "flow.workflow.compensate"
	CmdStartCompensationStep    store.CommandType = "flow.step.compensation.start"
	CmdCompleteCompensationStep store.CommandType = "flow.step.compensation.complete"
	CmdFailCompensationStep     store.CommandType = "flow.step.compensation.fail"
	CmdCompleteWorkflow         store.CommandType = "flow.workflow.complete"
	CmdCancelWorkflow           store.CommandType = "flow.workflow.cancel"
	CmdSignalWorkflow           store.CommandType = "flow.workflow.signal"
	CmdFireTimer                store.CommandType = "flow.step.timer"
	CmdFireTimers               store.CommandType = "flow.step.timers"
	CmdUpdateStepProgress       store.CommandType = "flow.step.progress"
	CmdCompactWorkflows         store.CommandType = "flow.workflow.compact"
	CmdUpsertSchedule           store.CommandType = "flow.schedule.upsert"
	CmdDeleteSchedule           store.CommandType = "flow.schedule.delete"
	CmdTriggerSchedule          store.CommandType = "flow.schedule.trigger"
)

// StartWorkflowRequest initiates a new workflow execution.
type StartWorkflowRequest struct {
	WorkflowID string                  `json:"workflow_id"`
	RunID      string                  `json:"run_id,omitempty"`
	Definition flow.WorkflowDefinition `json:"definition"`
	Input      json.RawMessage         `json:"input,omitempty"`
	Metadata   map[string]string       `json:"metadata,omitempty"`
}

// DispatchStepRequest transitions a step to RUNNING state.
type DispatchStepRequest struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
	StepName   string `json:"step_name"`
	// At is the dispatch time that timers count from; zero means the apply time.
	At time.Time `json:"at,omitempty"`
}

// FireTimerRequest resolves a sleeping, signal-waiting, or retry-backoff step whose time has come.
type FireTimerRequest struct {
	WorkflowID string    `json:"workflow_id"`
	RunID      string    `json:"run_id"`
	StepName   string    `json:"step_name"`
	At         time.Time `json:"at"`
}

// FireTimersRequest resolves a batch of expired step timers in a single Raft commit.
type FireTimersRequest struct {
	Timers []TimerRef `json:"timers"`
	At     time.Time  `json:"at"`
}

// UpdateStepProgressRequest records an activity heartbeat progress checkpoint on a running step.
type UpdateStepProgressRequest struct {
	WorkflowID string          `json:"workflow_id"`
	RunID      string          `json:"run_id,omitempty"`
	StepName   string          `json:"step_name"`
	Progress   json.RawMessage `json:"progress"`
}

// CompactWorkflowsRequest evicts terminal workflows older than Before or beyond MaxRetained.
type CompactWorkflowsRequest struct {
	// Before evicts terminal workflows completed at or before this timestamp (if non-zero).
	Before time.Time `json:"before,omitempty"`
	// MaxRetained caps the number of terminal workflows kept in hot memory (0 means no count cap).
	MaxRetained int `json:"max_retained,omitempty"`
}

// CompactWorkflowsResponse reports which terminal workflows were evicted/archived.
type CompactWorkflowsResponse struct {
	Compacted   int      `json:"compacted"`
	WorkflowIDs []string `json:"workflow_ids,omitempty"`
}

// UpsertScheduleRequest creates or updates a durable recurring workflow schedule.
type UpsertScheduleRequest struct {
	Schedule flow.Schedule `json:"schedule"`
	Now      time.Time     `json:"now,omitempty"`
}

// DeleteScheduleRequest removes a schedule by ID.
type DeleteScheduleRequest struct {
	ScheduleID string `json:"schedule_id"`
}

// TriggerScheduleRequest fires a due schedule at the given timestamp, starting its workflow run.
type TriggerScheduleRequest struct {
	ScheduleID string    `json:"schedule_id"`
	At         time.Time `json:"at"`
}

// CompleteStepRequest marks a step execution as successfully completed with output.
type CompleteStepRequest struct {
	WorkflowID string          `json:"workflow_id"`
	RunID      string          `json:"run_id"`
	StepName   string          `json:"step_name"`
	Output     json.RawMessage `json:"output,omitempty"`
}

// FailStepRequest marks a step execution as failed and triggers retry or saga compensation.
type FailStepRequest struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
	StepName   string `json:"step_name"`
	Error      string `json:"error"`
	// Code is a machine-readable failure code (e.g. "STUDY_NOT_FOUND"); free-form, may be empty.
	Code      string `json:"code,omitempty"`
	Retryable bool   `json:"retryable"`
}

// TriggerCompensationRequest manually or automatically forces workflow into COMPENSATING status.
type TriggerCompensationRequest struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
	Reason     string `json:"reason"`
}

// StartCompensationStepRequest marks a compensating action as actively running.
type StartCompensationStepRequest struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
	StepName   string `json:"step_name"`
}

// CompleteCompensationStepRequest marks a compensating step as COMPENSATED.
type CompleteCompensationStepRequest struct {
	WorkflowID string          `json:"workflow_id"`
	RunID      string          `json:"run_id"`
	StepName   string          `json:"step_name"`
	Output     json.RawMessage `json:"output,omitempty"`
}

// FailCompensationStepRequest records that a compensation activity failed.
type FailCompensationStepRequest struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
	StepName   string `json:"step_name"`
	Error      string `json:"error"`
	Code       string `json:"code,omitempty"`
	Retryable  bool   `json:"retryable,omitempty"`
}

// CompleteWorkflowRequest marks the entire workflow as COMPLETED.
type CompleteWorkflowRequest struct {
	WorkflowID string          `json:"workflow_id"`
	RunID      string          `json:"run_id"`
	Output     json.RawMessage `json:"output,omitempty"`
}

// CancelWorkflowRequest requests cancellation of a running workflow.
type CancelWorkflowRequest struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
	Reason     string `json:"reason"`
}

// SignalWorkflowRequest transmits an external signal/event to a workflow.
type SignalWorkflowRequest struct {
	WorkflowID string          `json:"workflow_id"`
	RunID      string          `json:"run_id"`
	SignalName string          `json:"signal_name"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}
