package worker

import (
	"encoding/json"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus/pkg/store"
)

// Activity Store command types
const (
	CmdDispatchActivity        store.CommandType = "activity.dispatch"
	CmdCompensateActivity      store.CommandType = "activity.compensate"
	CmdPruneWorkflowActivities store.CommandType = "activity.prune_workflow"
)

// PruneWorkflowActivitiesRequest removes completed/terminal workflow task records from ActivityStore.
type PruneWorkflowActivitiesRequest struct {
	WorkflowID string `json:"workflow_id"`
}

// ActivityTask represents a scheduled step activity execution.
type ActivityTask struct {
	WorkflowID  string            `json:"workflow_id"`
	RunID       string            `json:"run_id"`
	StepName    string            `json:"step_name"`
	Activity    string            `json:"activity"`
	Input       json.RawMessage   `json:"input,omitempty"`
	Timeout     time.Duration     `json:"timeout,omitempty"`
	Retries     int               `json:"retries,omitempty"`
	RetryPolicy *flow.RetryPolicy `json:"retry_policy,omitempty"`
	Attempt     int               `json:"attempt,omitempty"`
	// LastProgress holds the last heartbeat progress checkpoint from a prior attempt, if any.
	LastProgress json.RawMessage `json:"last_progress,omitempty"`
	// DependencyOutputs holds the outputs of the steps this step depends on, by step name.
	DependencyOutputs map[string]json.RawMessage `json:"dependency_outputs,omitempty"`
	// Metadata is the workflow metadata.
	Metadata map[string]string `json:"metadata,omitempty"`
}

// CompensationTask represents a scheduled compensating action execution.
type CompensationTask struct {
	WorkflowID  string            `json:"workflow_id"`
	RunID       string            `json:"run_id"`
	StepName    string            `json:"step_name"`
	Activity    string            `json:"activity"`
	Input       json.RawMessage   `json:"input,omitempty"`
	Timeout     time.Duration     `json:"timeout,omitempty"`
	Retries     int               `json:"retries,omitempty"`
	RetryPolicy *flow.RetryPolicy `json:"retry_policy,omitempty"`
	Attempt     int               `json:"attempt,omitempty"`
}
