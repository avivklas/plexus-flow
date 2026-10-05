package worker

import (
	"encoding/json"
	"time"

	"github.com/avivklas/plexus/pkg/store"
)

// Activity Store command types
const (
	CmdDispatchActivity   store.CommandType = "activity.dispatch"
	CmdCompensateActivity store.CommandType = "activity.compensate"
)

// ActivityTask represents a scheduled step activity execution.
type ActivityTask struct {
	WorkflowID string          `json:"workflow_id"`
	RunID      string          `json:"run_id"`
	StepName   string          `json:"step_name"`
	Activity   string          `json:"activity"`
	Input      json.RawMessage `json:"input,omitempty"`
	Timeout    time.Duration   `json:"timeout,omitempty"`
	Retries    int             `json:"retries,omitempty"`
	Attempt    int             `json:"attempt,omitempty"`
}

// CompensationTask represents a scheduled compensating action execution.
type CompensationTask struct {
	WorkflowID string          `json:"workflow_id"`
	RunID      string          `json:"run_id"`
	StepName   string          `json:"step_name"`
	Activity   string          `json:"activity"`
	Input      json.RawMessage `json:"input,omitempty"`
	Timeout    time.Duration   `json:"timeout,omitempty"`
	Retries    int             `json:"retries,omitempty"`
	Attempt    int             `json:"attempt,omitempty"`
}
