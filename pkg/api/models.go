package api

import "encoding/json"

// ClusterStatusResponse provides cluster membership and leadership diagnostics.
type ClusterStatusResponse struct {
	NodeID      string            `json:"node_id"`
	IsLeader    bool              `json:"is_leader"`
	LeaderID    string            `json:"leader_id,omitempty"`
	LeaderAddr  string            `json:"leader_addr,omitempty"`
	StoreCount  int               `json:"store_count"`
	WorkflowNum int               `json:"workflow_num"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// SignalRequest payload for signaling a workflow.
type SignalRequest struct {
	SignalName string `json:"signal_name"`
	Payload    any    `json:"payload,omitempty"`
}

// CancelRequest payload for cancelling a workflow.
type CancelRequest struct {
	Reason string `json:"reason"`
}

// ReplayRequest payload for replaying a workflow from a specific step with optional patched input.
type ReplayRequest struct {
	StepName string          `json:"step_name"`
	Input    json.RawMessage `json:"input,omitempty"`
}

// ErrorResponse represents a standardized JSON error message.
type ErrorResponse struct {
	Error string `json:"error"`
}
