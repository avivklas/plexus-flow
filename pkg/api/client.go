package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
)

// Client is the Go SDK client for Plexus-Flow.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a new Plexus-Flow SDK client.
func NewClient(baseURL string) *Client {
	baseURL = strings.TrimRight(baseURL, "/")
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "http://" + baseURL
	}
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// StartWorkflow initiates a workflow execution.
func (c *Client) StartWorkflow(ctx context.Context, req flowstore.StartWorkflowRequest) (*flow.WorkflowInstance, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/workflows/start", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("start workflow failed (status %d): %s", resp.StatusCode, errResp.Error)
	}

	var wf flow.WorkflowInstance
	if err := json.NewDecoder(resp.Body).Decode(&wf); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &wf, nil
}

// GetWorkflow retrieves the current state of a workflow.
func (c *Client) GetWorkflow(ctx context.Context, workflowID string) (*flow.WorkflowInstance, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/workflows/"+workflowID, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, flow.ErrWorkflowNotFound
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get workflow failed (status %d): %s", resp.StatusCode, string(body))
	}

	var wf flow.WorkflowInstance
	if err := json.NewDecoder(resp.Body).Decode(&wf); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &wf, nil
}

// ListWorkflows lists all workflows in the system.
func (c *Client) ListWorkflows(ctx context.Context) ([]*flow.WorkflowInstance, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/workflows", nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list workflows failed with status %d", resp.StatusCode)
	}

	var list []*flow.WorkflowInstance
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return list, nil
}

// GetHistory retrieves the audit event history for a workflow.
func (c *Client) GetHistory(ctx context.Context, workflowID string) ([]flow.Event, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/workflows/"+workflowID+"/history", nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, flow.ErrWorkflowNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get history failed with status %d", resp.StatusCode)
	}

	var history []flow.Event
	if err := json.NewDecoder(resp.Body).Decode(&history); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return history, nil
}

// SignalWorkflow sends an external signal to a running workflow.
func (c *Client) SignalWorkflow(ctx context.Context, workflowID, signalName string, payload any) error {
	sigReq := SignalRequest{
		SignalName: signalName,
		Payload:    payload,
	}
	b, _ := json.Marshal(sigReq)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/workflows/"+workflowID+"/signal", bytes.NewReader(b))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("signal workflow failed with status %d", resp.StatusCode)
	}
	return nil
}

// CancelWorkflow cancels a running workflow and triggers saga compensation.
func (c *Client) CancelWorkflow(ctx context.Context, workflowID, reason string) error {
	req := CancelRequest{Reason: reason}
	b, _ := json.Marshal(req)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/workflows/"+workflowID+"/cancel", bytes.NewReader(b))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("cancel workflow failed with status %d", resp.StatusCode)
	}
	return nil
}

// GetClusterStatus queries cluster diagnostics.
func (c *Client) GetClusterStatus(ctx context.Context) (*ClusterStatusResponse, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/cluster/status", nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get cluster status failed with status %d", resp.StatusCode)
	}

	var status ClusterStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &status, nil
}

// UpsertSchedule registers or updates a recurring workflow schedule.
func (c *Client) UpsertSchedule(ctx context.Context, sched flow.Schedule) (*flow.Schedule, error) {
	b, err := json.Marshal(sched)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/schedules", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("upsert schedule failed (status %d): %s", resp.StatusCode, errResp.Error)
	}
	var out flow.Schedule
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListSchedules lists all registered recurring workflow schedules.
func (c *Client) ListSchedules(ctx context.Context) ([]*flow.Schedule, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/schedules", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list schedules failed with status %d", resp.StatusCode)
	}
	var out []*flow.Schedule
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// DeleteSchedule deletes a recurring workflow schedule by ID.
func (c *Client) DeleteSchedule(ctx context.Context, scheduleID string) error {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/api/v1/schedules/"+scheduleID, nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return flow.ErrScheduleNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("delete schedule failed with status %d", resp.StatusCode)
	}
	return nil
}

// CompactWorkflows triggers eviction/archival of terminal workflows via the HTTP API.
func (c *Client) CompactWorkflows(ctx context.Context, req flowstore.CompactWorkflowsRequest) (*flowstore.CompactWorkflowsResponse, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/workflows/compact", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("compact workflows failed with status %d", resp.StatusCode)
	}
	var out flowstore.CompactWorkflowsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ReplayStep resets a step (and its downstream dependents) with optional patched input and resumes the workflow.
func (c *Client) ReplayStep(ctx context.Context, workflowID, stepName string, input json.RawMessage) (*flow.WorkflowInstance, error) {
	b, err := json.Marshal(ReplayRequest{
		StepName: stepName,
		Input:    input,
	})
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/workflows/"+workflowID+"/replay", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, flow.ErrWorkflowNotFound
	}
	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("replay step failed (status %d): %s", resp.StatusCode, errResp.Error)
	}
	var wf flow.WorkflowInstance
	if err := json.NewDecoder(resp.Body).Decode(&wf); err != nil {
		return nil, err
	}
	return &wf, nil
}

// SearchWorkflows executes a structured, full-text, metadata, and faceted search query.
func (c *Client) SearchWorkflows(ctx context.Context, q flowstore.SearchQuery) (*flowstore.SearchResult, error) {
	b, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/workflows/search", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("search workflows failed (status %d): %s", resp.StatusCode, errResp.Error)
	}
	var out flowstore.SearchResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}


