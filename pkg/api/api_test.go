package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/graphflow"
	"github.com/avivklas/plexus-flow/pkg/worker"
)

func TestHTTPAPIAndSDK(t *testing.T) {
	reg := worker.NewRegistry()
	reg.Register("api-step-act", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"processed": true}`), nil
	})

	coord, err := graphflow.NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatalf("NewEmbeddedCoordinator failed: %v", err)
	}
	defer coord.Close()

	// Spin up server on random free port
	srv := NewServer("127.0.0.1:0", coord, "node-test-1")
	if err := srv.Start(); err != nil {
		t.Fatalf("Start server failed: %v", err)
	}
	defer srv.Close()

	client := NewClient("http://" + srv.Addr())
	ctx := context.Background()

	// 1. Cluster Status
	status, err := client.GetClusterStatus(ctx)
	if err != nil {
		t.Fatalf("GetClusterStatus failed: %v", err)
	}
	if status.NodeID != "node-test-1" || !status.IsLeader {
		t.Fatalf("unexpected cluster status: %+v", status)
	}

	// 2. Start Workflow
	def := flow.WorkflowDefinition{
		Name: "api-test-flow",
		Steps: []flow.StepDefinition{
			{Name: "step-1", Activity: "api-step-act"},
		},
	}

	wf, err := client.StartWorkflow(ctx, flowstore.StartWorkflowRequest{
		WorkflowID: "wf-api-1",
		Definition: def,
		Input:      json.RawMessage(`{"user": "alice"}`),
	})
	if err != nil {
		t.Fatalf("StartWorkflow failed: %v", err)
	}
	if wf.WorkflowID != "wf-api-1" {
		t.Fatalf("unexpected workflow id: %s", wf.WorkflowID)
	}

	// Wait for completion
	deadline := time.Now().Add(4 * time.Second)
	var completedWf *flow.WorkflowInstance
	for time.Now().Before(deadline) {
		got, err := client.GetWorkflow(ctx, "wf-api-1")
		if err == nil && got.Status == flow.StatusCompleted {
			completedWf = got
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if completedWf == nil {
		t.Fatalf("workflow did not complete via API")
	}

	// 3. List Workflows
	list, err := client.ListWorkflows(ctx)
	if err != nil {
		t.Fatalf("ListWorkflows failed: %v", err)
	}
	if len(list) < 1 {
		t.Fatalf("expected at least 1 workflow, got %d", len(list))
	}

	// 4. Get History
	history, err := client.GetHistory(ctx, "wf-api-1")
	if err != nil {
		t.Fatalf("GetHistory failed: %v", err)
	}
	if len(history) == 0 {
		t.Fatalf("expected history events, got none")
	}

	// 5. Signal Workflow: only signals some step waits for are accepted
	err = client.SignalWorkflow(ctx, "wf-api-1", "user-approved", map[string]string{"by": "manager"})
	if err == nil || !strings.Contains(err.Error(), "400") {
		t.Fatalf("expected a 400 for a signal no step waits for, got %v", err)
	}

	// 6. Cancel Workflow
	err = client.CancelWorkflow(ctx, "wf-api-1", "manual stop")
	if err != nil {
		t.Fatalf("CancelWorkflow failed: %v", err)
	}
}
