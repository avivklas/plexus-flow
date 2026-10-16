package api

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/graphflow"
	"github.com/avivklas/plexus-flow/pkg/worker"
)

func watchServer(t *testing.T) (*Client, string) {
	t.Helper()
	reg := worker.NewRegistry()
	reg.Register("noop", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	})
	coord, err := graphflow.NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatalf("coordinator: %v", err)
	}
	t.Cleanup(func() { coord.Close() })
	srv := NewServer("127.0.0.1:0", coord, "node-watch")
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	return NewClient("http://" + srv.Addr()), "http://" + srv.Addr()
}

var gated = flow.WorkflowDefinition{Name: "gated", Steps: []flow.StepDefinition{
	{Name: "work", Activity: "noop"},
	{Name: "approve", Kind: flow.KindWaitSignal, Signal: "approval", DependsOn: []string{"work"}},
	{Name: "finish", Activity: "noop", DependsOn: []string{"approve"}},
}}

func TestWatchWorkflowStreamsUntilTerminal(t *testing.T) {
	client, _ := watchServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := client.StartWorkflow(ctx, flowstore.StartWorkflowRequest{WorkflowID: "wf-watch", Definition: gated}); err != nil {
		t.Fatal(err)
	}

	var versions []uint64
	signaled := false
	err := client.WatchWorkflow(ctx, "wf-watch", func(wf *flow.WorkflowInstance) error {
		versions = append(versions, wf.Version)
		// React to the push, not to a poll: approve as soon as the gate opens.
		if !signaled && wf.Steps["approve"].Status == flow.StepStatusRunning {
			signaled = true
			go func() { _ = client.SignalWorkflow(ctx, "wf-watch", "approval", map[string]bool{"ok": true}) }()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WatchWorkflow: %v", err)
	}
	if !signaled {
		t.Fatal("never saw the approval gate open")
	}
	for i := 1; i < len(versions); i++ {
		if versions[i] <= versions[i-1] {
			t.Fatalf("versions not strictly increasing: %v", versions)
		}
	}

	final, err := client.WaitWorkflow(ctx, "wf-watch") // already terminal: returns at once
	if err != nil || final.Status != flow.StatusCompleted {
		t.Fatalf("WaitWorkflow = %v, %v; want COMPLETED", final, err)
	}
}

func TestWatchWorkflowNotFound(t *testing.T) {
	client, _ := watchServer(t)
	if _, err := client.WaitWorkflow(context.Background(), "nope"); err != flow.ErrWorkflowNotFound {
		t.Fatalf("err = %v, want ErrWorkflowNotFound", err)
	}
}

func TestWatchFlowTerminalOnly(t *testing.T) {
	client, base := watchServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/v1/flows/plain/watch?terminal=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content type %q", ct)
	}

	plain := flow.WorkflowDefinition{Name: "plain", Steps: []flow.StepDefinition{{Name: "s", Activity: "noop"}}}
	for _, id := range []string{"p1", "p2"} {
		if _, err := client.StartWorkflow(ctx, flowstore.StartWorkflowRequest{WorkflowID: id, Definition: plain}); err != nil {
			t.Fatal(err)
		}
	}

	got := map[string]flow.Status{}
	sc := bufio.NewScanner(resp.Body)
	for len(got) < 2 && sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var sum flow.TraceSummary
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &sum); err != nil {
			t.Fatal(err)
		}
		if sum.Status == flow.StatusRunning || sum.Status == flow.StatusPending {
			t.Fatalf("non-terminal event with terminal=1: %+v", sum)
		}
		got[sum.WorkflowID] = sum.Status
	}
	if got["p1"] != flow.StatusCompleted || got["p2"] != flow.StatusCompleted {
		t.Fatalf("terminal events = %v", got)
	}
}
