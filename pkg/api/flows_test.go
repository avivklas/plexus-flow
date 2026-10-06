package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/graphflow"
	"github.com/avivklas/plexus-flow/pkg/worker"
)

func getJSON(t *testing.T, url string, out any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
}

func TestFlowsAndTracesAPI(t *testing.T) {
	ok := func(context.Context, json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{}`), nil }
	reg := worker.NewRegistry()
	reg.Register("reserve", ok)
	reg.Register("release", ok)
	reg.Register("charge", func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New("card declined")
	})

	coord, err := graphflow.NewEmbeddedCoordinator(reg)
	if err != nil {
		t.Fatal(err)
	}
	defer coord.Close()
	srv := NewServer("127.0.0.1:0", coord, "n1")
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	base := "http://" + srv.Addr()

	def := flow.WorkflowDefinition{Name: "order", Steps: []flow.StepDefinition{
		{Name: "reserve", Activity: "reserve", CompensatingAction: "release"},
		{Name: "charge", Activity: "charge"},
	}}
	// A registered flow is visible before any run.
	if err := coord.RegisterFlow(def); err != nil {
		t.Fatal(err)
	}
	var flows []FlowSummary
	getJSON(t, base+"/api/v1/flows", &flows)
	if len(flows) != 1 || flows[0].Name != "order" || flows[0].Traces != 0 {
		t.Fatalf("flows = %+v", flows)
	}

	if _, err := coord.StartWorkflow(context.Background(), flowstore.StartWorkflowRequest{WorkflowID: "ORD-1", Definition: def}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if wf, _ := coord.GetWorkflow("ORD-1"); wf != nil && wf.Status == flow.StatusCompensated {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	var g flow.FlowGraph
	getJSON(t, base+"/api/v1/flows/order", &g)
	if len(g.Edges) == 0 || len(g.Nodes) < 4 {
		t.Fatalf("graph = %+v", g)
	}

	var list []flow.TraceSummary
	getJSON(t, base+"/api/v1/flows/order/traces?status=compensated", &list)
	if len(list) != 1 || list[0].WorkflowID != "ORD-1" {
		t.Fatalf("traces = %+v", list)
	}
	getJSON(t, base+"/api/v1/flows/order/traces?status=completed", &list)
	if len(list) != 0 {
		t.Fatalf("status filter ignored: %+v", list)
	}

	var tr flow.Trace
	getJSON(t, base+"/api/v1/traces/ORD-1", &tr)
	if got := strings.Join(tr.Path, ","); got != "step:reserve,step:charge,comp:reserve,end:compensated" {
		t.Fatalf("path = %s", got)
	}
	if tr.Nodes["step:charge"].Error != "card declined" {
		t.Fatalf("charge node: %+v", tr.Nodes["step:charge"])
	}

	resp, _ := http.Get(base + "/api/v1/flows/nope")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown flow status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, _ = http.Get(base + "/")
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/html") {
		t.Fatalf("dashboard not served: %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	resp.Body.Close()
}
