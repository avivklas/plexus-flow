package flow

import (
	"sort"
	"strings"
	"testing"
	"time"
)

func edgeSet(g FlowGraph) []string {
	var out []string
	for _, e := range g.Edges {
		out = append(out, string(e.Kind)+" "+e.ID)
	}
	sort.Strings(out)
	return out
}

func hasEdge(g FlowGraph, kind EdgeKind, id string) bool {
	for _, e := range g.Edges {
		if e.Kind == kind && e.ID == id {
			return true
		}
	}
	return false
}

func sagaDef() WorkflowDefinition {
	return WorkflowDefinition{Name: "saga", Steps: []StepDefinition{
		{Name: "reserve", Activity: "reserve", CompensatingAction: "release", Retries: 2, Timeout: 30 * time.Second},
		{Name: "charge", Activity: "charge", CompensatingAction: "refund"},
		{Name: "ship", Activity: "ship"},
	}}
}

func TestBuildGraphSequentialSaga(t *testing.T) {
	g := BuildGraph(sagaDef())
	want := []string{
		"compensation comp:charge->comp:reserve",
		"compensation comp:reserve->end:compensated",
		"failure step:charge->comp:reserve",
		"failure step:reserve->end:failed",
		"failure step:ship->comp:charge",
		"success step:charge->step:ship",
		"success step:reserve->step:charge",
		"success step:ship->end:completed",
	}
	got := edgeSet(g)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("edges mismatch\n got: %v\nwant: %v", got, want)
	}
}

func TestBuildGraphForkJoin(t *testing.T) {
	g := BuildGraph(WorkflowDefinition{Name: "fj", Steps: []StepDefinition{
		{Name: "a", Activity: "a", CompensatingAction: "undo-a"},
		{Name: "b", Activity: "b", CompensatingAction: "undo-b", DependsOn: []string{"a"}},
		{Name: "c", Activity: "c", CompensatingAction: "undo-c", DependsOn: []string{"a"}},
		{Name: "d", Activity: "d", DependsOn: []string{"b", "c"}},
	}})
	// If b fails, sibling c may have completed and is rolled back first, then a.
	if !hasEdge(g, EdgeFailure, "step:b->comp:c") {
		t.Fatalf("b failure should roll back sibling c: %v", edgeSet(g))
	}
	if hasEdge(g, EdgeFailure, "step:b->comp:a") {
		t.Fatalf("a is rolled back after c, not directly: %v", edgeSet(g))
	}
	if !hasEdge(g, EdgeCompensation, "comp:c->comp:a") || !hasEdge(g, EdgeCompensation, "comp:b->comp:a") {
		t.Fatalf("missing comp edges to a: %v", edgeSet(g))
	}
	// d failing rolls back both branches.
	if !hasEdge(g, EdgeFailure, "step:d->comp:b") || !hasEdge(g, EdgeFailure, "step:d->comp:c") {
		t.Fatalf("d failure should enter at b and c: %v", edgeSet(g))
	}
	// Fork has two success edges out of a; join has two into d.
	if !hasEdge(g, EdgeSuccess, "step:a->step:b") || !hasEdge(g, EdgeSuccess, "step:a->step:c") ||
		!hasEdge(g, EdgeSuccess, "step:b->step:d") || !hasEdge(g, EdgeSuccess, "step:c->step:d") {
		t.Fatalf("fork/join success edges wrong: %v", edgeSet(g))
	}
}

func TestBuildGraphNoFailedNodeWhenAlwaysCompensable(t *testing.T) {
	g := BuildGraph(WorkflowDefinition{Name: "x", Steps: []StepDefinition{
		{Name: "a", Activity: "a", CompensatingAction: "ua"},
	}})
	// The only step failing has nothing to roll back, so end:failed is used.
	found := false
	for _, n := range g.Nodes {
		if n.ID == EndFailed {
			found = true
		}
	}
	if !found {
		t.Fatal("expected end:failed node")
	}
}

func runInstance(t *testing.T) *WorkflowInstance {
	t.Helper()
	inst, err := NewWorkflowInstance("W1", "R1", sagaDef(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return inst
}

func TestBuildTraceCompensated(t *testing.T) {
	inst := runInstance(t)
	t0 := inst.CreatedAt
	at := func(ms int) *time.Time { x := t0.Add(time.Duration(ms) * time.Millisecond); return &x }

	r := inst.Steps["reserve"]
	r.Status, r.Attempt, r.StartedAt, r.CompletedAt = StepStatusCompensated, 1, at(0), at(100)
	r.CompensatedAt = at(400)
	c := inst.Steps["charge"]
	c.Status, c.Attempt, c.StartedAt, c.Error = StepStatusFailed, 3, at(100), "card declined"
	inst.History = append(inst.History,
		Event{Type: "STEP_FAILED", StepName: "charge", Timestamp: *at(300)},
		Event{Type: "COMPENSATION_STEP_STARTED", StepName: "reserve", Timestamp: *at(320)},
	)
	inst.Status, inst.CompletedAt = StatusCompensated, at(400)

	tr := BuildTrace(inst)
	wantPath := []string{"step:reserve", "step:charge", "comp:reserve", "end:compensated"}
	if strings.Join(tr.Path, ",") != strings.Join(wantPath, ",") {
		t.Fatalf("path = %v, want %v", tr.Path, wantPath)
	}
	if n := tr.Nodes["step:charge"]; n.Status != "failed" || n.DurationMS != 200 || n.Error != "card declined" {
		t.Fatalf("charge node: %+v", n)
	}
	if n := tr.Nodes["step:reserve"]; n.Status != "completed" || n.DurationMS != 100 {
		t.Fatalf("reserve node: %+v", n)
	}
	if n := tr.Nodes["comp:reserve"]; n.Status != "completed" || n.DurationMS != 80 {
		t.Fatalf("comp node: %+v", n)
	}
	wantEdges := []string{
		"comp:reserve->end:compensated",
		"failure step:charge->comp:reserve",
		"success step:reserve->step:charge",
	}
	var got []string
	for _, id := range tr.Edges {
		for _, e := range BuildGraph(inst.Definition).Edges {
			if e.ID == id {
				got = append(got, string(e.Kind)+" "+id)
			}
		}
	}
	// compensation kind prefix differs; compare on ids only.
	ids := strings.Join(tr.Edges, ",")
	for _, w := range []string{"step:reserve->step:charge", "step:charge->comp:reserve", "comp:reserve->end:compensated"} {
		if !strings.Contains(ids, w) {
			t.Fatalf("edge %s not taken; got %v (%v, want %v)", w, tr.Edges, got, wantEdges)
		}
	}
	if len(tr.Edges) != 3 {
		t.Fatalf("expected exactly 3 edges taken, got %v", tr.Edges)
	}
}

func TestBuildTraceCompleted(t *testing.T) {
	inst := runInstance(t)
	t0 := inst.CreatedAt
	for i, n := range inst.StepOrder {
		s := inst.Steps[n]
		st, en := t0.Add(time.Duration(i)*time.Second), t0.Add(time.Duration(i)*time.Second+500*time.Millisecond)
		s.Status, s.Attempt, s.StartedAt, s.CompletedAt = StepStatusCompleted, 1, &st, &en
	}
	end := t0.Add(3 * time.Second)
	inst.Status, inst.CompletedAt = StatusCompleted, &end

	tr := BuildTrace(inst)
	if tr.Path[len(tr.Path)-1] != EndCompleted || len(tr.Path) != 4 {
		t.Fatalf("path = %v", tr.Path)
	}
	if len(tr.Edges) != 3 {
		t.Fatalf("edges = %v", tr.Edges)
	}
}
