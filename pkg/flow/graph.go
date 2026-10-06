package flow

import (
	"encoding/json"
	"sort"
	"time"
)

// NodeKind classifies a node in a flow graph.
type NodeKind string

const (
	NodeStep         NodeKind = "step"
	NodeCompensation NodeKind = "compensation"
	NodeEnd          NodeKind = "end"
)

// EdgeKind classifies an edge in a flow graph.
type EdgeKind string

const (
	// EdgeSuccess is followed when the source step completes.
	EdgeSuccess EdgeKind = "success"
	// EdgeFailure is followed when the source step fails permanently.
	EdgeFailure EdgeKind = "failure"
	// EdgeCompensation is followed when a compensating action completes.
	EdgeCompensation EdgeKind = "compensation"
)

// Well-known terminal node IDs.
const (
	EndCompleted   = "end:completed"
	EndCompensated = "end:compensated"
	EndFailed      = "end:failed"
)

// StepNodeID returns the graph node ID of a step.
func StepNodeID(step string) string { return "step:" + step }

// CompNodeID returns the graph node ID of a step's compensating action.
func CompNodeID(step string) string { return "comp:" + step }

// GraphNode is one activity (or terminal state) in a flow.
type GraphNode struct {
	ID        string   `json:"id"`
	Kind      NodeKind `json:"kind"`
	Label     string   `json:"label"`
	Step      string   `json:"step,omitempty"`
	Activity  string   `json:"activity,omitempty"`
	Retries   int      `json:"retries,omitempty"`
	TimeoutMS int64    `json:"timeout_ms,omitempty"`
	DependsOn []string `json:"depends_on,omitempty"`
	// Undoes is the step a compensation node reverts.
	Undoes string `json:"undoes,omitempty"`
}

// GraphEdge is a possible transition between two nodes.
type GraphEdge struct {
	ID   string   `json:"id"`
	From string   `json:"from"`
	To   string   `json:"to"`
	Kind EdgeKind `json:"kind"`
}

// FlowGraph is the self-describing shape of a flow: every activity and every
// transition, including failure and rollback paths.
type FlowGraph struct {
	Name  string      `json:"name"`
	Nodes []GraphNode `json:"nodes"`
	Edges []GraphEdge `json:"edges"`
}

// BuildGraph derives the full transition graph of a workflow definition.
//
// Failure semantics mirror the engine: when a step fails permanently, every
// completed step that has a compensating action is compensated in reverse
// order. If nothing can have been completed, the workflow ends as failed.
func BuildGraph(def WorkflowDefinition) FlowGraph {
	def = def.Normalized()
	g := FlowGraph{Name: def.Name}

	steps := make(map[string]StepDefinition, len(def.Steps))
	dependents := make(map[string][]string)
	for _, s := range def.Steps {
		steps[s.Name] = s
		for _, d := range s.DependsOn {
			dependents[d] = append(dependents[d], s.Name)
		}
	}

	// Transitive closure of ancestors.
	ancestors := make(map[string]map[string]bool, len(def.Steps))
	var collect func(name string) map[string]bool
	collect = func(name string) map[string]bool {
		if a, ok := ancestors[name]; ok {
			return a
		}
		a := map[string]bool{}
		ancestors[name] = a // guards against cycles; Validate rejects them anyway
		for _, d := range steps[name].DependsOn {
			a[d] = true
			for x := range collect(d) {
				a[x] = true
			}
		}
		return a
	}
	for _, s := range def.Steps {
		collect(s.Name)
	}
	isAncestor := func(anc, of string) bool { return ancestors[of][anc] }

	// maximal returns the members of set that have no descendant inside set,
	// i.e. the ones that would be rolled back first.
	maximal := func(set []string) []string {
		var out []string
		for _, a := range set {
			hasDesc := false
			for _, b := range set {
				if a != b && isAncestor(a, b) {
					hasDesc = true
					break
				}
			}
			if !hasDesc {
				out = append(out, a)
			}
		}
		return out
	}

	addEdge := func(from, to string, kind EdgeKind) {
		g.Edges = append(g.Edges, GraphEdge{ID: from + "->" + to, From: from, To: to, Kind: kind})
	}

	for _, s := range def.Steps {
		g.Nodes = append(g.Nodes, GraphNode{
			ID: StepNodeID(s.Name), Kind: NodeStep, Label: s.Name, Step: s.Name,
			Activity: s.Activity, Retries: s.Retries, TimeoutMS: s.Timeout.Milliseconds(),
			DependsOn: append([]string(nil), s.DependsOn...),
		})
	}
	for _, s := range def.Steps {
		if s.CompensatingAction != "" {
			g.Nodes = append(g.Nodes, GraphNode{
				ID: CompNodeID(s.Name), Kind: NodeCompensation, Label: s.CompensatingAction,
				Step: s.Name, Activity: s.CompensatingAction, Undoes: s.Name,
			})
		}
	}
	g.Nodes = append(g.Nodes,
		GraphNode{ID: EndCompleted, Kind: NodeEnd, Label: "Completed"},
		GraphNode{ID: EndCompensated, Kind: NodeEnd, Label: "Compensated"},
		GraphNode{ID: EndFailed, Kind: NodeEnd, Label: "Failed"},
	)

	// Success edges.
	for _, s := range def.Steps {
		for _, d := range s.DependsOn {
			addEdge(StepNodeID(d), StepNodeID(s.Name), EdgeSuccess)
		}
		if len(dependents[s.Name]) == 0 {
			addEdge(StepNodeID(s.Name), EndCompleted, EdgeSuccess)
		}
	}

	// Failure edges: failing step -> first compensations to run.
	usesFailed := false
	for _, s := range def.Steps {
		var candidates []string
		for _, o := range def.Steps {
			if o.Name == s.Name || o.CompensatingAction == "" || isAncestor(s.Name, o.Name) {
				continue
			}
			candidates = append(candidates, o.Name)
		}
		entry := maximal(candidates)
		if len(entry) == 0 {
			addEdge(StepNodeID(s.Name), EndFailed, EdgeFailure)
			usesFailed = true
			continue
		}
		for _, e := range entry {
			addEdge(StepNodeID(s.Name), CompNodeID(e), EdgeFailure)
		}
	}

	// Compensation edges: each rollback hands over to its nearest compensable ancestors.
	for _, s := range def.Steps {
		if s.CompensatingAction == "" {
			continue
		}
		var anc []string
		for _, o := range def.Steps {
			if o.CompensatingAction != "" && isAncestor(o.Name, s.Name) {
				anc = append(anc, o.Name)
			}
		}
		next := maximal(anc)
		if len(next) == 0 {
			addEdge(CompNodeID(s.Name), EndCompensated, EdgeCompensation)
			continue
		}
		for _, n := range next {
			addEdge(CompNodeID(s.Name), CompNodeID(n), EdgeCompensation)
		}
	}

	if !usesFailed {
		g.Nodes = removeNode(g.Nodes, EndFailed)
	}
	return g
}

func removeNode(nodes []GraphNode, id string) []GraphNode {
	out := nodes[:0]
	for _, n := range nodes {
		if n.ID != id {
			out = append(out, n)
		}
	}
	return out
}

// TraceNode is what happened at one node during a run.
type TraceNode struct {
	NodeID      string          `json:"node_id"`
	Seq         int             `json:"seq"`
	Status      string          `json:"status"` // running | completed | failed | compensating
	Attempts    int             `json:"attempts,omitempty"`
	MaxAttempts int             `json:"max_attempts,omitempty"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	EndedAt     *time.Time      `json:"ended_at,omitempty"`
	DurationMS  int64           `json:"duration_ms,omitempty"`
	Error       string          `json:"error,omitempty"`
	Input       json.RawMessage `json:"input,omitempty"`
	Output      json.RawMessage `json:"output,omitempty"`
}

// Trace is a single workflow run overlaid on its flow graph.
type Trace struct {
	WorkflowID string               `json:"workflow_id"`
	RunID      string               `json:"run_id"`
	Flow       string               `json:"flow"`
	Status     Status               `json:"status"`
	Error      string               `json:"error,omitempty"`
	StartedAt  time.Time            `json:"started_at"`
	EndedAt    *time.Time           `json:"ended_at,omitempty"`
	DurationMS int64                `json:"duration_ms"`
	Nodes      map[string]TraceNode `json:"nodes"`
	// Path lists visited node IDs in execution order.
	Path []string `json:"path"`
	// Edges lists IDs of graph edges that were actually taken.
	Edges []string `json:"edges"`
}

// TraceSummary is the list-row form of a Trace.
type TraceSummary struct {
	WorkflowID string     `json:"workflow_id"`
	Flow       string     `json:"flow"`
	Status     Status     `json:"status"`
	StartedAt  time.Time  `json:"started_at"`
	EndedAt    *time.Time `json:"ended_at,omitempty"`
	DurationMS int64      `json:"duration_ms"`
}

// Summary returns the list-row form of the instance.
func (w *WorkflowInstance) Summary() TraceSummary {
	return TraceSummary{
		WorkflowID: w.WorkflowID, Flow: w.DefinitionName, Status: w.Status,
		StartedAt: w.CreatedAt, EndedAt: w.CompletedAt, DurationMS: w.durationMS(),
	}
}

func (w *WorkflowInstance) durationMS() int64 {
	end := w.UpdatedAt
	if w.CompletedAt != nil {
		end = *w.CompletedAt
	}
	return end.Sub(w.CreatedAt).Milliseconds()
}

// lastEvent returns the timestamp of the latest event of a type for a step.
func (w *WorkflowInstance) lastEvent(eventType, step string) *time.Time {
	var ts *time.Time
	for i := range w.History {
		e := &w.History[i]
		if e.Type == eventType && e.StepName == step {
			t := e.Timestamp
			ts = &t
		}
	}
	return ts
}

// BuildTrace overlays a workflow instance on the graph of its definition.
func BuildTrace(w *WorkflowInstance) Trace {
	g := BuildGraph(w.Definition)
	t := Trace{
		WorkflowID: w.WorkflowID, RunID: w.RunID, Flow: w.DefinitionName,
		Status: w.Status, Error: w.Error, StartedAt: w.CreatedAt, EndedAt: w.CompletedAt,
		DurationMS: w.durationMS(), Nodes: map[string]TraceNode{},
		Path: []string{}, Edges: []string{},
	}

	for _, name := range w.StepOrder {
		s := w.Steps[name]
		if s == nil {
			continue
		}
		// Step node.
		if s.Status != StepStatusPending && s.Status != StepStatusSkipped {
			n := TraceNode{
				NodeID: StepNodeID(name), Attempts: s.Attempt, MaxAttempts: s.MaxRetries + 1,
				StartedAt: s.StartedAt, EndedAt: s.CompletedAt, Error: s.Error,
				Input: s.Input, Output: s.Output,
			}
			switch {
			case s.CompletedAt != nil:
				n.Status = "completed"
				n.Error = ""
			case s.Status == StepStatusFailed:
				n.Status = "failed"
				n.EndedAt = w.lastEvent("STEP_FAILED", name)
			default:
				n.Status = "running"
			}
			n.DurationMS = spanMS(n.StartedAt, n.EndedAt)
			t.Nodes[n.NodeID] = n
		}
		// Compensation node.
		if s.CompensatingAction != "" && (s.Status == StepStatusCompensating || s.Status == StepStatusCompensated) {
			started := w.lastEvent("COMPENSATION_STEP_STARTED", name)
			n := TraceNode{NodeID: CompNodeID(name), StartedAt: started, EndedAt: s.CompensatedAt, Input: s.Output}
			if s.Status == StepStatusCompensated {
				n.Status = "completed"
			} else {
				n.Status = "compensating"
			}
			n.DurationMS = spanMS(n.StartedAt, n.EndedAt)
			t.Nodes[n.NodeID] = n
		}
	}

	// Terminal node.
	switch w.Status {
	case StatusCompleted:
		t.Nodes[EndCompleted] = TraceNode{NodeID: EndCompleted, Status: "completed", StartedAt: w.CompletedAt}
	case StatusCompensated:
		t.Nodes[EndCompensated] = TraceNode{NodeID: EndCompensated, Status: "completed", StartedAt: w.CompletedAt}
	case StatusFailed:
		t.Nodes[EndFailed] = TraceNode{NodeID: EndFailed, Status: "failed", StartedAt: w.CompletedAt}
	}

	// Execution order: by start time, terminal last, ID as a stable tiebreak.
	for id := range t.Nodes {
		t.Path = append(t.Path, id)
	}
	rank := func(id string) int {
		switch id {
		case EndCompleted, EndCompensated, EndFailed:
			return 1
		}
		return 0
	}
	sort.Slice(t.Path, func(i, j int) bool {
		a, b := t.Nodes[t.Path[i]], t.Nodes[t.Path[j]]
		if ra, rb := rank(a.NodeID), rank(b.NodeID); ra != rb {
			return ra < rb
		}
		if a.StartedAt != nil && b.StartedAt != nil && !a.StartedAt.Equal(*b.StartedAt) {
			return a.StartedAt.Before(*b.StartedAt)
		}
		return a.NodeID < b.NodeID
	})
	for i, id := range t.Path {
		n := t.Nodes[id]
		n.Seq = i + 1
		t.Nodes[id] = n
	}

	// Edges taken.
	taken := map[string]bool{}
	visited := func(id string) bool { _, ok := t.Nodes[id]; return ok }
	status := func(id string) string { return t.Nodes[id].Status }
	var compEnds []GraphEdge
	for _, e := range g.Edges {
		switch e.Kind {
		case EdgeSuccess:
			if status(e.From) == "completed" && visited(e.To) {
				taken[e.ID] = true
			}
		case EdgeFailure:
			if status(e.From) == "failed" && visited(e.To) {
				taken[e.ID] = true
			}
		case EdgeCompensation:
			if e.To == EndCompensated {
				compEnds = append(compEnds, e)
			} else if visited(e.From) && visited(e.To) {
				taken[e.ID] = true
			}
		}
	}
	for _, e := range compEnds {
		if visited(e.From) && visited(EndCompensated) {
			taken[e.ID] = true
		}
	}
	for _, e := range g.Edges {
		if taken[e.ID] {
			t.Edges = append(t.Edges, e.ID)
		}
	}
	return t
}

func spanMS(from, to *time.Time) int64 {
	if from == nil || to == nil {
		return 0
	}
	return to.Sub(*from).Milliseconds()
}
