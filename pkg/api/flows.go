package api

import (
	_ "embed"
	"net/http"
	"sort"
	"strings"

	"github.com/avivklas/plexus-flow/pkg/flow"
)

//go:embed dashboard/index.html
var dashboardHTML []byte

// FlowSummary is the list-row form of a flow.
type FlowSummary struct {
	Name        string `json:"name"`
	Steps       int    `json:"steps"`
	Traces      int    `json:"traces"`
	Completed   int    `json:"completed"`
	Compensated int    `json:"compensated"`
	Failed      int    `json:"failed"`
	Running     int    `json:"running"`
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(dashboardHTML)
}

// GET /api/v1/flows
func (s *Server) handleFlows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	byName := map[string]*FlowSummary{}
	for _, d := range s.coord.Flows() {
		byName[d.Name] = &FlowSummary{Name: d.Name, Steps: len(d.Steps)}
	}
	for _, wf := range s.coord.ListWorkflows() {
		fs := byName[wf.DefinitionName]
		if fs == nil {
			fs = &FlowSummary{Name: wf.DefinitionName, Steps: len(wf.Definition.Steps)}
			byName[wf.DefinitionName] = fs
		}
		fs.Traces++
		switch wf.Status {
		case flow.StatusCompleted:
			fs.Completed++
		case flow.StatusCompensated:
			fs.Compensated++
		case flow.StatusFailed:
			fs.Failed++
		case flow.StatusRunning, flow.StatusCompensating:
			fs.Running++
		}
	}
	out := make([]FlowSummary, 0, len(byName))
	for _, fs := range byName {
		out = append(out, *fs)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	writeJSON(w, http.StatusOK, out)
}

// GET /api/v1/flows/{name}          -> flow graph
// GET /api/v1/flows/{name}/traces   -> runs of the flow, newest first (?status=COMPLETED)
func (s *Server) handleFlowByName(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/flows/")
	name, sub, _ := strings.Cut(rest, "/")
	if name == "" {
		writeJSONError(w, http.StatusBadRequest, "flow name required")
		return
	}

	switch sub {
	case "":
		def, ok := s.coord.Flow(name)
		if !ok {
			// Fall back to the definition carried by a run.
			for _, wf := range s.coord.ListWorkflows() {
				if wf.DefinitionName == name {
					def, ok = wf.Definition, true
					break
				}
			}
		}
		if !ok {
			writeJSONError(w, http.StatusNotFound, "flow not found")
			return
		}
		writeJSON(w, http.StatusOK, flow.BuildGraph(def))
	case "traces":
		status := strings.ToUpper(r.URL.Query().Get("status"))
		out := []flow.TraceSummary{}
		for _, wf := range s.coord.ListWorkflows() {
			if wf.DefinitionName != name || (status != "" && string(wf.Status) != status) {
				continue
			}
			out = append(out, wf.Summary())
		}
		sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
		writeJSON(w, http.StatusOK, out)
	default:
		writeJSONError(w, http.StatusNotFound, "unknown sub-resource")
	}
}

// GET /api/v1/traces/{workflowID} -> one run overlaid on its flow graph
func (s *Server) handleTrace(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/traces/")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "workflow ID required")
		return
	}
	wf, ok := s.coord.GetWorkflow(id)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "trace not found")
		return
	}
	writeJSON(w, http.StatusOK, flow.BuildTrace(wf))
}
