package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
)

// Watch endpoints stream workflow changes as server-sent events
// (text/event-stream), so clients and dashboards don't have to poll.
//
//	GET /api/v1/workflows/{id}/watch           one workflow: its current state, then every change;
//	                                           the stream ends when the workflow reaches a terminal status
//	GET /api/v1/flows/{name}/watch[?terminal=1] summaries of changes to every run of a flow
//	                                           (only runs reaching a terminal status with terminal=1)
//
// Each event carries the workflow version as its id. A stream that falls too
// far behind is closed by the server; reconnect and read the current state.

const pingEvery = 15 * time.Second

func sseStart(w http.ResponseWriter) (http.Flusher, bool) {
	f, ok := w.(http.Flusher)
	if !ok {
		writeJSONError(w, http.StatusInternalServerError, "streaming unsupported")
		return nil, false
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	f.Flush()
	return f, true
}

func sseSend(w http.ResponseWriter, f http.Flusher, event string, version uint64, data any) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", version, event, b); err != nil {
		return err
	}
	f.Flush()
	return nil
}

func ssePing(w http.ResponseWriter, f http.Flusher) error {
	if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
		return err
	}
	f.Flush()
	return nil
}

// GET /api/v1/workflows/{id}/watch
func (s *Server) handleWatchWorkflow(w http.ResponseWriter, r *http.Request, id string) {
	// Subscribe before reading the current state so no change can fall in between.
	ch, cancel := s.coord.Watch(func(wf *flow.WorkflowInstance) bool { return wf.WorkflowID == id }, 256)
	defer cancel()

	cur, ok := s.coord.GetWorkflow(id)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "workflow not found")
		return
	}
	f, ok := sseStart(w)
	if !ok {
		return
	}
	if sseSend(w, f, "workflow", cur.Version, cur) != nil || cur.IsTerminal() {
		return
	}
	last := cur.Version

	ping := time.NewTicker(pingEvery)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ping.C:
			if ssePing(w, f) != nil {
				return
			}
		case wf, open := <-ch:
			if !open {
				return // fell behind; the client reconnects and gets the current state
			}
			if wf.Version <= last {
				continue // already covered by the initial state
			}
			last = wf.Version
			if sseSend(w, f, "workflow", wf.Version, wf) != nil || wf.IsTerminal() {
				return
			}
		}
	}
}

// GET /api/v1/flows/{name}/watch
func (s *Server) handleWatchFlow(w http.ResponseWriter, r *http.Request, name string) {
	terminalOnly := r.URL.Query().Get("terminal") == "1"
	ch, cancel := s.coord.WatchSummaries(func(wf *flow.WorkflowInstance) bool {
		return wf.DefinitionName == name && (!terminalOnly || wf.IsTerminal())
	}, 4096)
	defer cancel()

	f, ok := sseStart(w)
	if !ok {
		return
	}
	ping := time.NewTicker(pingEvery)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ping.C:
			if ssePing(w, f) != nil {
				return
			}
		case sum, open := <-ch:
			if !open {
				return
			}
			if sseSend(w, f, "trace", sum.Version, sum) != nil {
				return
			}
		}
	}
}
