package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/graphflow"
)

const forwardedHeader = "X-Plexus-Forwarded"

// Server provides the REST HTTP API for Plexus-Flow.
type Server struct {
	addr       string
	coord      *graphflow.Coordinator
	nodeID     string
	httpServer *http.Server
	listener   net.Listener
	mux        *http.ServeMux

	leaderMu       sync.RWMutex
	isLeaderFn     func() bool
	leaderHTTPFunc func() string
	forwardClient  *http.Client
}

// NewServer creates a new HTTP API Server.
func NewServer(addr string, coord *graphflow.Coordinator, nodeID string) *Server {
	s := &Server{
		addr:          addr,
		coord:         coord,
		nodeID:        nodeID,
		forwardClient: &http.Client{Timeout: 15 * time.Second},
	}

	mux := http.NewServeMux()
	s.mux = mux
	mux.HandleFunc("/api/v1/workflows/start", s.withLeaderForward(s.handleStartWorkflow))
	mux.HandleFunc("/api/v1/workflows/compact", s.withLeaderForward(s.handleCompactWorkflows))
	mux.HandleFunc("/api/v1/workflows", s.handleWorkflows)
	mux.HandleFunc("/api/v1/workflows/", s.handleWorkflowByID)
	mux.HandleFunc("/api/v1/schedules", s.handleSchedules)
	mux.HandleFunc("/api/v1/schedules/", s.handleScheduleByID)
	mux.HandleFunc("/api/v1/cluster/status", s.handleClusterStatus)
	mux.HandleFunc("/api/v1/flows", s.handleFlows)
	mux.HandleFunc("/api/v1/flows/", s.handleFlowByName)
	mux.HandleFunc("/api/v1/traces/", s.handleTrace)
	mux.HandleFunc("/", s.handleDashboard)

	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return s
}

// SetLeaderResolver configures custom leadership check and leader HTTP address lookup
// for transparent follower-to-leader request forwarding.
func (s *Server) SetLeaderResolver(isLeader func() bool, leaderHTTPAddr func() string) {
	s.leaderMu.Lock()
	defer s.leaderMu.Unlock()
	s.isLeaderFn = isLeader
	s.leaderHTTPFunc = leaderHTTPAddr
}

func (s *Server) isLeader() bool {
	s.leaderMu.RLock()
	fn := s.isLeaderFn
	s.leaderMu.RUnlock()
	if fn != nil {
		return fn()
	}
	if s.coord != nil && s.coord.Upstream() != nil {
		return s.coord.Upstream().IsLeader()
	}
	return true
}

func (s *Server) leaderHTTPAddr() string {
	s.leaderMu.RLock()
	fn := s.leaderHTTPFunc
	s.leaderMu.RUnlock()
	if fn != nil {
		return fn()
	}
	if s.isLeader() {
		return s.Addr()
	}
	return ""
}

// withLeaderForward proxies mutating requests arriving on a follower node to the current leader's HTTP API.
func (s *Server) withLeaderForward(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.isLeader() && r.Header.Get(forwardedHeader) == "" {
			if target := s.leaderHTTPAddr(); target != "" && target != s.Addr() {
				s.proxyToLeader(w, r, target)
				return
			}
		}
		next(w, r)
	}
}

func (s *Server) proxyToLeader(w http.ResponseWriter, r *http.Request, target string) {
	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		target = "http://" + target
	}
	targetURL := strings.TrimRight(target, "/") + r.URL.RequestURI()
	req, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, r.Body)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "build leader proxy request: "+err.Error())
		return
	}
	for k, vals := range r.Header {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set(forwardedHeader, s.nodeID)

	resp, err := s.forwardClient.Do(req)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "forward to leader failed: "+err.Error())
		return
	}
	defer resp.Body.Close()

	for k, vals := range resp.Header {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set("X-Plexus-Routed-Leader", target)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// Handle mounts an extra handler, for applications embedding the server. It
// must be called before Start; patterns follow http.ServeMux rules and may not
// repeat the built-in routes.
func (s *Server) Handle(pattern string, h http.Handler) { s.mux.Handle(pattern, h) }

// Start begins listening on the configured HTTP address.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = ln
	go func() {
		_ = s.httpServer.Serve(ln)
	}()
	return nil
}

// Addr returns the bound address of the server.
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

// Close gracefully stops the server.
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleStartWorkflow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req flowstore.StartWorkflowRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	wf, err := s.coord.StartWorkflow(r.Context(), req)
	if err != nil {
		if errors.Is(err, flow.ErrWorkflowAlreadyExist) || strings.Contains(err.Error(), flow.ErrWorkflowAlreadyExist.Error()) {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, wf)
}

func (s *Server) handleCompactWorkflows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req flowstore.CompactWorkflowsRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	resp, err := s.coord.CompactWorkflows(r.Context(), req)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleSchedules(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.coord.ListSchedules())
	case http.MethodPost:
		s.withLeaderForward(func(w http.ResponseWriter, r *http.Request) {
			var sched flow.Schedule
			if err := json.NewDecoder(r.Body).Decode(&sched); err != nil {
				writeJSONError(w, http.StatusBadRequest, "invalid schedule body: "+err.Error())
				return
			}
			out, err := s.coord.UpsertSchedule(r.Context(), sched)
			if err != nil {
				writeJSONError(w, http.StatusBadRequest, err.Error())
				return
			}
			writeJSON(w, http.StatusCreated, out)
		})(w, r)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleScheduleByID(w http.ResponseWriter, r *http.Request) {
	schedID := strings.TrimPrefix(r.URL.Path, "/api/v1/schedules/")
	if schedID == "" {
		writeJSONError(w, http.StatusBadRequest, "schedule ID required")
		return
	}
	switch r.Method {
	case http.MethodGet:
		sched, ok := s.coord.GetSchedule(schedID)
		if !ok {
			writeJSONError(w, http.StatusNotFound, "schedule not found")
			return
		}
		writeJSON(w, http.StatusOK, sched)
	case http.MethodDelete:
		s.withLeaderForward(func(w http.ResponseWriter, r *http.Request) {
			if err := s.coord.DeleteSchedule(r.Context(), schedID); err != nil {
				if errors.Is(err, flow.ErrScheduleNotFound) || strings.Contains(err.Error(), flow.ErrScheduleNotFound.Error()) {
					writeJSONError(w, http.StatusNotFound, err.Error())
					return
				}
				writeJSONError(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
		})(w, r)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleWorkflows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	workflows := s.coord.ListWorkflows()
	writeJSON(w, http.StatusOK, workflows)
}

func (s *Server) handleWorkflowByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/workflows/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		writeJSONError(w, http.StatusBadRequest, "workflow ID required")
		return
	}

	wfID := parts[0]

	// Handle sub-resources: /signal, /cancel, /history, /watch
	if len(parts) > 1 {
		sub := parts[1]
		switch sub {
		case "signal":
			if r.Method != http.MethodPost {
				writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			s.withLeaderForward(func(w http.ResponseWriter, r *http.Request) {
				var sigReq SignalRequest
				if err := json.NewDecoder(r.Body).Decode(&sigReq); err != nil {
					writeJSONError(w, http.StatusBadRequest, "invalid signal payload: "+err.Error())
					return
				}
				rawPayload, _ := json.Marshal(sigReq.Payload)
				if err := s.coord.SignalWorkflow(r.Context(), wfID, sigReq.SignalName, rawPayload); err != nil {
					writeJSONError(w, signalErrorStatus(err), err.Error())
					return
				}
				writeJSON(w, http.StatusOK, map[string]string{"status": "signaled"})
			})(w, r)
			return

		case "cancel":
			if r.Method != http.MethodPost {
				writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			s.withLeaderForward(func(w http.ResponseWriter, r *http.Request) {
				var cancelReq CancelRequest
				_ = json.NewDecoder(r.Body).Decode(&cancelReq)
				reason := cancelReq.Reason
				if reason == "" {
					reason = "user requested cancellation"
				}
				if err := s.coord.CancelWorkflow(r.Context(), wfID, reason); err != nil {
					writeJSONError(w, http.StatusInternalServerError, err.Error())
					return
				}
				writeJSON(w, http.StatusOK, map[string]string{"status": "cancelling"})
			})(w, r)
			return

		case "watch":
			if r.Method != http.MethodGet {
				writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			s.handleWatchWorkflow(w, r, wfID)
			return

		case "history":
			if r.Method != http.MethodGet {
				writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			history, exists := s.coord.FlowStore().GetHistory(wfID)
			if !exists {
				writeJSONError(w, http.StatusNotFound, "workflow not found")
				return
			}
			writeJSON(w, http.StatusOK, history)
			return

		default:
			writeJSONError(w, http.StatusNotFound, "unknown sub-resource")
			return
		}
	}

	// Standard GET /api/v1/workflows/{id}
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	wf, exists := s.coord.GetWorkflow(wfID)
	if !exists {
		writeJSONError(w, http.StatusNotFound, "workflow not found")
		return
	}

	writeJSON(w, http.StatusOK, wf)
}

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	leader := s.isLeader()
	leaderID := ""
	if leader {
		leaderID = s.nodeID
	}
	leaderAddr := s.leaderHTTPAddr()
	if leaderAddr == "" && leader {
		leaderAddr = s.Addr()
	}

	allWfs := s.coord.ListWorkflows()
	resp := ClusterStatusResponse{
		NodeID:      s.nodeID,
		IsLeader:    leader,
		LeaderID:    leaderID,
		LeaderAddr:  leaderAddr,
		StoreCount:  2,
		WorkflowNum: len(allWfs),
		Metadata: map[string]string{
			"graph": "plexus-flow-topology",
		},
	}

	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, ErrorResponse{Error: msg})
}

// signalErrorStatus maps a signal failure to an HTTP status. Errors that came
// back through consensus lose their identity, so the message is matched too.
func signalErrorStatus(err error) int {
	switch {
	case errors.Is(err, flow.ErrWorkflowNotFound) || strings.Contains(err.Error(), flow.ErrWorkflowNotFound.Error()):
		return http.StatusNotFound
	case errors.Is(err, flow.ErrUnknownSignal) || strings.Contains(err.Error(), flow.ErrUnknownSignal.Error()):
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}
