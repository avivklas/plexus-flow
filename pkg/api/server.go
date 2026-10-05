package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/avivklas/plexus-flow/pkg/blueprint"
	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/graphflow"
)

// Server provides the REST HTTP API and Web Visualizer for Plexus-Flow.
type Server struct {
	addr              string
	coord             *graphflow.Coordinator
	nodeID            string
	blueprintRegistry *blueprint.Registry
	httpServer        *http.Server
	listener          net.Listener
}

// NewServer creates a new HTTP API and Web Visualizer Server.
func NewServer(addr string, coord *graphflow.Coordinator, nodeID string) *Server {
	s := &Server{
		addr:              addr,
		coord:             coord,
		nodeID:            nodeID,
		blueprintRegistry: blueprint.NewRegistry(),
	}

	mux := http.NewServeMux()

	// Web UI Visualizer routes
	mux.HandleFunc("/", s.handleRoot)
	mux.HandleFunc("/ui", s.handleUI)

	// API routes
	mux.HandleFunc("/api/v1/blueprints", s.handleBlueprints)
	mux.HandleFunc("/api/v1/blueprints/", s.handleBlueprintByID)
	mux.HandleFunc("/api/v1/workflows/start", s.handleStartWorkflow)
	mux.HandleFunc("/api/v1/workflows", s.handleWorkflows)
	mux.HandleFunc("/api/v1/workflows/", s.handleWorkflowByID)
	mux.HandleFunc("/api/v1/cluster/status", s.handleClusterStatus)

	// Wrap mux with CORS middleware
	corsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		mux.ServeHTTP(w, r)
	})

	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: corsHandler,
	}

	return s
}

// SetBlueprintRegistry configures a custom blueprint registry.
func (s *Server) SetBlueprintRegistry(reg *blueprint.Registry) {
	if reg != nil {
		s.blueprintRegistry = reg
	}
}

// BlueprintRegistry returns the active blueprint registry.
func (s *Server) BlueprintRegistry() *blueprint.Registry {
	return s.blueprintRegistry
}

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

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	s.handleUI(w, r)
}

func (s *Server) handleUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(UIHTML))
}

func (s *Server) handleBlueprints(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		bps := s.blueprintRegistry.List()
		writeJSON(w, http.StatusOK, bps)
		return
	}

	if r.Method == http.MethodPost {
		var bp blueprint.Blueprint
		if err := json.NewDecoder(r.Body).Decode(&bp); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid blueprint: "+err.Error())
			return
		}
		if err := s.blueprintRegistry.Register(bp); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, bp)
		return
	}

	writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
}

func (s *Server) handleBlueprintByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/blueprints/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		writeJSONError(w, http.StatusBadRequest, "blueprint ID required")
		return
	}

	bpID := parts[0]
	bp, ok := s.blueprintRegistry.Get(bpID)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "blueprint not found")
		return
	}

	// Sub-resource: /run
	if len(parts) > 1 && parts[1] == "run" {
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		var runReq struct {
			WorkflowID string          `json:"workflow_id"`
			Input      json.RawMessage `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&runReq)

		wfID := runReq.WorkflowID
		if wfID == "" {
			wfID = fmt.Sprintf("%s-%d", bpID, time.Now().UnixNano()%100000)
		}
		input := runReq.Input
		if len(input) == 0 {
			input = bp.DefaultInput
		}

		wf, err := s.coord.StartWorkflow(r.Context(), flowstore.StartWorkflowRequest{
			WorkflowID: wfID,
			Definition: bp.Definition,
			Input:      input,
		})
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}

		writeJSON(w, http.StatusCreated, wf)
		return
	}

	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	writeJSON(w, http.StatusOK, bp)
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
		if errors.Is(err, flow.ErrWorkflowAlreadyExist) {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, wf)
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

	// Handle sub-resources: /signal, /cancel, /history
	if len(parts) > 1 {
		sub := parts[1]
		switch sub {
		case "signal":
			if r.Method != http.MethodPost {
				writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			var sigReq SignalRequest
			if err := json.NewDecoder(r.Body).Decode(&sigReq); err != nil {
				writeJSONError(w, http.StatusBadRequest, "invalid signal payload: "+err.Error())
				return
			}
			rawPayload, _ := json.Marshal(sigReq.Payload)
			if err := s.coord.SignalWorkflow(r.Context(), wfID, sigReq.SignalName, rawPayload); err != nil {
				writeJSONError(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, map[string]string{"status": "signaled"})
			return

		case "cancel":
			if r.Method != http.MethodPost {
				writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
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

	allWfs := s.coord.ListWorkflows()
	resp := ClusterStatusResponse{
		NodeID:      s.nodeID,
		IsLeader:    true,
		LeaderID:    s.nodeID,
		LeaderAddr:  s.Addr(),
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
