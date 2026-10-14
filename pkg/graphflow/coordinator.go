package graphflow

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/worker"
	"github.com/avivklas/plexus/pkg/dedup"
	"github.com/avivklas/plexus/pkg/graph"
	"github.com/avivklas/plexus/pkg/machine"
	"github.com/avivklas/plexus/pkg/store"
	"github.com/hashicorp/raft"
)

// Config configures the Raft Graph workflow coordinator.
type Config struct {
	GraphName     string
	Upstream      machine.Machine
	Downstream    machine.Machine
	FlowStore     *flowstore.Store
	ActivityStore *worker.ActivityStore
	DedupStore    dedup.Store
	Registry      *worker.Registry
	// TimerInterval is how often the leader looks for expired sleep and
	// signal-wait timers. Defaults to 100ms.
	TimerInterval time.Duration
}

// Coordinator wires the Raft Graph command topology between workflow states and activity execution.
type Coordinator struct {
	mu            sync.RWMutex
	cfg           Config
	graph         *graph.Graph
	flowStore     *flowstore.Store
	activityStore *worker.ActivityStore
	dedupStore    dedup.Store
	executor      *worker.Executor
	dispatcher    worker.Dispatcher
	registry      *worker.Registry
	edge          *graph.Edge
	ownsMachines  bool
	cleanupFns    []func()
	stopTimers    chan struct{}
	timersDone    chan struct{}

	flowsMu sync.RWMutex
	flows   map[string]flow.WorkflowDefinition
}

// NewCoordinator creates a new Raft Graph coordinator with the provided machines and stores.
func NewCoordinator(cfg Config) (*Coordinator, error) {
	if cfg.GraphName == "" {
		cfg.GraphName = "plexus-flow-topology"
	}
	if cfg.Upstream == nil {
		return nil, fmt.Errorf("upstream workflow machine is required")
	}
	if cfg.Downstream == nil {
		return nil, fmt.Errorf("downstream activity machine is required")
	}
	if cfg.FlowStore == nil {
		return nil, fmt.Errorf("flow store is required")
	}
	if cfg.ActivityStore == nil {
		return nil, fmt.Errorf("activity store is required")
	}
	if cfg.DedupStore == nil {
		cfg.DedupStore = dedup.NewMemoryStore()
	}
	if cfg.Registry == nil {
		cfg.Registry = worker.NewRegistry()
	}

	g := graph.New(cfg.GraphName)

	// Attach deduplication store to downstream machine & activity store
	g.AddMachine(cfg.Upstream, nil)
	g.AddMachine(cfg.Downstream, cfg.DedupStore)
	g.EnableDedupOnStore(cfg.Downstream, cfg.ActivityStore)

	// Create activity executor that applies results back to upstream workflow machine
	executor := worker.NewExecutor(cfg.Registry, cfg.Upstream)

	// Hook activity store callbacks into executor
	c := &Coordinator{
		cfg:           cfg,
		graph:         g,
		flowStore:     cfg.FlowStore,
		activityStore: cfg.ActivityStore,
		dedupStore:    cfg.DedupStore,
		executor:      executor,
		registry:      cfg.Registry,
		flows:         make(map[string]flow.WorkflowDefinition),
		dispatcher:    executor,
	}
	cfg.ActivityStore.SetListeners(
		func(task worker.ActivityTask) { c.currentDispatcher().DispatchActivity(task) },
		func(task worker.CompensationTask) { c.currentDispatcher().DispatchCompensation(task) },
	)

	// Connect Upstream -> Downstream via Raft Graph Pipe
	edge := g.Pipe(cfg.Upstream, cfg.Downstream, func(ctx context.Context, cmd *store.Command, res any) ([]*store.Command, error) {
		return c.transformWorkflowEvent(ctx, cmd, res)
	},
		flowstore.CmdStartWorkflow,
		flowstore.CmdCompleteStep,
		flowstore.CmdFailStep,
		flowstore.CmdTriggerCompensation,
		flowstore.CmdCompleteCompensationStep,
		flowstore.CmdCancelWorkflow,
		flowstore.CmdSignalWorkflow,
		flowstore.CmdFireTimer,
	)
	c.edge = edge

	if cfg.TimerInterval <= 0 {
		cfg.TimerInterval = 100 * time.Millisecond
		c.cfg.TimerInterval = cfg.TimerInterval
	}
	c.stopTimers = make(chan struct{})
	c.timersDone = make(chan struct{})
	go c.runTimers()

	return c, nil
}

// transformWorkflowEvent transforms upstream committed workflow state changes into downstream activity commands.
func (c *Coordinator) transformWorkflowEvent(ctx context.Context, cmd *store.Command, res any) ([]*store.Command, error) {
	// Extract workflow ID from the command payload
	var baseReq struct {
		WorkflowID string `json:"workflow_id"`
	}
	if err := cmd.Decode(&baseReq); err != nil || baseReq.WorkflowID == "" {
		return nil, nil
	}

	wf, exists := c.flowStore.GetWorkflow(baseReq.WorkflowID)
	if !exists || wf == nil {
		return nil, nil
	}

	var downstreamCmds []*store.Command

	switch wf.Status {
	case flow.StatusRunning:
		// Resolving a step can ready the next one without any further command
		// (a buffered signal completes its wait on dispatch), so go again
		// until dispatching changes nothing but running state.
		for wf.Status == flow.StatusRunning {
			resolvedInline := false
			for _, step := range wf.NextReadySteps() {
				// Mark step running on upstream state machine so subsequent concurrent commits do not re-dispatch it
				_, _ = c.cfg.Upstream.Apply(ctx, flowstore.CmdDispatchStep, flowstore.DispatchStepRequest{
					WorkflowID: wf.WorkflowID,
					RunID:      wf.RunID,
					StepName:   step.Name,
					At:         time.Now().UTC(),
				})

				if !step.IsActivity() {
					if _, status, ok := c.flowStore.StepState(wf.WorkflowID, step.Name); ok && status == flow.StepStatusCompleted {
						resolvedInline = true
					}
					continue
				}

				task := worker.ActivityTask{
					WorkflowID: wf.WorkflowID,
					RunID:      wf.RunID,
					StepName:   step.Name,
					Activity:   step.Activity,
					Input:      step.Input,
					Timeout:    step.Timeout,
					Retries:    step.MaxRetries,
					Attempt:    step.Attempt,
					Metadata:   wf.Metadata,
				}
				if len(step.DependsOn) > 0 {
					task.DependencyOutputs = make(map[string]json.RawMessage, len(step.DependsOn))
					for _, dep := range step.DependsOn {
						if d := wf.Steps[dep]; d != nil {
							task.DependencyOutputs[dep] = d.Output
						}
					}
				}
				dCmd, err := store.NewCommand(worker.CmdDispatchActivity, task)
				if err != nil {
					continue
				}
				downstreamCmds = append(downstreamCmds, dCmd)
			}
			if !resolvedInline {
				break
			}
			var ok bool
			if wf, ok = c.flowStore.GetWorkflow(wf.WorkflowID); !ok {
				break
			}
		}

	case flow.StatusCompensating:
		// Find next compensating step (reverse order of completion)
		nextComp := wf.NextCompensatingStep()
		if nextComp != nil {
			// Mark step compensating on upstream so subsequent concurrent commits do not re-dispatch it
			_, _ = c.cfg.Upstream.Apply(ctx, flowstore.CmdStartCompensationStep, flowstore.StartCompensationStepRequest{
				WorkflowID: wf.WorkflowID,
				RunID:      wf.RunID,
				StepName:   nextComp.Name,
			})

			compTask := worker.CompensationTask{
				WorkflowID: wf.WorkflowID,
				RunID:      wf.RunID,
				StepName:   nextComp.Name,
				Activity:   nextComp.CompensatingAction,
				Input:      nextComp.Output,
				Timeout:    nextComp.Timeout,
				Retries:    nextComp.MaxRetries,
				Attempt:    nextComp.Attempt,
			}
			if len(compTask.Input) == 0 {
				compTask.Input = nextComp.Input
			}
			dCmd, err := store.NewCommand(worker.CmdCompensateActivity, compTask)
			if err == nil {
				downstreamCmds = append(downstreamCmds, dCmd)
			}
		}
	}

	return downstreamCmds, nil
}

// runTimers resolves expired sleep and signal-wait steps. Only the leader
// fires timers; each firing is idempotent, so overlapping leaders are harmless.
func (c *Coordinator) runTimers() {
	defer close(c.timersDone)
	ticker := time.NewTicker(c.cfg.TimerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopTimers:
			return
		case <-ticker.C:
		}
		if !c.cfg.Upstream.IsLeader() {
			continue
		}
		now := time.Now().UTC()
		for _, due := range c.flowStore.DueTimers(now) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = c.cfg.Upstream.Apply(ctx, flowstore.CmdFireTimer, flowstore.FireTimerRequest{
				WorkflowID: due.WorkflowID, RunID: due.RunID, StepName: due.StepName, At: now,
			})
			cancel()
		}
	}
}

// Executor returns the in-process executor that runs activities from the local registry.
func (c *Coordinator) Executor() *worker.Executor { return c.executor }

// SetDispatcher replaces where committed tasks go. By default they run in the
// local executor; a remote broker takes over to hand them to connected workers
// (and may fall back to the executor for activities it has no worker for).
func (c *Coordinator) SetDispatcher(d worker.Dispatcher) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d == nil {
		d = c.executor
	}
	c.dispatcher = d
}

func (c *Coordinator) currentDispatcher() worker.Dispatcher {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.dispatcher
}

// Upstream returns the workflow machine; results from workers are applied to it.
func (c *Coordinator) Upstream() machine.Machine { return c.cfg.Upstream }

// RegisterFlow declares a flow so it is visible through the flows API even
// before any run has started. Starting a workflow registers its flow as well.
func (c *Coordinator) RegisterFlow(def flow.WorkflowDefinition) error {
	if err := def.Validate(); err != nil {
		return fmt.Errorf("invalid flow definition: %w", err)
	}
	c.flowsMu.Lock()
	c.flows[def.Name] = def.Normalized()
	c.flowsMu.Unlock()
	return nil
}

// Flows returns all known flow definitions sorted by name.
func (c *Coordinator) Flows() []flow.WorkflowDefinition {
	c.flowsMu.RLock()
	defer c.flowsMu.RUnlock()
	out := make([]flow.WorkflowDefinition, 0, len(c.flows))
	for _, d := range c.flows {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Flow returns a known flow definition by name.
func (c *Coordinator) Flow(name string) (flow.WorkflowDefinition, bool) {
	c.flowsMu.RLock()
	defer c.flowsMu.RUnlock()
	d, ok := c.flows[name]
	return d, ok
}

// StartWorkflow initiates a workflow via upstream Raft machine consensus.
func (c *Coordinator) StartWorkflow(ctx context.Context, req flowstore.StartWorkflowRequest) (*flow.WorkflowInstance, error) {
	_ = c.RegisterFlow(req.Definition) // invalid definitions are rejected by the store below
	res, err := c.cfg.Upstream.Apply(ctx, flowstore.CmdStartWorkflow, req)
	if err != nil {
		return nil, err
	}
	wf, ok := res.(*flow.WorkflowInstance)
	if !ok {
		// Fallback to local store
		if localWf, exists := c.flowStore.GetWorkflow(req.WorkflowID); exists {
			return localWf, nil
		}
		return nil, fmt.Errorf("unexpected start workflow response type: %T", res)
	}
	return wf, nil
}

// GetWorkflow retrieves a workflow by ID from the local store.
func (c *Coordinator) GetWorkflow(workflowID string) (*flow.WorkflowInstance, bool) {
	return c.flowStore.GetWorkflow(workflowID)
}

// ListWorkflows retrieves all workflows from the local store.
func (c *Coordinator) ListWorkflows() []*flow.WorkflowInstance {
	return c.flowStore.ListWorkflows()
}

// SignalWorkflow sends an external signal to a workflow.
func (c *Coordinator) SignalWorkflow(ctx context.Context, workflowID, signalName string, payload json.RawMessage) error {
	_, err := c.cfg.Upstream.Apply(ctx, flowstore.CmdSignalWorkflow, flowstore.SignalWorkflowRequest{
		WorkflowID: workflowID,
		SignalName: signalName,
		Payload:    payload,
	})
	return err
}

// CancelWorkflow triggers graceful cancellation and compensation.
func (c *Coordinator) CancelWorkflow(ctx context.Context, workflowID, reason string) error {
	_, err := c.cfg.Upstream.Apply(ctx, flowstore.CmdCancelWorkflow, flowstore.CancelWorkflowRequest{
		WorkflowID: workflowID,
		Reason:     reason,
	})
	return err
}

// FlowStore returns the underlying flow store.
func (c *Coordinator) FlowStore() *flowstore.Store {
	return c.flowStore
}

// DedupStore returns the deduplication store.
func (c *Coordinator) DedupStore() dedup.Store {
	return c.dedupStore
}

// Registry returns the activity registry.
func (c *Coordinator) Registry() *worker.Registry {
	return c.registry
}

// Graph returns the Raft Graph instance.
func (c *Coordinator) Graph() *graph.Graph {
	return c.graph
}

// Close gracefully stops the coordinator, graph, executor, and any owned machines.
func (c *Coordinator) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	select {
	case <-c.stopTimers:
	default:
		close(c.stopTimers)
	}
	<-c.timersDone
	c.executor.Close()
	c.graph.Close()

	for _, cleanup := range c.cleanupFns {
		cleanup()
	}

	return nil
}

// NewEmbeddedCoordinator boots an embedded, self-contained Raft cluster coordinator
// with in-memory Raft machines, ideal for unit testing, integration tests, and single-node applications.
func NewEmbeddedCoordinator(reg *worker.Registry) (*Coordinator, error) {
	if reg == nil {
		reg = worker.NewRegistry()
	}

	flowSt := flowstore.New()
	actSt := worker.NewActivityStore()
	dedupSt := dedup.NewMemoryStore()

	// 1. Create Upstream Workflow RaftMachine
	upID := "workflow-raft-machine"
	upTmpDir, err := os.MkdirTemp("", "plexus-flow-up-*")
	if err != nil {
		return nil, fmt.Errorf("create up temp dir: %w", err)
	}

	_, upTrans := raft.NewInmemTransport(raft.ServerAddress(upID))
	upCfg := machine.DefaultConfig(machine.MachineID(upID), &machine.Node{ID: upID, Address: upID, Voter: true}, upTmpDir)
	upCfg.Bootstrap = true
	upCfg.Transport = upTrans
	upCfg.SnapshotStore = raft.NewInmemSnapshotStore()
	upCfg.LogStore = raft.NewInmemStore()
	upCfg.StableStore = raft.NewInmemStore()
	upCfg.ApplyTimeout = 5 * time.Second

	upMachine := machine.NewRaftMachine(upCfg)
	upMachine.Register(flowSt)

	// 2. Create Downstream Activity RaftMachine
	downID := "activity-raft-machine"
	downTmpDir, err := os.MkdirTemp("", "plexus-flow-down-*")
	if err != nil {
		_ = os.RemoveAll(upTmpDir)
		return nil, fmt.Errorf("create down temp dir: %w", err)
	}

	_, downTrans := raft.NewInmemTransport(raft.ServerAddress(downID))
	downCfg := machine.DefaultConfig(machine.MachineID(downID), &machine.Node{ID: downID, Address: downID, Voter: true}, downTmpDir)
	downCfg.Bootstrap = true
	downCfg.Transport = downTrans
	downCfg.SnapshotStore = raft.NewInmemSnapshotStore()
	downCfg.LogStore = raft.NewInmemStore()
	downCfg.StableStore = raft.NewInmemStore()
	downCfg.ApplyTimeout = 5 * time.Second

	downMachine := machine.NewRaftMachine(downCfg)
	downMachine.Register(actSt)

	// Start both machines
	ctx := context.Background()
	if err := upMachine.Start(ctx); err != nil {
		return nil, fmt.Errorf("start up machine: %w", err)
	}
	if err := downMachine.Start(ctx); err != nil {
		_ = upMachine.Stop()
		return nil, fmt.Errorf("start down machine: %w", err)
	}

	// Wait for leadership on both machines
	deadline := time.Now().Add(5 * time.Second)
	for !upMachine.IsLeader() || !downMachine.IsLeader() {
		if time.Now().After(deadline) {
			_ = upMachine.Stop()
			_ = downMachine.Stop()
			return nil, fmt.Errorf("timed out waiting for embedded raft leadership")
		}
		time.Sleep(20 * time.Millisecond)
	}

	coord, err := NewCoordinator(Config{
		GraphName:     "embedded-flow-topology",
		Upstream:      upMachine,
		Downstream:    downMachine,
		FlowStore:     flowSt,
		ActivityStore: actSt,
		DedupStore:    dedupSt,
		Registry:      reg,
	})
	if err != nil {
		_ = upMachine.Stop()
		_ = downMachine.Stop()
		return nil, err
	}

	coord.ownsMachines = true
	coord.cleanupFns = append(coord.cleanupFns, func() {
		_ = upMachine.Stop()
		_ = downMachine.Stop()
		_ = os.RemoveAll(upTmpDir)
		_ = os.RemoveAll(downTmpDir)
	})

	return coord, nil
}
