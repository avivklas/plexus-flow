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
	// TimerInterval is how often the leader looks for expired sleep,
	// signal-wait, and retry-backoff timers. Defaults to 100ms.
	TimerInterval time.Duration
	// RetentionTTL evicts terminal workflows older than this duration when > 0.
	RetentionTTL time.Duration
	// MaxCompletedWorkflows caps hot terminal workflows in memory when > 0.
	MaxCompletedWorkflows int
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

	// Enable single-commit atomic step advancement on FlowStore so ready steps
	// and compensation steps transition inside the same Raft commit.
	cfg.FlowStore.SetAutoAdvance(true)

	g := graph.New(cfg.GraphName)

	// Attach deduplication store to downstream machine & activity store
	cfg.ActivityStore.SetDedupStore(cfg.DedupStore)
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
		flowstore.CmdFailCompensationStep,
		flowstore.CmdCompleteWorkflow,
		flowstore.CmdCancelWorkflow,
		flowstore.CmdSignalWorkflow,
		flowstore.CmdFireTimer,
		flowstore.CmdFireTimers,
		flowstore.CmdTriggerSchedule,
		flowstore.CmdReplayStep,
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

// transformWorkflowEvent transforms upstream committed workflow state changes into downstream activity commands
// without any secondary Upstream.Apply Raft commits.
func (c *Coordinator) transformWorkflowEvent(_ context.Context, cmd *store.Command, res any) ([]*store.Command, error) {
	switch cmd.Type {
	case flowstore.CmdFireTimers:
		if wfs, ok := res.([]*flow.WorkflowInstance); ok {
			var out []*store.Command
			for _, wf := range wfs {
				out = append(out, c.commandsForWorkflowInstance(wf)...)
			}
			return out, nil
		}
		var ids []string
		if s, ok := res.([]string); ok {
			ids = s
		} else {
			var req flowstore.FireTimersRequest
			if err := cmd.Decode(&req); err == nil {
				seen := make(map[string]bool, len(req.Timers))
				for _, t := range req.Timers {
					if !seen[t.WorkflowID] {
						seen[t.WorkflowID] = true
						ids = append(ids, t.WorkflowID)
					}
				}
			}
		}
		var out []*store.Command
		for _, id := range ids {
			if wf, ok := c.flowStore.GetWorkflow(id); ok && wf != nil {
				out = append(out, c.commandsForWorkflowInstance(wf)...)
			}
		}
		return out, nil

	case flowstore.CmdTriggerSchedule:
		if wf, ok := res.(*flow.WorkflowInstance); ok && wf != nil {
			return c.commandsForWorkflowInstance(wf), nil
		}
		return nil, nil
	}

	if wf, ok := res.(*flow.WorkflowInstance); ok && wf != nil {
		return c.commandsForWorkflowInstance(wf), nil
	}

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
	return c.commandsForWorkflowInstance(wf), nil
}

func (c *Coordinator) commandsForWorkflowInstance(wf *flow.WorkflowInstance) []*store.Command {
	if wf == nil {
		return nil
	}

	var downstreamCmds []*store.Command

	if wf.IsTerminal() {
		if pruneCmd, err := store.NewCommand(worker.CmdPruneWorkflowActivities, worker.PruneWorkflowActivitiesRequest{
			WorkflowID: wf.WorkflowID,
		}); err == nil {
			downstreamCmds = append(downstreamCmds, pruneCmd)
		}
		return downstreamCmds
	}

	switch wf.Status {
	case flow.StatusRunning:
		for _, name := range wf.StepOrder {
			step := wf.Steps[name]
			if step == nil || !step.IsActivity() || step.Status != flow.StepStatusRunning {
				continue
			}
			if wf.Version > 0 && step.DispatchedVersion > 0 && step.DispatchedVersion != wf.Version {
				continue
			}
			task := worker.ActivityTask{
				WorkflowID:   wf.WorkflowID,
				RunID:        wf.RunID,
				StepName:     step.Name,
				Activity:     step.Activity,
				Input:        step.Input,
				Timeout:      step.Timeout,
				Retries:      step.MaxRetries,
				RetryPolicy:  step.RetryPolicy,
				Attempt:      max(step.Attempt-1, 0),
				LastProgress: step.Progress,
				Metadata:     wf.Metadata,
			}
			if len(step.DependsOn) > 0 {
				task.DependencyOutputs = make(map[string]json.RawMessage, len(step.DependsOn))
				for _, dep := range step.DependsOn {
					if d := wf.Steps[dep]; d != nil {
						task.DependencyOutputs[dep] = d.Output
					}
				}
			}
			if dCmd, err := store.NewCommand(worker.CmdDispatchActivity, task); err == nil {
				downstreamCmds = append(downstreamCmds, dCmd)
			}
		}

	case flow.StatusCompensating:
		for _, name := range wf.StepOrder {
			step := wf.Steps[name]
			if step == nil || step.Status != flow.StepStatusCompensating {
				continue
			}
			if wf.Version > 0 && step.DispatchedVersion > 0 && step.DispatchedVersion != wf.Version {
				continue
			}
			compTask := worker.CompensationTask{
				WorkflowID:  wf.WorkflowID,
				RunID:       wf.RunID,
				StepName:    step.Name,
				Activity:    step.CompensatingAction,
				Input:       step.Output,
				Timeout:     step.Timeout,
				Retries:     step.MaxRetries,
				RetryPolicy: step.RetryPolicy,
				Attempt:     max(step.CompensationAttempt-1, 0),
			}
			if len(compTask.Input) == 0 {
				compTask.Input = step.Input
			}
			if dCmd, err := store.NewCommand(worker.CmdCompensateActivity, compTask); err == nil {
				downstreamCmds = append(downstreamCmds, dCmd)
			}
		}
	}

	return downstreamCmds
}

// runTimers resolves expired sleep, signal-wait, and retry-backoff steps in batches,
// triggers due cron schedules, and runs periodic workflow compaction.
func (c *Coordinator) runTimers() {
	defer close(c.timersDone)
	ticker := time.NewTicker(c.cfg.TimerInterval)
	defer ticker.Stop()
	var ticks int
	for {
		select {
		case <-c.stopTimers:
			return
		case <-ticker.C:
		}
		if !c.cfg.Upstream.IsLeader() {
			continue
		}
		ticks++
		now := time.Now().UTC()

		// 1. Batch-fire all due step timers in a single Raft commit.
		if due := c.flowStore.DueTimers(now); len(due) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = c.cfg.Upstream.Apply(ctx, flowstore.CmdFireTimers, flowstore.FireTimersRequest{
				Timers: due,
				At:     now,
			})
			cancel()
		}

		// 2. Trigger any due recurring workflow schedules.
		for _, schedID := range c.flowStore.DueSchedules(now) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = c.cfg.Upstream.Apply(ctx, flowstore.CmdTriggerSchedule, flowstore.TriggerScheduleRequest{
				ScheduleID: schedID,
				At:         now,
			})
			cancel()
		}

		// 3. Periodically compact terminal workflows if retention limits are configured.
		if (c.cfg.RetentionTTL > 0 || c.cfg.MaxCompletedWorkflows > 0) && ticks%10 == 0 {
			var before time.Time
			if c.cfg.RetentionTTL > 0 {
				before = now.Add(-c.cfg.RetentionTTL)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = c.cfg.Upstream.Apply(ctx, flowstore.CmdCompactWorkflows, flowstore.CompactWorkflowsRequest{
				Before:      before,
				MaxRetained: c.cfg.MaxCompletedWorkflows,
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

// ActivityStore returns the downstream activity store.
func (c *Coordinator) ActivityStore() *worker.ActivityStore { return c.activityStore }

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
	if req.At.IsZero() {
		req.At = time.Now().UTC()
	}
	if req.RunID == "" {
		req.RunID = fmt.Sprintf("run-%d", req.At.UnixNano())
	}
	res, err := c.cfg.Upstream.Apply(ctx, flowstore.CmdStartWorkflow, req)
	if err != nil {
		return nil, err
	}
	wf, ok := res.(*flow.WorkflowInstance)
	if !ok {
		if localWf, exists := c.flowStore.GetWorkflow(req.WorkflowID); exists {
			return localWf, nil
		}
		return nil, fmt.Errorf("unexpected start workflow response type: %T", res)
	}
	return wf, nil
}

// ReplayStep resets a step (and its downstream dependents) to PENDING with optional patched input and resumes the workflow.
func (c *Coordinator) ReplayStep(ctx context.Context, req flowstore.ReplayStepRequest) (*flow.WorkflowInstance, error) {
	if req.At.IsZero() {
		req.At = time.Now().UTC()
	}
	// Prune previous activity records for this workflow on downstream so replayed attempt=1 tasks are dispatched cleanly.
	if pruneCmd, err := store.NewCommand(worker.CmdPruneWorkflowActivities, worker.PruneWorkflowActivitiesRequest{
		WorkflowID: req.WorkflowID,
	}); err == nil {
		_, _ = c.cfg.Downstream.Apply(ctx, pruneCmd.Type, worker.PruneWorkflowActivitiesRequest{WorkflowID: req.WorkflowID})
	}
	res, err := c.cfg.Upstream.Apply(ctx, flowstore.CmdReplayStep, req)
	if err != nil {
		return nil, err
	}
	if wf, ok := res.(*flow.WorkflowInstance); ok {
		return wf, nil
	}
	if localWf, exists := c.flowStore.GetWorkflow(req.WorkflowID); exists {
		return localWf, nil
	}
	return nil, fmt.Errorf("unexpected replay step response: %T", res)
}

// UpsertSchedule registers or updates a recurring cron/interval workflow schedule.
func (c *Coordinator) UpsertSchedule(ctx context.Context, sched flow.Schedule) (*flow.Schedule, error) {
	_ = c.RegisterFlow(sched.Definition)
	res, err := c.cfg.Upstream.Apply(ctx, flowstore.CmdUpsertSchedule, flowstore.UpsertScheduleRequest{
		Schedule: sched,
		Now:      time.Now().UTC(),
	})
	if err != nil {
		return nil, err
	}
	if out, ok := res.(*flow.Schedule); ok {
		return out, nil
	}
	if local, ok := c.flowStore.GetSchedule(sched.ScheduleID); ok {
		return local, nil
	}
	return nil, fmt.Errorf("unexpected upsert schedule response: %T", res)
}

// DeleteSchedule removes a recurring schedule by ID.
func (c *Coordinator) DeleteSchedule(ctx context.Context, scheduleID string) error {
	_, err := c.cfg.Upstream.Apply(ctx, flowstore.CmdDeleteSchedule, flowstore.DeleteScheduleRequest{
		ScheduleID: scheduleID,
	})
	return err
}

// GetSchedule retrieves a schedule by ID.
func (c *Coordinator) GetSchedule(scheduleID string) (*flow.Schedule, bool) {
	return c.flowStore.GetSchedule(scheduleID)
}

// ListSchedules returns all registered schedules.
func (c *Coordinator) ListSchedules() []*flow.Schedule {
	return c.flowStore.ListSchedules()
}

// CompactWorkflows evicts terminal workflows matching req from hot memory (archiving them if an Archiver is set).
func (c *Coordinator) CompactWorkflows(ctx context.Context, req flowstore.CompactWorkflowsRequest) (flowstore.CompactWorkflowsResponse, error) {
	res, err := c.cfg.Upstream.Apply(ctx, flowstore.CmdCompactWorkflows, req)
	if err != nil {
		return flowstore.CompactWorkflowsResponse{}, err
	}
	if out, ok := res.(flowstore.CompactWorkflowsResponse); ok {
		return out, nil
	}
	return flowstore.CompactWorkflowsResponse{}, nil
}

// GetWorkflow retrieves a workflow by ID from the local store (or cold archive).
func (c *Coordinator) GetWorkflow(workflowID string) (*flow.WorkflowInstance, bool) {
	return c.flowStore.GetWorkflow(workflowID)
}

// ListWorkflows retrieves all hot workflows from the local store.
func (c *Coordinator) ListWorkflows() []*flow.WorkflowInstance {
	return c.flowStore.ListWorkflows()
}

// SearchWorkflows executes a structured, full-text, and faceted query over local workflow state with zero CDC lag.
func (c *Coordinator) SearchWorkflows(q flowstore.SearchQuery) flowstore.SearchResult {
	return c.flowStore.SearchWorkflows(q)
}

// Watch subscribes to changes of the workflows match accepts; see flowstore.Store.Watch.
func (c *Coordinator) Watch(match func(*flow.WorkflowInstance) bool, buffer int) (<-chan *flow.WorkflowInstance, func()) {
	return c.flowStore.Watch(match, buffer)
}

// WatchSummaries subscribes to summaries of changes; see flowstore.Store.WatchSummaries.
func (c *Coordinator) WatchSummaries(match func(*flow.WorkflowInstance) bool, buffer int) (<-chan flow.TraceSummary, func()) {
	return c.flowStore.WatchSummaries(match, buffer)
}

// SignalWorkflow sends an external signal to a workflow.
func (c *Coordinator) SignalWorkflow(ctx context.Context, workflowID, signalName string, payload json.RawMessage) error {
	_, err := c.cfg.Upstream.Apply(ctx, flowstore.CmdSignalWorkflow, flowstore.SignalWorkflowRequest{
		WorkflowID: workflowID,
		SignalName: signalName,
		Payload:    payload,
		At:         time.Now().UTC(),
	})
	return err
}

// CancelWorkflow triggers graceful cancellation and compensation.
func (c *Coordinator) CancelWorkflow(ctx context.Context, workflowID, reason string) error {
	_, err := c.cfg.Upstream.Apply(ctx, flowstore.CmdCancelWorkflow, flowstore.CancelWorkflowRequest{
		WorkflowID: workflowID,
		Reason:     reason,
		At:         time.Now().UTC(),
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
