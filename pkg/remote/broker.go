// Package remote runs activities outside the plexus-flow process.
//
// The Broker receives the tasks the engine commits, hands them to workers
// connected over the WorkerService gRPC API, and reports their results back to
// the workflow machine. It owns everything that makes remote execution safe:
// capacity limits, liveness (heartbeats), per-step timeouts, redelivery when a
// worker disappears, and cancellation of tasks whose step no longer runs.
package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/remote/workerpb"
	"github.com/avivklas/plexus-flow/pkg/worker"
)

// Failure codes the broker itself reports.
const (
	CodeTimeout     = "TIMEOUT"
	CodeWorkerLost  = "WORKER_LOST"
	defaultTimeout  = 30 * time.Second
	defaultInterval = 2 * time.Second
)

// FlowReader is the read access to workflow state the broker needs.
type FlowReader interface {
	StepState(workflowID, stepName string) (flow.Status, flow.StepStatus, bool)
	ListWorkflows() []*flow.WorkflowInstance
}

// Options configure a Broker.
type Options struct {
	// Applier proposes step results to the workflow machine. Required.
	Applier worker.WorkflowApplier
	// Flows reads workflow state. Required.
	Flows FlowReader
	// IsLeader gates dispatching; nil means always the leader.
	IsLeader func() bool
	// Local runs activities no remote worker serves, if it has them registered.
	Local    *worker.Executor
	Registry *worker.Registry
	// HeartbeatInterval is how often workers must heartbeat (default 2s); a
	// worker silent for three intervals is considered lost.
	HeartbeatInterval time.Duration
	// DefaultTimeout applies to steps without a timeout (default 30s).
	DefaultTimeout time.Duration
	// Tick is the period of the housekeeping loop (default 100ms).
	Tick time.Duration
}

// Broker implements worker.Dispatcher for remote workers.
type Broker struct {
	opt Options

	mu      sync.Mutex
	pending map[string][]*delivery // activity -> FIFO
	byKey   map[string]*delivery   // current delivery of a step
	byID    map[string]*delivery   // assigned deliveries
	workers map[string]*Session
	seq     uint64
	leader  bool
	closed  bool

	stop chan struct{}
	done chan struct{}
	wg   sync.WaitGroup
}

type delivery struct {
	id         string
	key        string
	kind       workerpb.TaskKind
	workflowID string
	runID      string
	step       string
	activity   string
	input      json.RawMessage
	deps       map[string]json.RawMessage
	meta       map[string]string
	attempt    int
	retries    int
	timeout    time.Duration

	tries    int // compensation attempts made so far
	session  *Session
	deadline time.Time
}

// NewBroker creates a broker and starts its housekeeping loop.
func NewBroker(opt Options) (*Broker, error) {
	if opt.Applier == nil || opt.Flows == nil {
		return nil, fmt.Errorf("broker needs an Applier and a FlowReader")
	}
	if opt.HeartbeatInterval <= 0 {
		opt.HeartbeatInterval = defaultInterval
	}
	if opt.DefaultTimeout <= 0 {
		opt.DefaultTimeout = defaultTimeout
	}
	if opt.Tick <= 0 {
		opt.Tick = 100 * time.Millisecond
	}
	b := &Broker{
		opt:     opt,
		pending: map[string][]*delivery{},
		byKey:   map[string]*delivery{},
		byID:    map[string]*delivery{},
		workers: map[string]*Session{},
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	b.leader = b.isLeader()
	b.wg.Add(1)
	go b.loop()
	return b, nil
}

// Close stops the housekeeping loop and disconnects every worker.
func (b *Broker) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	close(b.stop)
	for _, s := range b.workers {
		s.closeLocked()
	}
	b.mu.Unlock()
	b.wg.Wait()
}

func (b *Broker) isLeader() bool { return b.opt.IsLeader == nil || b.opt.IsLeader() }

// DispatchActivity implements worker.Dispatcher.
func (b *Broker) DispatchActivity(t worker.ActivityTask) {
	b.enqueue(&delivery{
		kind: workerpb.TaskKind_TASK_KIND_ACTIVITY, workflowID: t.WorkflowID, runID: t.RunID, step: t.StepName,
		activity: t.Activity, input: t.Input, deps: t.DependencyOutputs, meta: t.Metadata,
		attempt: t.Attempt + 1, retries: t.Retries, timeout: t.Timeout,
	}, func() { b.runLocalActivity(t) })
}

// DispatchCompensation implements worker.Dispatcher.
func (b *Broker) DispatchCompensation(t worker.CompensationTask) {
	b.enqueue(&delivery{
		kind: workerpb.TaskKind_TASK_KIND_COMPENSATION, workflowID: t.WorkflowID, runID: t.RunID, step: t.StepName,
		activity: t.Activity, input: t.Input, attempt: t.Attempt + 1, retries: t.Retries, timeout: t.Timeout,
	}, func() { b.runLocalCompensation(t) })
}

func (b *Broker) runLocalActivity(t worker.ActivityTask) {
	if b.opt.Local != nil {
		b.opt.Local.ExecuteActivity(t)
	}
}

func (b *Broker) runLocalCompensation(t worker.CompensationTask) {
	if b.opt.Local != nil {
		b.opt.Local.ExecuteCompensation(t)
	}
}

func keyOf(kind workerpb.TaskKind, wf, step string) string {
	return fmt.Sprintf("%d|%s|%s", kind, wf, step)
}

func (b *Broker) enqueue(d *delivery, local func()) {
	d.key = keyOf(d.kind, d.workflowID, d.step)
	if d.timeout <= 0 {
		d.timeout = b.opt.DefaultTimeout
	}

	b.mu.Lock()
	if b.closed || !b.isLeader() {
		b.mu.Unlock()
		return
	}
	if _, dup := b.byKey[d.key]; dup {
		b.mu.Unlock()
		return
	}
	if !b.hasWorkerLocked(d.activity) && b.opt.Registry != nil && b.opt.Local != nil {
		if _, ok := b.opt.Registry.Get(d.activity); ok {
			b.mu.Unlock()
			local()
			return
		}
	}
	b.addPendingLocked(d, false)
	b.assignLocked()
	b.mu.Unlock()
}

func (b *Broker) addPendingLocked(d *delivery, front bool) {
	b.seq++
	d.id = fmt.Sprintf("%s#%d", d.key, b.seq)
	d.session = nil
	b.byKey[d.key] = d
	if front {
		b.pending[d.activity] = append([]*delivery{d}, b.pending[d.activity]...)
	} else {
		b.pending[d.activity] = append(b.pending[d.activity], d)
	}
}

func (b *Broker) hasWorkerLocked(activity string) bool {
	for _, s := range b.workers {
		if s.activities[activity] {
			return true
		}
	}
	return false
}

// wanted reports whether the engine still expects a result for d.
func (b *Broker) wanted(d *delivery) bool {
	wfStatus, stepStatus, ok := b.opt.Flows.StepState(d.workflowID, d.step)
	if !ok {
		return false
	}
	switch d.kind {
	case workerpb.TaskKind_TASK_KIND_COMPENSATION:
		return stepStatus == flow.StepStatusCompensating && wfStatus == flow.StatusCompensating
	default:
		return stepStatus == flow.StepStatusRunning && (wfStatus == flow.StatusRunning || wfStatus == flow.StatusCompensating)
	}
}

// assignLocked hands pending tasks to workers with free capacity.
func (b *Broker) assignLocked() {
	for activity, queue := range b.pending {
		for len(queue) > 0 {
			d := queue[0]
			if !b.wanted(d) {
				queue = queue[1:]
				delete(b.byKey, d.key)
				continue
			}
			s := b.pickLocked(activity)
			if s == nil {
				break
			}
			queue = queue[1:]
			if !b.sendTaskLocked(s, d) {
				queue = append([]*delivery{d}, queue...)
				break
			}
		}
		if len(queue) == 0 {
			delete(b.pending, activity)
		} else {
			b.pending[activity] = queue
		}
	}
}

func (b *Broker) pickLocked(activity string) *Session {
	var best *Session
	for _, s := range b.workers {
		if !s.activities[activity] || len(s.inflight) >= s.max {
			continue
		}
		if best == nil || len(s.inflight) < len(best.inflight) {
			best = s
		}
	}
	return best
}

func (b *Broker) sendTaskLocked(s *Session, d *delivery) bool {
	deps := make(map[string][]byte, len(d.deps))
	for k, v := range d.deps {
		deps[k] = v
	}
	msg := &workerpb.ServerMessage{Message: &workerpb.ServerMessage_Task{Task: &workerpb.Task{
		TaskId: d.id, Kind: d.kind, WorkflowId: d.workflowID, RunId: d.runID, StepName: d.step,
		Activity: d.activity, Input: d.input, DependencyOutputs: deps, Metadata: d.meta,
		Attempt: int32(d.attempt), TimeoutMs: d.timeout.Milliseconds(),
	}}}
	if !s.sendLocked(msg) {
		return false
	}
	d.session = s
	d.deadline = time.Now().Add(d.timeout)
	s.inflight[d.id] = d
	b.byID[d.id] = d
	return true
}

// Recover re-enqueues every step the engine considers in progress. Call it
// after the workflow state has been restored (startup, leadership change):
// tasks handed out before a crash were lost with the broker's memory.
func (b *Broker) Recover() {
	var found []*delivery
	for _, wf := range b.opt.Flows.ListWorkflows() {
		if wf.IsTerminal() {
			continue
		}
		for name, st := range wf.Steps {
			switch {
			case wf.Status == flow.StatusRunning && st.Status == flow.StepStatusRunning:
				found = append(found, &delivery{
					kind: workerpb.TaskKind_TASK_KIND_ACTIVITY, workflowID: wf.WorkflowID, runID: wf.RunID, step: name,
					activity: st.Activity, input: st.Input, meta: wf.Metadata, attempt: st.Attempt, retries: st.MaxRetries, timeout: st.Timeout,
					deps: depOutputs(wf, st),
				})
			case wf.Status == flow.StatusCompensating && st.Status == flow.StepStatusCompensating:
				in := st.Output
				if len(in) == 0 {
					in = st.Input
				}
				found = append(found, &delivery{
					kind: workerpb.TaskKind_TASK_KIND_COMPENSATION, workflowID: wf.WorkflowID, runID: wf.RunID, step: name,
					activity: st.CompensatingAction, input: in, attempt: st.Attempt, retries: st.MaxRetries, timeout: st.Timeout,
				})
			}
		}
	}
	for _, d := range found {
		d := d
		b.enqueue(d, func() {
			// No remote worker serves it: fall back to the local executor.
			if d.kind == workerpb.TaskKind_TASK_KIND_ACTIVITY {
				b.runLocalActivity(worker.ActivityTask{WorkflowID: d.workflowID, RunID: d.runID, StepName: d.step,
					Activity: d.activity, Input: d.input, Timeout: d.timeout, Retries: d.retries, Attempt: d.attempt, Metadata: d.meta, DependencyOutputs: d.deps})
			} else {
				b.runLocalCompensation(worker.CompensationTask{WorkflowID: d.workflowID, RunID: d.runID, StepName: d.step,
					Activity: d.activity, Input: d.input, Timeout: d.timeout, Retries: d.retries, Attempt: d.attempt})
			}
		})
	}
}

func depOutputs(wf *flow.WorkflowInstance, st *flow.StepExecution) map[string]json.RawMessage {
	if len(st.DependsOn) == 0 {
		return nil
	}
	out := make(map[string]json.RawMessage, len(st.DependsOn))
	for _, d := range st.DependsOn {
		if ds := wf.Steps[d]; ds != nil {
			out[d] = ds.Output
		}
	}
	return out
}

// loop is the housekeeping: leadership changes, worker liveness, step
// timeouts and cancellation of tasks that are no longer wanted.
func (b *Broker) loop() {
	defer b.wg.Done()
	t := time.NewTicker(b.opt.Tick)
	defer t.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-t.C:
			b.housekeep()
		}
	}
}

func (b *Broker) housekeep() {
	leader := b.isLeader()
	b.mu.Lock()
	was := b.leader
	b.leader = leader
	if was && !leader {
		b.dropAllLocked("leadership lost")
		b.mu.Unlock()
		return
	}
	now := time.Now()
	var reports []func()

	lease := 3 * b.opt.HeartbeatInterval
	for id, s := range b.workers {
		if now.Sub(s.lastSeen) > lease {
			log.Printf("[Broker] worker %s missed heartbeats, requeueing %d task(s)", id, len(s.inflight))
			b.loseSessionLocked(s)
		}
	}
	for id, d := range b.byID {
		switch {
		case !b.wanted(d):
			d.session.sendCancelLocked(id, "step no longer running")
			b.releaseLocked(d)
		case now.After(d.deadline):
			d.session.sendCancelLocked(id, "step timed out")
			b.releaseLocked(d)
			reports = append(reports, b.failReport(d, CodeTimeout, fmt.Sprintf("step %s timed out after %s", d.step, d.timeout), true))
		}
	}
	b.assignLocked()
	b.mu.Unlock()

	if !was && leader {
		b.Recover()
	}
	for _, r := range reports {
		go r()
	}
}

// releaseLocked removes an assigned delivery without requeueing it.
func (b *Broker) releaseLocked(d *delivery) {
	if d.session != nil {
		delete(d.session.inflight, d.id)
	}
	delete(b.byID, d.id)
	delete(b.byKey, d.key)
}

// loseSessionLocked disconnects a worker and puts its tasks back at the front of the queue.
func (b *Broker) loseSessionLocked(s *Session) {
	delete(b.workers, s.id)
	for id, d := range s.inflight {
		delete(b.byID, id)
		delete(b.byKey, d.key)
		b.addPendingLocked(d, true)
	}
	s.inflight = map[string]*delivery{}
	s.closeLocked()
}

func (b *Broker) dropAllLocked(reason string) {
	for id, d := range b.byID {
		d.session.sendCancelLocked(id, reason)
	}
	b.byID = map[string]*delivery{}
	b.byKey = map[string]*delivery{}
	b.pending = map[string][]*delivery{}
	for _, s := range b.workers {
		s.inflight = map[string]*delivery{}
	}
}

// failReport returns a function that reports the failure of d to the workflow.
func (b *Broker) failReport(d *delivery, code, msg string, retryable bool) func() {
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		var err error
		if d.kind == workerpb.TaskKind_TASK_KIND_COMPENSATION {
			_, err = b.opt.Applier.Apply(ctx, flowstore.CmdFailCompensationStep, flowstore.FailCompensationStepRequest{
				WorkflowID: d.workflowID, RunID: d.runID, StepName: d.step, Error: msg, Code: code})
		} else {
			_, err = b.opt.Applier.Apply(ctx, flowstore.CmdFailStep, flowstore.FailStepRequest{
				WorkflowID: d.workflowID, RunID: d.runID, StepName: d.step, Error: msg, Code: code, Retryable: retryable})
		}
		if err != nil {
			log.Printf("[Broker] reporting failure of %s/%s: %v", d.workflowID, d.step, err)
		}
	}
}

func (b *Broker) successReport(d *delivery, out []byte) func() {
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		var err error
		if d.kind == workerpb.TaskKind_TASK_KIND_COMPENSATION {
			_, err = b.opt.Applier.Apply(ctx, flowstore.CmdCompleteCompensationStep, flowstore.CompleteCompensationStepRequest{
				WorkflowID: d.workflowID, RunID: d.runID, StepName: d.step, Output: out})
		} else {
			_, err = b.opt.Applier.Apply(ctx, flowstore.CmdCompleteStep, flowstore.CompleteStepRequest{
				WorkflowID: d.workflowID, RunID: d.runID, StepName: d.step, Output: out})
		}
		if err != nil {
			log.Printf("[Broker] reporting result of %s/%s: %v", d.workflowID, d.step, err)
		}
	}
}

// Stats is a snapshot of the broker for diagnostics and tests.
type Stats struct {
	Workers  int            `json:"workers"`
	Pending  int            `json:"pending"`
	InFlight int            `json:"in_flight"`
	ByWorker map[string]int `json:"by_worker"`
}

// Stats returns the current state of the broker.
func (b *Broker) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	st := Stats{Workers: len(b.workers), InFlight: len(b.byID), ByWorker: map[string]int{}}
	for _, q := range b.pending {
		st.Pending += len(q)
	}
	for id, s := range b.workers {
		st.ByWorker[id] = len(s.inflight)
	}
	return st
}
