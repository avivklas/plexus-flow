package test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/graphflow"
	"github.com/avivklas/plexus-flow/pkg/remote"
	"github.com/avivklas/plexus-flow/pkg/remote/workerpb"
	"github.com/avivklas/plexus-flow/pkg/worker"
	"github.com/avivklas/plexus-flow/pkg/workerclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type remoteEnv struct {
	t      *testing.T
	coord  *graphflow.Coordinator
	broker *remote.Broker
	addr   string
}

// newRemoteEnv runs plexus-flow with a broker and a gRPC server on a free port.
func newRemoteEnv(t *testing.T) *remoteEnv {
	t.Helper()
	coord, err := graphflow.NewEmbeddedCoordinator(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { coord.Close() })

	e := &remoteEnv{t: t, coord: coord}
	e.broker = e.newBroker()
	t.Cleanup(e.broker.Close)

	e.serve()
	return e
}

// serve exposes the current broker on a new gRPC port and points e.addr at it.
func (e *remoteEnv) serve() {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		e.t.Fatal(err)
	}
	g := grpc.NewServer()
	remote.NewServer(e.broker).Register(g)
	go g.Serve(lis)
	e.t.Cleanup(g.Stop)
	e.addr = lis.Addr().String()
}

func (e *remoteEnv) newBroker() *remote.Broker {
	b, err := remote.NewBroker(remote.Options{
		Applier: e.coord.Upstream(), Flows: e.coord.FlowStore(),
		Local: e.coord.Executor(), Registry: e.coord.Registry(),
		HeartbeatInterval: 100 * time.Millisecond, Tick: 20 * time.Millisecond,
	})
	if err != nil {
		e.t.Fatal(err)
	}
	e.coord.SetDispatcher(b)
	return b
}

// startWorker runs a worker until the test ends or the returned stop is called.
func (e *remoteEnv) startWorker(id string, max int, register func(*worker.Registry)) (stop func()) {
	w := workerclient.New(e.addr, workerclient.Options{WorkerID: id, MaxConcurrency: max, ReconnectBackoff: 20 * time.Millisecond})
	register(w.Registry())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	e.t.Cleanup(stop)
	// wait until the broker knows the worker
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := e.broker.Stats().ByWorker[id]; ok {
			return stop
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.t.Fatalf("worker %s did not connect", id)
	return stop
}

func (e *remoteEnv) start(id string, def flow.WorkflowDefinition, meta map[string]string) {
	e.t.Helper()
	if _, err := e.coord.StartWorkflow(context.Background(), flowstore.StartWorkflowRequest{
		WorkflowID: id, Definition: def, Input: json.RawMessage(`{"in":1}`), Metadata: meta}); err != nil {
		e.t.Fatal(err)
	}
}

func (e *remoteEnv) wait(id string, want flow.Status) *flow.WorkflowInstance {
	e.t.Helper()
	return waitStatus(e.t, e.coord, id, want)
}

func ok(out string) worker.ActivityFunc {
	return func(context.Context, json.RawMessage) (json.RawMessage, error) { return json.RawMessage(out), nil }
}

func TestRemoteWorkerRunsFlowWithDependencies(t *testing.T) {
	e := newRemoteEnv(t)
	var gotDeps map[string]json.RawMessage
	var gotMeta map[string]string
	e.startWorker("w1", 4, func(r *worker.Registry) {
		r.Register("first", ok(`{"x":42}`))
		r.Register("second", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
			info, _ := workerclient.TaskFromContext(ctx)
			gotDeps, gotMeta = info.DependencyOutputs, info.Metadata
			return json.RawMessage(`{"done":true}`), nil
		})
	})
	e.start("wf", flow.WorkflowDefinition{Name: "deps", Steps: []flow.StepDefinition{
		{Name: "a", Activity: "first"},
		{Name: "b", Activity: "second", DependsOn: []string{"a"}},
	}}, map[string]string{"tenant": "acme"})

	wf := e.wait("wf", flow.StatusCompleted)
	if string(gotDeps["a"]) != `{"x":42}` {
		t.Fatalf("dependency output not delivered: %v", gotDeps)
	}
	if gotMeta["tenant"] != "acme" {
		t.Fatalf("metadata not delivered: %v", gotMeta)
	}
	if string(wf.Steps["a"].Output) != `{"x":42}` {
		t.Fatalf("step output not recorded: %s", wf.Steps["a"].Output)
	}
}

func TestRemoteFailureCodeAndReverseCompensation(t *testing.T) {
	e := newRemoteEnv(t)
	var mu sync.Mutex
	var order []string
	rec := func(name string) worker.ActivityFunc {
		return func(context.Context, json.RawMessage) (json.RawMessage, error) {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return json.RawMessage(`{}`), nil
		}
	}
	e.startWorker("w1", 2, func(r *worker.Registry) {
		r.Register("do-a", rec("do-a"))
		r.Register("do-b", rec("do-b"))
		r.Register("undo-a", rec("undo-a"))
		r.Register("undo-b", rec("undo-b"))
		r.Register("do-c", func(context.Context, json.RawMessage) (json.RawMessage, error) {
			return nil, worker.NonRetryable(worker.WithCode("ROUTE_BLOCKED", errors.New("destination route is blocked")))
		})
	})
	e.start("wf", flow.WorkflowDefinition{Name: "saga", Steps: []flow.StepDefinition{
		{Name: "a", Activity: "do-a", CompensatingAction: "undo-a", Retries: 3},
		{Name: "b", Activity: "do-b", CompensatingAction: "undo-b"},
		{Name: "c", Activity: "do-c", Retries: 3},
	}}, nil)

	wf := e.wait("wf", flow.StatusCompensated)
	if wf.ErrorCode != "ROUTE_BLOCKED" {
		t.Fatalf("failure code not propagated: %q (%s)", wf.ErrorCode, wf.Error)
	}
	if wf.Steps["c"].Attempt != 1 {
		t.Fatalf("a non-retryable failure must not be retried, attempts=%d", wf.Steps["c"].Attempt)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 4 || order[2] != "undo-b" || order[3] != "undo-a" {
		t.Fatalf("expected compensation in reverse order, got %v", order)
	}
}

func TestRemoteRetriesAreEngineRetries(t *testing.T) {
	e := newRemoteEnv(t)
	var calls atomic.Int32
	var attempts []int
	var mu sync.Mutex
	e.startWorker("w1", 1, func(r *worker.Registry) {
		r.Register("flaky", func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
			info, _ := workerclient.TaskFromContext(ctx)
			mu.Lock()
			attempts = append(attempts, info.Attempt)
			mu.Unlock()
			if calls.Add(1) < 3 {
				return nil, errors.New("transient")
			}
			return json.RawMessage(`{}`), nil
		})
		r.Register("always-fails", func(context.Context, json.RawMessage) (json.RawMessage, error) {
			calls.Add(100)
			return nil, errors.New("boom")
		})
	})
	e.start("ok", flow.WorkflowDefinition{Name: "retry", Steps: []flow.StepDefinition{{Name: "s", Activity: "flaky", Retries: 2}}}, nil)
	e.wait("ok", flow.StatusCompleted)
	mu.Lock()
	if len(attempts) != 3 || attempts[0] != 1 || attempts[1] != 2 || attempts[2] != 3 {
		t.Fatalf("attempt numbers should count 1,2,3, got %v", attempts)
	}
	mu.Unlock()

	calls.Store(0)
	e.start("bad", flow.WorkflowDefinition{Name: "retry-exhausted", Steps: []flow.StepDefinition{{Name: "s", Activity: "always-fails", Retries: 1}}}, nil)
	wf := e.wait("bad", flow.StatusFailed)
	if calls.Load() != 200 {
		t.Fatalf("Retries: 1 means two attempts, got %d calls", calls.Load()/100)
	}
	if wf.Steps["s"].Attempt != 2 {
		t.Fatalf("attempts recorded: %d", wf.Steps["s"].Attempt)
	}
}

func TestRemoteStepTimeoutIsEnforcedByServer(t *testing.T) {
	e := newRemoteEnv(t)
	cancelled := make(chan struct{})
	e.startWorker("w1", 1, func(r *worker.Registry) {
		r.Register("hang", func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
			<-ctx.Done()
			close(cancelled)
			return nil, ctx.Err()
		})
	})
	e.start("wf", flow.WorkflowDefinition{Name: "timeout", Steps: []flow.StepDefinition{
		{Name: "s", Activity: "hang", Timeout: 300 * time.Millisecond}}}, nil)
	wf := e.wait("wf", flow.StatusFailed)
	if wf.ErrorCode != remote.CodeTimeout {
		t.Fatalf("expected code %s, got %q (%s)", remote.CodeTimeout, wf.ErrorCode, wf.Error)
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("the worker was not told to stop")
	}
}

func TestRemoteTaskIsRedeliveredWhenWorkerDisconnects(t *testing.T) {
	e := newRemoteEnv(t)
	started := make(chan struct{})
	var startedOnce sync.Once
	stopA := e.startWorker("worker-a", 1, func(r *worker.Registry) {
		r.Register("work", func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
			startedOnce.Do(func() { close(started) })
			<-ctx.Done()
			return nil, ctx.Err()
		})
	})
	e.start("wf", flow.WorkflowDefinition{Name: "redeliver", Steps: []flow.StepDefinition{
		{Name: "s", Activity: "work", Timeout: 30 * time.Second}}}, nil)
	<-started

	var ranOn atomic.Value
	e.startWorker("worker-b", 1, func(r *worker.Registry) {
		r.Register("work", func(context.Context, json.RawMessage) (json.RawMessage, error) {
			ranOn.Store("worker-b")
			return json.RawMessage(`{}`), nil
		})
	})
	stopA() // worker A goes away mid-task

	e.wait("wf", flow.StatusCompleted)
	if ranOn.Load() != "worker-b" {
		t.Fatalf("the task should have been redelivered to worker-b")
	}
}

func TestRemoteSilentWorkerLosesItsLease(t *testing.T) {
	e := newRemoteEnv(t)

	// A "worker" that registers, accepts a task and then goes silent without closing the stream.
	conn, err := grpc.NewClient(e.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stream, err := workerpb.NewWorkerServiceClient(conn).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(&workerpb.WorkerMessage{Message: &workerpb.WorkerMessage_Register{
		Register: &workerpb.Register{WorkerId: "zombie", Activities: []string{"work"}, MaxConcurrency: 1}}})
	if _, err := stream.Recv(); err != nil { // Registered
		t.Fatal(err)
	}

	e.start("wf", flow.WorkflowDefinition{Name: "lease", Steps: []flow.StepDefinition{
		{Name: "s", Activity: "work", Timeout: 30 * time.Second}}}, nil)
	msg, err := stream.Recv()
	if err != nil || msg.GetTask() == nil {
		t.Fatalf("zombie should have received the task: %v %v", msg, err)
	}

	e.startWorker("healthy", 1, func(r *worker.Registry) { r.Register("work", ok(`{}`)) })
	e.wait("wf", flow.StatusCompleted) // only possible after the zombie's lease expired (3 x 100ms)
	if _, err := stream.Recv(); err == nil {
		t.Fatal("the zombie's stream should have been closed by the server")
	}
}

func TestRemoteCancelReachesRunningWorker(t *testing.T) {
	e := newRemoteEnv(t)
	running := make(chan struct{})
	interrupted := make(chan struct{})
	var undone atomic.Int32
	e.startWorker("w1", 2, func(r *worker.Registry) {
		r.Register("quick", ok(`{}`))
		r.Register("undo-quick", func(context.Context, json.RawMessage) (json.RawMessage, error) {
			undone.Add(1)
			return json.RawMessage(`{}`), nil
		})
		r.Register("long", func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
			close(running)
			<-ctx.Done()
			close(interrupted)
			return nil, ctx.Err()
		})
	})
	e.start("wf", flow.WorkflowDefinition{Name: "cancel-remote", Steps: []flow.StepDefinition{
		{Name: "a", Activity: "quick", CompensatingAction: "undo-quick"},
		{Name: "b", Activity: "long", DependsOn: []string{"a"}, Timeout: 30 * time.Second},
	}}, nil)
	<-running
	if err := e.coord.CancelWorkflow(context.Background(), "wf", "stop"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-interrupted:
	case <-time.After(2 * time.Second):
		t.Fatal("the running activity was not cancelled")
	}
	wf := e.wait("wf", flow.StatusCompensated)
	if undone.Load() != 1 || wf.Steps["b"].Status != flow.StepStatusSkipped {
		t.Fatalf("undone=%d step b=%s", undone.Load(), wf.Steps["b"].Status)
	}
	if st := e.broker.Stats(); st.InFlight != 0 {
		t.Fatalf("broker still tracks %d task(s)", st.InFlight)
	}
}

func TestRemoteWorkerConcurrencyLimit(t *testing.T) {
	e := newRemoteEnv(t)
	var cur, peak atomic.Int32
	e.startWorker("w1", 2, func(r *worker.Registry) {
		r.Register("slow", func(context.Context, json.RawMessage) (json.RawMessage, error) {
			n := cur.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			cur.Add(-1)
			return json.RawMessage(`{}`), nil
		})
	})
	for i := 0; i < 8; i++ {
		e.start("wf-"+string(rune('a'+i)), flow.WorkflowDefinition{Name: "limit", Steps: []flow.StepDefinition{{Name: "s", Activity: "slow"}}}, nil)
	}
	for i := 0; i < 8; i++ {
		e.wait("wf-"+string(rune('a'+i)), flow.StatusCompleted)
	}
	if peak.Load() != 2 {
		t.Fatalf("expected exactly 2 tasks in parallel, got a peak of %d", peak.Load())
	}
}

func TestRemoteTaskWaitsForAWorker(t *testing.T) {
	e := newRemoteEnv(t)
	e.start("wf", flow.WorkflowDefinition{Name: "late-worker", Steps: []flow.StepDefinition{
		{Name: "s", Activity: "not-yet", Timeout: 30 * time.Second}}}, nil)
	time.Sleep(200 * time.Millisecond)
	if wf, _ := e.coord.GetWorkflow("wf"); wf.Status != flow.StatusRunning {
		t.Fatalf("with no worker the step must wait, got %s", wf.Status)
	}
	if e.broker.Stats().Pending != 1 {
		t.Fatalf("task should be pending: %+v", e.broker.Stats())
	}
	e.startWorker("late", 1, func(r *worker.Registry) { r.Register("not-yet", ok(`{}`)) })
	e.wait("wf", flow.StatusCompleted)
}

func TestRemoteBrokerRestartRecoversRunningSteps(t *testing.T) {
	e := newRemoteEnv(t)
	started := make(chan struct{})
	e.startWorker("w-old", 1, func(r *worker.Registry) {
		r.Register("work", func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		})
	})
	e.start("wf", flow.WorkflowDefinition{Name: "recover", Steps: []flow.StepDefinition{
		{Name: "s", Activity: "work", Timeout: 30 * time.Second}}}, nil)
	<-started

	// The broker "crashes": its memory of the task is gone, the workflow still says RUNNING.
	e.broker.Close()
	e.broker = e.newBroker()
	e.serve()
	e.startWorker("w-new", 1, func(r *worker.Registry) { r.Register("work", ok(`{"recovered":true}`)) })
	e.broker.Recover()

	wf := e.wait("wf", flow.StatusCompleted)
	if string(wf.Steps["s"].Output) != `{"recovered":true}` {
		t.Fatalf("unexpected output %s", wf.Steps["s"].Output)
	}
}
