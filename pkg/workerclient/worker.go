// Package workerclient is the SDK for running plexus-flow activities in your
// own process: register activity functions, point the worker at the
// plexus-flow gRPC address and Run it.
//
//	w := workerclient.New("plexus-flow:9090", workerclient.Options{WorkerID: "pacs-1", MaxConcurrency: 16})
//	w.Registry().Register("fetch-study", fetchStudy)
//	go w.Run(ctx)
//
// Activities must be idempotent: delivery is at-least-once.
package workerclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/avivklas/plexus-flow/pkg/remote/workerpb"
	"github.com/avivklas/plexus-flow/pkg/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Options configure a Worker.
type Options struct {
	// WorkerID identifies this worker; it must be unique among connected workers.
	// Defaults to hostname-pid.
	WorkerID string
	// MaxConcurrency is the number of tasks run in parallel (default 1).
	MaxConcurrency int
	// Registry holds the activities; a new one is created when nil.
	Registry *worker.Registry
	// Addresses optionally lists multiple cluster gRPC addresses for automatic leader failover.
	Addresses []string
	// DialOptions replace the default (insecure transport).
	DialOptions []grpc.DialOption
	// ReconnectBackoff is the pause before reconnecting (default 500ms, growing to 5s).
	ReconnectBackoff time.Duration
}

// TaskInfo describes the task being executed; read it with TaskFromContext.
type TaskInfo struct {
	TaskID            string
	Compensation      bool
	WorkflowID        string
	RunID             string
	StepName          string
	Activity          string
	Attempt           int
	LastProgress      json.RawMessage
	DependencyOutputs map[string]json.RawMessage
	Metadata          map[string]string
	reportProgress    func(json.RawMessage) error
}

type taskKey struct{}

// TaskFromContext returns the task an activity is running for.
func TaskFromContext(ctx context.Context) (TaskInfo, bool) {
	t, ok := ctx.Value(taskKey{}).(TaskInfo)
	return t, ok
}

// ReportProgress sends a heartbeat checkpoint for the currently executing activity task.
// If the worker crashes or the task is retried, the next attempt receives this checkpoint
// in TaskInfo.LastProgress.
func ReportProgress(ctx context.Context, progress json.RawMessage) error {
	t, ok := TaskFromContext(ctx)
	if !ok || t.reportProgress == nil {
		return nil
	}
	return t.reportProgress(progress)
}

// Worker connects to plexus-flow and executes activities.
type Worker struct {
	addr string
	opt  Options
}

// New creates a worker for the plexus-flow gRPC address.
func New(addr string, opt Options) *Worker {
	if opt.WorkerID == "" {
		host, _ := os.Hostname()
		opt.WorkerID = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	if opt.MaxConcurrency <= 0 {
		opt.MaxConcurrency = 1
	}
	if opt.Registry == nil {
		opt.Registry = worker.NewRegistry()
	}
	if opt.ReconnectBackoff <= 0 {
		opt.ReconnectBackoff = 500 * time.Millisecond
	}
	if len(opt.DialOptions) == 0 {
		opt.DialOptions = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return &Worker{addr: addr, opt: opt}
}

// Registry returns the activity registry.
func (w *Worker) Registry() *worker.Registry { return w.opt.Registry }

// Run connects and serves tasks until ctx is cancelled, reconnecting with
// backoff when the connection drops and cycling across cluster addresses when
// multiple nodes are configured. Tasks in flight when a connection drops are
// redelivered by the server.
func (w *Worker) Run(ctx context.Context) error {
	addrs := w.targetAddresses()
	if len(addrs) == 0 {
		return errors.New("worker: at least one server address is required")
	}

	conns := make([]*grpc.ClientConn, len(addrs))
	clients := make([]workerpb.WorkerServiceClient, len(addrs))
	for i, a := range addrs {
		conn, err := grpc.NewClient(a, w.opt.DialOptions...)
		if err != nil {
			for j := 0; j < i; j++ {
				_ = conns[j].Close()
			}
			return err
		}
		conns[i] = conn
		clients[i] = workerpb.NewWorkerServiceClient(conn)
	}
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()

	backoff := w.opt.ReconnectBackoff
	idx := 0
	triedInCycle := 0
	for ctx.Err() == nil {
		client := clients[idx]
		target := addrs[idx]
		started := time.Now()
		err := w.session(ctx, client)
		if ctx.Err() != nil {
			return nil
		}
		if time.Since(started) > 2*time.Second {
			backoff = w.opt.ReconnectBackoff
			triedInCycle = 0
		}

		idx = (idx + 1) % len(clients)
		triedInCycle++
		if triedInCycle < len(clients) {
			// Immediately try the next cluster node address before sleeping backoff.
			continue
		}
		triedInCycle = 0
		log.Printf("[Worker %s] disconnected from %s: %v", w.opt.WorkerID, target, err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
	return nil
}

func (w *Worker) targetAddresses() []string {
	seen := map[string]bool{}
	var out []string
	add := func(raw string) {
		for _, part := range strings.Split(raw, ",") {
			if p := strings.TrimSpace(part); p != "" && !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
	}
	add(w.addr)
	for _, a := range w.opt.Addresses {
		add(a)
	}
	return out
}

func (w *Worker) activities() []string {
	return w.opt.Registry.Names()
}

func (w *Worker) session(ctx context.Context, client workerpb.WorkerServiceClient) error {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Connect(sctx)
	if err != nil {
		return err
	}
	names := w.activities()
	sort.Strings(names)
	if err := stream.Send(&workerpb.WorkerMessage{Message: &workerpb.WorkerMessage_Register{Register: &workerpb.Register{
		WorkerId: w.opt.WorkerID, Activities: names, MaxConcurrency: int32(w.opt.MaxConcurrency)}}}); err != nil {
		return err
	}

	var sendMu sync.Mutex
	send := func(m *workerpb.WorkerMessage) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(m)
	}

	var (
		running   sync.WaitGroup
		cancelsMu sync.Mutex
		cancels   = map[string]context.CancelFunc{}
	)
	defer func() {
		cancel()
		running.Wait()
	}()

	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		switch m := msg.Message.(type) {
		case *workerpb.ServerMessage_Registered:
			interval := time.Duration(m.Registered.GetHeartbeatIntervalMs()) * time.Millisecond
			if interval <= 0 {
				interval = 2 * time.Second
			}
			running.Add(1)
			go func() {
				defer running.Done()
				t := time.NewTicker(interval / 2)
				defer t.Stop()
				for {
					select {
					case <-sctx.Done():
						return
					case <-t.C:
						if err := send(&workerpb.WorkerMessage{Message: &workerpb.WorkerMessage_Heartbeat{Heartbeat: &workerpb.Heartbeat{}}}); err != nil {
							cancel()
							return
						}
					}
				}
			}()

		case *workerpb.ServerMessage_Task:
			t := m.Task
			tctx, tcancel := context.WithCancel(sctx)
			cancelsMu.Lock()
			cancels[t.GetTaskId()] = tcancel
			cancelsMu.Unlock()
			running.Add(1)
			go func() {
				defer running.Done()
				defer func() {
					cancelsMu.Lock()
					delete(cancels, t.GetTaskId())
					cancelsMu.Unlock()
					tcancel()
				}()
				res := w.execute(tctx, t, send)
				if tctx.Err() != nil {
					return // cancelled by the server or worker shutting down
				}
				if err := send(&workerpb.WorkerMessage{Message: &workerpb.WorkerMessage_Result{Result: res}}); err != nil {
					cancel()
				}
			}()

		case *workerpb.ServerMessage_Cancel:
			cancelsMu.Lock()
			if c := cancels[m.Cancel.GetTaskId()]; c != nil {
				c()
			}
			cancelsMu.Unlock()
		}
	}
}

func (w *Worker) execute(ctx context.Context, t *workerpb.Task, send func(*workerpb.WorkerMessage) error) (res *workerpb.TaskResult) {
	res = &workerpb.TaskResult{TaskId: t.GetTaskId()}
	fail := func(code, msg string, retryable bool) *workerpb.TaskResult {
		res.Outcome = &workerpb.TaskResult_Failure{Failure: &workerpb.Failure{Code: code, Message: msg, Retryable: retryable}}
		return res
	}
	defer func() {
		if r := recover(); r != nil {
			res = fail("PANIC", fmt.Sprint("activity panicked: ", r), true)
		}
	}()

	fn, ok := w.opt.Registry.Get(t.GetActivity())
	if !ok {
		return fail("ACTIVITY_NOT_REGISTERED", fmt.Sprintf("activity %q is not registered on worker %s", t.GetActivity(), w.opt.WorkerID), false)
	}
	deps := make(map[string]json.RawMessage, len(t.GetDependencyOutputs()))
	for k, v := range t.GetDependencyOutputs() {
		deps[k] = v
	}
	ctx = context.WithValue(ctx, taskKey{}, TaskInfo{
		TaskID: t.GetTaskId(), Compensation: t.GetKind() == workerpb.TaskKind_TASK_KIND_COMPENSATION,
		WorkflowID: t.GetWorkflowId(), RunID: t.GetRunId(), StepName: t.GetStepName(), Activity: t.GetActivity(),
		Attempt: int(t.GetAttempt()), LastProgress: t.GetLastProgress(), DependencyOutputs: deps, Metadata: t.GetMetadata(),
		reportProgress: func(progress json.RawMessage) error {
			if send == nil {
				return nil
			}
			return send(&workerpb.WorkerMessage{
				Message: &workerpb.WorkerMessage_Heartbeat{
					Heartbeat: &workerpb.Heartbeat{
						TaskId:   t.GetTaskId(),
						Progress: append([]byte(nil), progress...),
					},
				},
			})
		},
	})
	if ms := t.GetTimeoutMs(); ms > 0 {
		var c context.CancelFunc
		ctx, c = context.WithTimeout(ctx, time.Duration(ms)*time.Millisecond)
		defer c()
	}

	out, err := fn(ctx, t.GetInput())
	if err != nil {
		code := worker.CodeOf(err)
		if code == "" && errors.Is(err, context.DeadlineExceeded) {
			code = "TIMEOUT"
		}
		return fail(code, err.Error(), !worker.IsNonRetryable(err))
	}
	res.Outcome = &workerpb.TaskResult_Success{Success: &workerpb.Success{Output: out}}
	return res
}
