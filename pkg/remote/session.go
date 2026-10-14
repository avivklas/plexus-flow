package remote

import (
	"errors"
	"fmt"
	"time"

	"github.com/avivklas/plexus-flow/pkg/remote/workerpb"
)

// Session is one connected worker.
type Session struct {
	b          *Broker
	id         string
	activities map[string]bool
	max        int
	inflight   map[string]*delivery
	lastSeen   time.Time
	out        chan *workerpb.ServerMessage
	closed     bool
}

// ErrBrokerClosed is returned when attaching to a closed broker.
var ErrBrokerClosed = errors.New("broker closed")

// Attach registers a worker. The caller must drain Out and call Close when
// the connection ends.
func (b *Broker) Attach(workerID string, activities []string, maxConcurrency int) (*Session, error) {
	if workerID == "" {
		return nil, errors.New("worker id is required")
	}
	if len(activities) == 0 {
		return nil, errors.New("a worker must register at least one activity")
	}
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}
	s := &Session{
		b: b, id: workerID, activities: map[string]bool{}, max: maxConcurrency,
		inflight: map[string]*delivery{}, lastSeen: time.Now(),
		out: make(chan *workerpb.ServerMessage, 2*maxConcurrency+16),
	}
	for _, a := range activities {
		s.activities[a] = true
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrBrokerClosed
	}
	if !b.isLeader() {
		return nil, errors.New("not the leader: connect to the leader node")
	}
	if old, dup := b.workers[workerID]; dup { // a reconnect replaces the stale session
		b.loseSessionLocked(old)
	}
	b.workers[workerID] = s
	b.assignLocked()
	return s, nil
}

// HeartbeatInterval is what the worker is told to use.
func (s *Session) HeartbeatInterval() time.Duration { return s.b.opt.HeartbeatInterval }

// Out delivers messages for the worker; it is closed when the session ends.
func (s *Session) Out() <-chan *workerpb.ServerMessage { return s.out }

// Heartbeat records that the worker is alive.
func (s *Session) Heartbeat() {
	s.b.mu.Lock()
	s.lastSeen = time.Now()
	s.b.mu.Unlock()
}

// Result handles the outcome of a task.
func (s *Session) Result(r *workerpb.TaskResult) error {
	b := s.b
	b.mu.Lock()
	s.lastSeen = time.Now()
	d, ok := s.inflight[r.GetTaskId()]
	if !ok { // stale delivery (timed out, cancelled or redelivered): the engine has moved on
		b.mu.Unlock()
		return nil
	}

	var report func()
	switch o := r.Outcome.(type) {
	case *workerpb.TaskResult_Success:
		b.releaseLocked(d)
		report = b.successReport(d, o.Success.GetOutput())
	case *workerpb.TaskResult_Failure:
		f := o.Failure
		b.releaseLocked(d)
		if d.kind == workerpb.TaskKind_TASK_KIND_COMPENSATION && f.GetRetryable() && d.tries < d.retries {
			d.tries++
			b.addPendingLocked(d, true)
		} else {
			report = b.failReport(d, f.GetCode(), f.GetMessage(), f.GetRetryable())
		}
	default:
		b.mu.Unlock()
		return fmt.Errorf("task result %q has no outcome", r.GetTaskId())
	}
	b.assignLocked()
	b.mu.Unlock()

	if report != nil {
		go report()
	}
	return nil
}

// Close ends the session; its unfinished tasks are redelivered to other workers.
func (s *Session) Close() {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	if cur, ok := s.b.workers[s.id]; ok && cur == s {
		s.b.loseSessionLocked(s)
		s.b.assignLocked()
	} else {
		s.closeLocked()
	}
}

func (s *Session) closeLocked() {
	if !s.closed {
		s.closed = true
		close(s.out)
	}
}

func (s *Session) sendLocked(m *workerpb.ServerMessage) bool {
	if s.closed {
		return false
	}
	select {
	case s.out <- m:
		return true
	default:
		return false
	}
}

func (s *Session) sendCancelLocked(taskID, reason string) {
	if s == nil {
		return
	}
	s.sendLocked(&workerpb.ServerMessage{Message: &workerpb.ServerMessage_Cancel{
		Cancel: &workerpb.CancelTask{TaskId: taskID, Reason: reason}}})
}
