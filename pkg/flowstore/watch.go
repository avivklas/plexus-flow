package flowstore

import (
	"context"
	"reflect"
	"sync"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus/pkg/store"
)

// Watching workflows
//
// Every command that changes a workflow bumps its Version and publishes a
// copy of it to the watchers whose filter matches. Commands are applied in
// Raft order on every node, so versions are identical across replicas and any
// node can serve watches.
//
// Publishing never blocks the state machine: a watcher that falls more than
// its buffer behind is dropped (its channel is closed) and should re-subscribe,
// reading the current state first.

type watcher struct {
	match func(*flow.WorkflowInstance) bool
	// deliver hands a change to the watcher without blocking; false means it
	// is full. copyOf returns a deep copy of the instance, made at most once
	// per change and shared by every watcher that asks for it.
	deliver func(wf *flow.WorkflowInstance, copyOf func() *flow.WorkflowInstance) bool
	close   func()
}

type watchers struct {
	mu   sync.Mutex
	next int
	set  map[int]*watcher
}

// Watch subscribes to changes of the workflows match accepts. match runs on
// the state machine's goroutine against the live instance, so it must be fast
// and must not modify or retain it. Delivered instances are copies shared
// with other watchers: read them, don't modify them. The channel is closed
// when cancel is called or the watcher falls more than buffer changes behind.
func (s *Store) Watch(match func(*flow.WorkflowInstance) bool, buffer int) (<-chan *flow.WorkflowInstance, func()) {
	ch := make(chan *flow.WorkflowInstance, max(buffer, 1))
	cancel := s.subscribe(&watcher{
		match: match,
		deliver: func(_ *flow.WorkflowInstance, copyOf func() *flow.WorkflowInstance) bool {
			select {
			case ch <- copyOf():
				return true
			default:
				return false
			}
		},
		close: func() { close(ch) },
	})
	return ch, cancel
}

// WatchSummaries is Watch for list views: it delivers the small summary of
// each change instead of a full copy of the workflow.
func (s *Store) WatchSummaries(match func(*flow.WorkflowInstance) bool, buffer int) (<-chan flow.TraceSummary, func()) {
	ch := make(chan flow.TraceSummary, max(buffer, 1))
	cancel := s.subscribe(&watcher{
		match: match,
		deliver: func(wf *flow.WorkflowInstance, _ func() *flow.WorkflowInstance) bool {
			select {
			case ch <- wf.Summary():
				return true
			default:
				return false
			}
		},
		close: func() { close(ch) },
	})
	return ch, cancel
}

func (s *Store) subscribe(w *watcher) func() {
	s.watch.mu.Lock()
	if s.watch.set == nil {
		s.watch.set = make(map[int]*watcher)
	}
	id := s.watch.next
	s.watch.next++
	s.watch.set[id] = w
	s.watch.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			s.watch.mu.Lock()
			defer s.watch.mu.Unlock()
			if _, ok := s.watch.set[id]; ok {
				delete(s.watch.set, id)
				w.close()
			}
		})
	}
}

// publishLocked delivers wf to matching watchers. Callers hold s.mu.
func (s *Store) publishLocked(wf *flow.WorkflowInstance) {
	s.watch.mu.Lock()
	defer s.watch.mu.Unlock()
	var cp *flow.WorkflowInstance
	copyOf := func() *flow.WorkflowInstance {
		if cp == nil {
			cp = wf.Clone()
		}
		return cp
	}
	for id, w := range s.watch.set {
		if w.match != nil && !w.match(wf) {
			continue
		}
		if !w.deliver(wf, copyOf) { // too slow: drop it rather than stall the state machine
			delete(s.watch.set, id)
			w.close()
		}
	}
}

// fingerprint captures what any real change to a workflow alters.
type fingerprint struct {
	exists  bool
	status  flow.Status
	updated int64
	events  int
}

func (s *Store) fingerprintOf(id string) fingerprint {
	s.mu.RLock()
	defer s.mu.RUnlock()
	wf, ok := s.workflows[id]
	if !ok {
		return fingerprint{}
	}
	return fingerprint{exists: true, status: wf.Status, updated: wf.UpdatedAt.UnixNano(), events: len(wf.History)}
}

// handle registers a command handler that versions and publishes the workflow
// it changed. Commands are applied one at a time, so nothing else can modify
// the workflow between the handler and the publish.
func handle[Req any, Resp any](s *Store, r *store.Router, typ store.CommandType, fn func(context.Context, Req) (Resp, error)) {
	store.HandleTyped(r, typ, func(ctx context.Context, req Req) (Resp, error) {
		id := workflowIDOf(req)
		before := s.fingerprintOf(id)
		resp, err := fn(ctx, req)
		if err != nil || id == "" {
			return resp, err
		}
		if s.fingerprintOf(id) != before {
			s.mu.Lock()
			if wf, ok := s.workflows[id]; ok {
				wf.Version++
				s.publishLocked(wf)
			}
			s.mu.Unlock()
		}
		return resp, err
	})
}

// workflowIDOf reads the WorkflowID field every request type carries.
func workflowIDOf(req any) string {
	v := reflect.ValueOf(req)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return ""
	}
	if f := v.FieldByName("WorkflowID"); f.IsValid() && f.Kind() == reflect.String {
		return f.String()
	}
	return ""
}
