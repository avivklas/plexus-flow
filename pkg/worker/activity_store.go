package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/avivklas/plexus/pkg/store"
)

// Ensure ActivityStore implements store.Store.
var _ store.Store = (*ActivityStore)(nil)

// ActivityStore is the downstream state store that receives activity and compensation dispatch commands.
type ActivityStore struct {
	store.BaseStore
	mu            sync.RWMutex
	tasks         map[string]ActivityTask
	compensations map[string]CompensationTask
	onDispatch    func(task ActivityTask)
	onCompensate  func(task CompensationTask)
}

// NewActivityStore creates an initialized ActivityStore.
func NewActivityStore() *ActivityStore {
	as := &ActivityStore{
		BaseStore:     store.NewBaseStore(),
		tasks:         make(map[string]ActivityTask),
		compensations: make(map[string]CompensationTask),
	}
	as.registerHandlers()
	return as
}

// ID implements store.Store.
func (s *ActivityStore) ID() store.StoreID {
	return "activitystore"
}

// SetListeners registers callback hooks when activities or compensations are committed to this store.
func (s *ActivityStore) SetListeners(
	onDispatch func(task ActivityTask),
	onCompensate func(task CompensationTask),
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onDispatch = onDispatch
	s.onCompensate = onCompensate
}

// TaskCount returns the number of retained activity and compensation task records in the store.
func (s *ActivityStore) TaskCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.tasks) + len(s.compensations)
}

func (s *ActivityStore) registerHandlers() {
	r := s.Router()

	// 1. Dispatch Activity
	store.HandleTyped(r, CmdDispatchActivity, func(ctx context.Context, task ActivityTask) (string, error) {
		taskKey := fmt.Sprintf("%s:%s:%d", task.WorkflowID, task.StepName, task.Attempt)

		s.mu.Lock()
		if _, already := s.tasks[taskKey]; already {
			s.mu.Unlock()
			return "ALREADY_DISPATCHED", nil
		}
		s.tasks[taskKey] = task
		dispatchCb := s.onDispatch
		s.mu.Unlock()

		if dispatchCb != nil {
			go dispatchCb(task)
		}

		return "DISPATCHED", nil
	})

	// 2. Compensate Activity
	store.HandleTyped(r, CmdCompensateActivity, func(ctx context.Context, task CompensationTask) (string, error) {
		taskKey := fmt.Sprintf("%s:%s:%d", task.WorkflowID, task.StepName, task.Attempt)

		s.mu.Lock()
		if _, already := s.compensations[taskKey]; already {
			s.mu.Unlock()
			return "ALREADY_COMPENSATED", nil
		}
		s.compensations[taskKey] = task
		compensateCb := s.onCompensate
		s.mu.Unlock()

		if compensateCb != nil {
			go compensateCb(task)
		}

		return "COMPENSATION_DISPATCHED", nil
	})

	// 3. Prune Workflow Activities (frees memory once a workflow reaches a terminal state)
	store.HandleTyped(r, CmdPruneWorkflowActivities, func(ctx context.Context, req PruneWorkflowActivitiesRequest) (int, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		pruned := 0
		for k, t := range s.tasks {
			if t.WorkflowID == req.WorkflowID {
				delete(s.tasks, k)
				pruned++
			}
		}
		for k, c := range s.compensations {
			if c.WorkflowID == req.WorkflowID {
				delete(s.compensations, k)
				pruned++
			}
		}
		return pruned, nil
	})
}

// Snapshot serializes ActivityStore state.
func (s *ActivityStore) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data := struct {
		Tasks         map[string]ActivityTask     `json:"tasks"`
		Compensations map[string]CompensationTask `json:"compensations"`
	}{
		Tasks:         s.tasks,
		Compensations: s.compensations,
	}

	return json.Marshal(data)
}

// Restore resets state from snapshot.
func (s *ActivityStore) Restore(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var state struct {
		Tasks         map[string]ActivityTask     `json:"tasks"`
		Compensations map[string]CompensationTask `json:"compensations"`
	}

	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("restore activity store: %w", err)
	}

	s.tasks = state.Tasks
	if s.tasks == nil {
		s.tasks = make(map[string]ActivityTask)
	}
	s.compensations = state.Compensations
	if s.compensations == nil {
		s.compensations = make(map[string]CompensationTask)
	}

	return nil
}
