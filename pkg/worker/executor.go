package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/avivklas/plexus/pkg/store"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
)

// WorkflowApplier represents anything that can propose state changes back to the workflow machine.
type WorkflowApplier interface {
	Apply(ctx context.Context, cmdType store.CommandType, data any) (any, error)
}

// WorkflowApplierFunc allows using a plain function as WorkflowApplier.
type WorkflowApplierFunc func(ctx context.Context, cmdType store.CommandType, data any) (any, error)

func (f WorkflowApplierFunc) Apply(ctx context.Context, cmdType store.CommandType, data any) (any, error) {
	return f(ctx, cmdType, data)
}

// Executor coordinates activity invocation, timeouts, retries, and reporting.
type Executor struct {
	mu       sync.RWMutex
	registry *Registry
	applier  WorkflowApplier
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewExecutor creates a new activity executor.
func NewExecutor(registry *Registry, applier WorkflowApplier) *Executor {
	ctx, cancel := context.WithCancel(context.Background())
	return &Executor{
		registry: registry,
		applier:  applier,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// SetApplier updates the workflow applier handle (e.g. after cluster election or startup).
func (e *Executor) SetApplier(applier WorkflowApplier) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.applier = applier
}

// Close gracefully stops the executor.
func (e *Executor) Close() {
	e.cancel()
	e.wg.Wait()
}

// ExecuteActivity runs an activity task with timeouts, retries, and completion/failure callbacks.
func (e *Executor) ExecuteActivity(task ActivityTask) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()

		timeout := task.Timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}

		taskCtx, cancel := context.WithTimeout(e.ctx, timeout)
		defer cancel()

		maxAttempts := task.Retries + 1
		var (
			lastErr error
			output  json.RawMessage
		)

		for attempt := 1; attempt <= maxAttempts; attempt++ {
			select {
			case <-taskCtx.Done():
				lastErr = taskCtx.Err()
				break
			default:
			}

			fn, ok := e.registry.Get(task.Activity)
			if !ok {
				lastErr = fmt.Errorf("activity function %q not registered", task.Activity)
				break
			}

			res, err := fn(taskCtx, task.Input)
			if err == nil {
				output = res
				lastErr = nil
				break
			}

			lastErr = err
			if attempt < maxAttempts {
				backoff := time.Duration(attempt*15) * time.Millisecond
				select {
				case <-time.After(backoff):
				case <-taskCtx.Done():
					lastErr = taskCtx.Err()
					break
				}
			}
		}

		e.mu.RLock()
		applier := e.applier
		e.mu.RUnlock()

		if applier == nil {
			log.Printf("[Executor] Error: no applier available to report step %s result", task.StepName)
			return
		}

		reportCtx, reportCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer reportCancel()

		if lastErr == nil {
			_, err := applier.Apply(reportCtx, flowstore.CmdCompleteStep, flowstore.CompleteStepRequest{
				WorkflowID: task.WorkflowID,
				RunID:      task.RunID,
				StepName:   task.StepName,
				Output:     output,
			})
			if err != nil {
				log.Printf("[Executor] Error reporting step completion for %s: %v", task.StepName, err)
			}
		} else {
			_, err := applier.Apply(reportCtx, flowstore.CmdFailStep, flowstore.FailStepRequest{
				WorkflowID: task.WorkflowID,
				RunID:      task.RunID,
				StepName:   task.StepName,
				Error:      lastErr.Error(),
				Retryable:  false,
			})
			if err != nil {
				log.Printf("[Executor] Error reporting step failure for %s: %v", task.StepName, err)
			}
		}
	}()
}

// ExecuteCompensation runs a compensating action to roll back completed step effects.
func (e *Executor) ExecuteCompensation(task CompensationTask) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()

		timeout := task.Timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}

		taskCtx, cancel := context.WithTimeout(e.ctx, timeout)
		defer cancel()

		fn, ok := e.registry.Get(task.Activity)
		var (
			output  json.RawMessage
			lastErr error
		)

		if !ok {
			lastErr = fmt.Errorf("compensating action %q not registered", task.Activity)
		} else {
			output, lastErr = fn(taskCtx, task.Input)
		}

		e.mu.RLock()
		applier := e.applier
		e.mu.RUnlock()

		if applier == nil {
			log.Printf("[Executor] Error: no applier available to report compensation for step %s", task.StepName)
			return
		}

		reportCtx, reportCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer reportCancel()

		if lastErr == nil {
			_, err := applier.Apply(reportCtx, flowstore.CmdCompleteCompensationStep, flowstore.CompleteCompensationStepRequest{
				WorkflowID: task.WorkflowID,
				RunID:      task.RunID,
				StepName:   task.StepName,
				Output:     output,
			})
			if err != nil {
				log.Printf("[Executor] Error reporting compensation completion for %s: %v", task.StepName, err)
			}
		} else {
			_, err := applier.Apply(reportCtx, flowstore.CmdFailCompensationStep, flowstore.FailCompensationStepRequest{
				WorkflowID: task.WorkflowID,
				RunID:      task.RunID,
				StepName:   task.StepName,
				Error:      lastErr.Error(),
			})
			if err != nil {
				log.Printf("[Executor] Error reporting compensation failure for %s: %v", task.StepName, err)
			}
		}
	}()
}
