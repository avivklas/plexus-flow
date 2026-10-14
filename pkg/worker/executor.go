package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus/pkg/store"
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

		output, lastErr := e.runWithRetries(taskCtx, task.Activity, task.Input, task.Retries)

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
				Code:       CodeOf(lastErr),
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

		output, lastErr := e.runWithRetries(taskCtx, task.Activity, task.Input, task.Retries)

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
				Code:       CodeOf(lastErr),
			})
			if err != nil {
				log.Printf("[Executor] Error reporting compensation failure for %s: %v", task.StepName, err)
			}
		}
	}()
}

// runWithRetries calls the activity up to retries+1 times with a small linear
// backoff. A NonRetryable error ends the attempts at once.
func (e *Executor) runWithRetries(ctx context.Context, activity string, input json.RawMessage, retries int) (json.RawMessage, error) {
	fn, ok := e.registry.Get(activity)
	if !ok {
		return nil, fmt.Errorf("activity function %q not registered", activity)
	}
	var lastErr error
	for attempt := 1; attempt <= retries+1; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		out, err := fn(ctx, input)
		if err == nil {
			return out, nil
		}
		lastErr = err
		if IsNonRetryable(err) || attempt == retries+1 {
			break
		}
		select {
		case <-time.After(time.Duration(attempt*15) * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
}
