// Command mlrelease showcases plexus-flow with a realistic ML model release pipeline:
//
//	ingest-data ──► train-xgboost ──┐
//	            └─► train-transformer ┴─► evaluate ──► await-approval ──► publish-model
//
// What it demonstrates:
//   - fork-join parallelism (two trainers run concurrently, evaluate waits for both)
//   - retries (ingest-data is flaky and succeeds on a later attempt)
//   - a human-in-the-loop gate driven by a workflow signal
//   - automatic LIFO saga rollback when a reviewer rejects the model
//
// Run:
//
//	go run ./examples/mlrelease --decision approve
//	go run ./examples/mlrelease --decision reject
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/graphflow"
	"github.com/avivklas/plexus-flow/pkg/worker"
)

const (
	reset  = "\033[0m"
	red    = "\033[31m"
	green  = "\033[32m"
	yellow = "\033[33m"
	blue   = "\033[34m"
	purple = "\033[35m"
	cyan   = "\033[36m"
	bold   = "\033[1m"
)

var start = time.Now()

func logf(color, tag, format string, args ...any) {
	fmt.Printf("%s%6.2fs %-12s%s %s\n", color, time.Since(start).Seconds(), tag, reset, fmt.Sprintf(format, args...))
}

func main() {
	decision := flag.String("decision", "approve", "reviewer decision: approve or reject")
	reviewDelay := flag.Duration("review-delay", 1500*time.Millisecond, "how long the human reviewer takes")
	flag.Parse()

	if *decision != "approve" && *decision != "reject" {
		fmt.Fprintln(os.Stderr, "--decision must be approve or reject")
		os.Exit(2)
	}

	reg := worker.NewRegistry()
	coord, err := graphflow.NewEmbeddedCoordinator(reg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "init:", err)
		os.Exit(1)
	}
	defer coord.Close()

	registerActivities(reg, coord)

	fmt.Printf("%s%sPlexus-Flow showcase: ML model release pipeline%s\n\n", bold, cyan, reset)

	const workflowID = "release-fraud-model-v7"
	input, _ := json.Marshal(map[string]any{"workflow_id": workflowID, "model": "fraud-detector", "version": 7})

	wf, err := coord.StartWorkflow(context.Background(), flowstore.StartWorkflowRequest{
		WorkflowID: workflowID,
		Definition: definition(),
		Input:      input,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "start:", err)
		os.Exit(1)
	}
	logf(cyan, "WORKFLOW", "started %s (run %s)", wf.WorkflowID, wf.RunID)

	// The "human reviewer": waits, then signals the workflow.
	go func() {
		// Wait until the approval gate is actually open.
		for {
			if inst, ok := coord.GetWorkflow(workflowID); ok && inst.Steps["await-approval"].Status == flow.StepStatusRunning {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		time.Sleep(*reviewDelay)
		logf(yellow, "REVIEWER", "submitting decision: %s", *decision)
		_ = coord.SignalWorkflow(context.Background(), workflowID, *decision, json.RawMessage(`{"reviewer":"alice"}`))
	}()

	// Wait for a terminal state.
	deadline := time.Now().Add(60 * time.Second)
	var final *flow.WorkflowInstance
	for time.Now().Before(deadline) {
		if inst, ok := coord.GetWorkflow(workflowID); ok && inst.IsTerminal() {
			final = inst
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if final == nil {
		fmt.Fprintln(os.Stderr, "timed out waiting for workflow")
		os.Exit(1)
	}

	// Let the final compensation/complete events settle in history.
	time.Sleep(100 * time.Millisecond)
	final, _ = coord.GetWorkflow(workflowID)

	fmt.Printf("\n%s%sAudit trail (replicated through Raft):%s\n", bold, cyan, reset)
	for _, ev := range final.History {
		step := ev.StepName
		if step == "" {
			step = "-"
		}
		fmt.Printf("  %s  %-28s %-18s %s\n", ev.Timestamp.Format("15:04:05.000"), ev.Type, step, ev.Message)
	}

	fmt.Println()
	switch final.Status {
	case flow.StatusCompleted:
		logf(green, "RESULT", "%sCOMPLETED%s: model published, each step committed exactly once despite retries", bold, reset)
	case flow.StatusCompensated:
		logf(purple, "RESULT", "%sCOMPENSATED%s: rejected model rolled back in reverse order, nothing left behind", bold, reset)
	default:
		logf(red, "RESULT", "status=%s error=%s", final.Status, final.Error)
		os.Exit(1)
	}
}

func definition() flow.WorkflowDefinition {
	return flow.WorkflowDefinition{
		Name: "ml-model-release",
		Steps: []flow.StepDefinition{
			{Name: "ingest-data", Activity: "ingest-data", CompensatingAction: "purge-staging", Retries: 3},
			{Name: "train-xgboost", Activity: "train-xgboost", CompensatingAction: "delete-artifacts", DependsOn: []string{"ingest-data"}},
			{Name: "train-transformer", Activity: "train-transformer", CompensatingAction: "delete-artifacts", DependsOn: []string{"ingest-data"}},
			{Name: "evaluate", Activity: "evaluate", DependsOn: []string{"train-xgboost", "train-transformer"}},
			{Name: "await-approval", Activity: "await-approval", DependsOn: []string{"evaluate"}, Timeout: 2 * time.Minute},
			{Name: "publish-model", Activity: "publish-model", CompensatingAction: "unpublish-model", DependsOn: []string{"await-approval"}},
		},
	}
}

func registerActivities(reg *worker.Registry, coord *graphflow.Coordinator) {
	var ingestAttempts atomic.Int32

	reg.Register("ingest-data", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		n := ingestAttempts.Add(1)
		logf(blue, "ACTIVITY", "ingest-data: pulling 2.4M rows from warehouse (attempt %d)", n)
		time.Sleep(200 * time.Millisecond)
		if n < 3 {
			logf(red, "ACTIVITY", "ingest-data: warehouse timeout, will retry")
			return nil, errors.New("warehouse timeout")
		}
		logf(green, "ACTIVITY", "ingest-data: staged 2.4M rows")
		return json.RawMessage(`{"rows":2400000,"staging":"s3://ml-staging/run-7"}`), nil
	})
	reg.Register("purge-staging", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		logf(purple, "ROLLBACK", "purge-staging: deleting s3://ml-staging/run-7")
		time.Sleep(100 * time.Millisecond)
		return json.RawMessage(`{"purged":true}`), nil
	})

	train := func(name string, d time.Duration, acc float64) worker.ActivityFunc {
		return func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
			logf(blue, "ACTIVITY", "%s: training started", name)
			time.Sleep(d)
			logf(green, "ACTIVITY", "%s: done, accuracy=%.3f", name, acc)
			return json.RawMessage(fmt.Sprintf(`{"model":%q,"accuracy":%.3f}`, name, acc)), nil
		}
	}
	reg.Register("train-xgboost", train("xgboost", 600*time.Millisecond, 0.947))
	reg.Register("train-transformer", train("transformer", 900*time.Millisecond, 0.962))
	reg.Register("delete-artifacts", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		logf(purple, "ROLLBACK", "delete-artifacts: removing trained model %s", strings.TrimSpace(string(in)))
		time.Sleep(100 * time.Millisecond)
		return json.RawMessage(`{"deleted":true}`), nil
	})

	reg.Register("evaluate", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		logf(blue, "ACTIVITY", "evaluate: comparing candidates on holdout set")
		time.Sleep(300 * time.Millisecond)
		logf(green, "ACTIVITY", "evaluate: champion = transformer (0.962)")
		return json.RawMessage(`{"champion":"transformer","accuracy":0.962}`), nil
	})

	reg.Register("await-approval", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		var meta struct {
			WorkflowID string `json:"workflow_id"`
		}
		_ = json.Unmarshal(in, &meta)
		logf(yellow, "APPROVAL", "waiting for a human reviewer signal (approve/reject)...")
		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
			history, _ := coord.FlowStore().GetHistory(meta.WorkflowID)
			for _, ev := range history {
				if ev.Type != "WORKFLOW_SIGNALED" {
					continue
				}
				switch {
				case strings.HasSuffix(ev.Message, ": approve"):
					logf(green, "APPROVAL", "approved")
					return ev.Payload, nil
				case strings.HasSuffix(ev.Message, ": reject"):
					logf(red, "APPROVAL", "rejected")
					return nil, errors.New("rejected by reviewer")
				}
			}
		}
	})

	reg.Register("publish-model", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		logf(blue, "ACTIVITY", "publish-model: promoting to production registry")
		time.Sleep(200 * time.Millisecond)
		logf(green, "ACTIVITY", "publish-model: fraud-detector v7 is live")
		return json.RawMessage(`{"published":true}`), nil
	})
	reg.Register("unpublish-model", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		logf(purple, "ROLLBACK", "unpublish-model: reverting registry")
		return json.RawMessage(`{"unpublished":true}`), nil
	})
}
