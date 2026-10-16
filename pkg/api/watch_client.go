package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
)

// WatchWorkflow streams a workflow's current state and then every change to
// it, calling fn for each, until the workflow reaches a terminal status, fn
// returns an error, or ctx is done. A dropped stream is resumed transparently.
func (c *Client) WatchWorkflow(ctx context.Context, workflowID string, fn func(*flow.WorkflowInstance) error) error {
	var last uint64
	seen := false
	backoff := 100 * time.Millisecond
	for {
		done, err := c.watchOnce(ctx, workflowID, func(wf *flow.WorkflowInstance) error {
			if seen && wf.Version <= last {
				return nil // replayed on reconnect
			}
			seen, last = true, wf.Version
			return fn(wf)
		})
		if done || ctx.Err() != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 2*time.Second)
	}
}

// WaitWorkflow blocks until the workflow reaches a terminal status and returns
// its final state.
func (c *Client) WaitWorkflow(ctx context.Context, workflowID string) (*flow.WorkflowInstance, error) {
	var final *flow.WorkflowInstance
	err := c.WatchWorkflow(ctx, workflowID, func(wf *flow.WorkflowInstance) error {
		final = wf
		return nil
	})
	if err != nil {
		return nil, err
	}
	return final, nil
}

// watchOnce runs one watch stream. done reports that watching is over (terminal
// status reached, fn failed, or a non-retryable error); otherwise the caller reconnects.
func (c *Client) watchOnce(ctx context.Context, id string, fn func(*flow.WorkflowInstance) error) (done bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/api/v1/workflows/%s/watch", c.baseURL, url.PathEscape(id)), nil)
	if err != nil {
		return true, err
	}
	req.Header.Set("Accept", "text/event-stream")
	// No client timeout: the stream lives as long as the workflow runs.
	resp, err := (&http.Client{Transport: c.httpClient.Transport}).Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return true, flow.ErrWorkflowNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return true, fmt.Errorf("watch %s: unexpected status %d", id, resp.StatusCode)
	}

	terminal := false
	err = readSSE(resp, func(event string, data []byte) error {
		if event != "workflow" {
			return nil
		}
		var wf flow.WorkflowInstance
		if err := json.Unmarshal(data, &wf); err != nil {
			return err
		}
		if err := fn(&wf); err != nil {
			return &stopError{err}
		}
		terminal = wf.IsTerminal()
		return nil
	})
	if se, ok := err.(*stopError); ok {
		return true, se.err
	}
	return terminal, nil
}

type stopError struct{ err error }

func (e *stopError) Error() string { return e.err.Error() }

// readSSE calls fn for every event in a text/event-stream body.
func readSSE(resp *http.Response, fn func(event string, data []byte) error) error {
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64*1024), 64*1024*1024) // a workflow with a long history is one line
	var event string
	var data strings.Builder
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if data.Len() > 0 {
				if err := fn(event, []byte(data.String())); err != nil {
					return err
				}
			}
			event = ""
			data.Reset()
		case strings.HasPrefix(line, ":"): // comment / keep-alive
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	return sc.Err()
}
