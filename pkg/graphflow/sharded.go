package graphflow

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/worker"
)

// ShardedCoordinator partitions workflow state across N independent Raft Graph shards
// by hashing WorkflowID (Multi-Raft), multiplying write throughput while preserving
// per-workflow strict quorum consistency and exactly-once dispatch semantics.
type ShardedCoordinator struct {
	shards []*Coordinator
}

// NewShardedCoordinator wraps a non-empty slice of independent Coordinator shards.
func NewShardedCoordinator(shards []*Coordinator) (*ShardedCoordinator, error) {
	if len(shards) == 0 {
		return nil, fmt.Errorf("at least one coordinator shard is required")
	}
	for i, s := range shards {
		if s == nil {
			return nil, fmt.Errorf("shard %d is nil", i)
		}
	}
	return &ShardedCoordinator{shards: shards}, nil
}

// NewEmbeddedShardedCoordinator boots numShards independent embedded Raft Graph shards
// sharing the given activity registry.
func NewEmbeddedShardedCoordinator(numShards int, reg *worker.Registry) (*ShardedCoordinator, error) {
	if numShards <= 0 {
		numShards = 1
	}
	if reg == nil {
		reg = worker.NewRegistry()
	}
	shards := make([]*Coordinator, 0, numShards)
	for i := 0; i < numShards; i++ {
		c, err := NewEmbeddedCoordinator(reg)
		if err != nil {
			for _, prev := range shards {
				_ = prev.Close()
			}
			return nil, fmt.Errorf("create shard %d: %w", i, err)
		}
		shards = append(shards, c)
	}
	return NewShardedCoordinator(shards)
}

// NumShards returns the number of Raft Graph shards.
func (sc *ShardedCoordinator) NumShards() int { return len(sc.shards) }

// Shard returns the shard at index i.
func (sc *ShardedCoordinator) Shard(i int) *Coordinator { return sc.shards[i] }

// ShardFor returns the Coordinator shard responsible for workflowID using FNV-1a hashing.
func (sc *ShardedCoordinator) ShardFor(workflowID string) *Coordinator {
	h := fnv.New64a()
	_, _ = h.Write([]byte(workflowID))
	idx := int(h.Sum64() % uint64(len(sc.shards)))
	return sc.shards[idx]
}

// RegisterFlow registers a workflow definition across all shards.
func (sc *ShardedCoordinator) RegisterFlow(def flow.WorkflowDefinition) error {
	for _, s := range sc.shards {
		if err := s.RegisterFlow(def); err != nil {
			return err
		}
	}
	return nil
}

// StartWorkflow routes the workflow to its deterministic shard and starts it.
func (sc *ShardedCoordinator) StartWorkflow(ctx context.Context, req flowstore.StartWorkflowRequest) (*flow.WorkflowInstance, error) {
	_ = sc.RegisterFlow(req.Definition)
	return sc.ShardFor(req.WorkflowID).StartWorkflow(ctx, req)
}

// GetWorkflow retrieves a workflow from its deterministic shard.
func (sc *ShardedCoordinator) GetWorkflow(workflowID string) (*flow.WorkflowInstance, bool) {
	return sc.ShardFor(workflowID).GetWorkflow(workflowID)
}

// SignalWorkflow routes an external signal to the workflow's deterministic shard.
func (sc *ShardedCoordinator) SignalWorkflow(ctx context.Context, workflowID, signalName string, payload json.RawMessage) error {
	return sc.ShardFor(workflowID).SignalWorkflow(ctx, workflowID, signalName, payload)
}

// CancelWorkflow routes a cancellation request to the workflow's deterministic shard.
func (sc *ShardedCoordinator) CancelWorkflow(ctx context.Context, workflowID, reason string) error {
	return sc.ShardFor(workflowID).CancelWorkflow(ctx, workflowID, reason)
}

// ReplayStep routes a step replay request to the workflow's deterministic shard.
func (sc *ShardedCoordinator) ReplayStep(ctx context.Context, req flowstore.ReplayStepRequest) (*flow.WorkflowInstance, error) {
	return sc.ShardFor(req.WorkflowID).ReplayStep(ctx, req)
}

// ListWorkflows aggregates active/hot workflows across all shards concurrently.
func (sc *ShardedCoordinator) ListWorkflows() []*flow.WorkflowInstance {
	var out []*flow.WorkflowInstance
	for _, s := range sc.shards {
		out = append(out, s.ListWorkflows()...)
	}
	return out
}

// SearchWorkflows executes a search query across all shards and merges hits and facet counts.
func (sc *ShardedCoordinator) SearchWorkflows(q flowstore.SearchQuery) flowstore.SearchResult {
	merged := flowstore.SearchResult{
		Hits:   []flow.TraceSummary{},
		Facets: map[string][]flowstore.SearchFacetCount{},
	}
	facetMaps := map[string]map[string]int{
		"status":     {},
		"flow":       {},
		"error_code": {},
	}
	shardQuery := q
	shardQuery.Offset = 0
	if q.Limit > 0 {
		shardQuery.Limit = q.Offset + q.Limit
	}
	for _, s := range sc.shards {
		res := s.SearchWorkflows(shardQuery)
		merged.Total += res.Total
		merged.TookNS += res.TookNS
		merged.Hits = append(merged.Hits, res.Hits...)
		for fk, buckets := range res.Facets {
			if facetMaps[fk] == nil {
				facetMaps[fk] = map[string]int{}
			}
			for _, b := range buckets {
				facetMaps[fk][b.Value] += b.Count
			}
		}
	}
	for fk, counts := range facetMaps {
		var list []flowstore.SearchFacetCount
		for val, cnt := range counts {
			list = append(list, flowstore.SearchFacetCount{Value: val, Count: cnt})
		}
		merged.Facets[fk] = list
	}
	return merged
}

// CompactWorkflows compacts terminal workflows across all shards.
func (sc *ShardedCoordinator) CompactWorkflows(ctx context.Context, req flowstore.CompactWorkflowsRequest) (flowstore.CompactWorkflowsResponse, error) {
	var total flowstore.CompactWorkflowsResponse
	for _, s := range sc.shards {
		resp, err := s.CompactWorkflows(ctx, req)
		if err != nil {
			return total, err
		}
		total.Compacted += resp.Compacted
		total.WorkflowIDs = append(total.WorkflowIDs, resp.WorkflowIDs...)
	}
	return total, nil
}

// Watch fans in workflow change notifications across all shards.
func (sc *ShardedCoordinator) Watch(match func(*flow.WorkflowInstance) bool, buffer int) (<-chan *flow.WorkflowInstance, func()) {
	out := make(chan *flow.WorkflowInstance, max(buffer, 1))
	cancels := make([]func(), 0, len(sc.shards))
	var wg sync.WaitGroup

	for _, s := range sc.shards {
		ch, cancel := s.Watch(match, buffer)
		cancels = append(cancels, cancel)
		wg.Add(1)
		go func(c <-chan *flow.WorkflowInstance) {
			defer wg.Done()
			for wf := range c {
				select {
				case out <- wf:
				default:
				}
			}
		}(ch)
	}

	var once sync.Once
	cancelAll := func() {
		once.Do(func() {
			for _, c := range cancels {
				c()
			}
			wg.Wait()
			close(out)
		})
	}
	return out, cancelAll
}

// Close stops all shards.
func (sc *ShardedCoordinator) Close() error {
	var firstErr error
	for _, s := range sc.shards {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
