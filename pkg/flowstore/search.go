package flowstore

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
)

// SearchQuery defines structured, full-text, metadata, and range filters for zero-CDC workflow search.
type SearchQuery struct {
	// Query performs case-insensitive substring/token matching across WorkflowID, DefinitionName,
	// Error, ErrorCode, Metadata values, and JSON Input/Output payloads.
	Query string `json:"query,omitempty"`
	// Flow filters by exact workflow definition name.
	Flow string `json:"flow,omitempty"`
	// Statuses filters workflows matching any of the given statuses (e.g. ["FAILED", "COMPENSATED"]).
	Statuses []flow.Status `json:"statuses,omitempty"`
	// ErrorCode filters by exact machine-readable error code.
	ErrorCode string `json:"error_code,omitempty"`
	// Metadata filters workflows that contain all specified key-value pairs.
	Metadata map[string]string `json:"metadata,omitempty"`
	// InputField filters by a dot-separated field path inside the workflow's JSON Input (using flow.Condition semantics).
	InputMatch *flow.Condition `json:"input_match,omitempty"`
	// CreatedAfter filters workflows created at or after this time.
	CreatedAfter *time.Time `json:"created_after,omitempty"`
	// CreatedBefore filters workflows created at or before this time.
	CreatedBefore *time.Time `json:"created_before,omitempty"`
	// MinDurationMS filters workflows whose execution duration is >= MinDurationMS.
	MinDurationMS int64 `json:"min_duration_ms,omitempty"`
	// MaxDurationMS filters workflows whose execution duration is <= MaxDurationMS (> 0).
	MaxDurationMS int64 `json:"max_duration_ms,omitempty"`
	// Limit caps the number of returned hits (default 50).
	Limit int `json:"limit,omitempty"`
	// Offset skips the first Offset matching workflows for pagination.
	Offset int `json:"offset,omitempty"`
}

// SearchFacetCount represents one bucket in a facet aggregation.
type SearchFacetCount struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

// SearchResult holds matching workflow summaries and zero-lag facet aggregations.
type SearchResult struct {
	Total  int                           `json:"total"`
	TookNS int64                         `json:"took_ns"`
	Hits   []flow.TraceSummary           `json:"hits"`
	Facets map[string][]SearchFacetCount `json:"facets"`
}

// SearchWorkflows executes a zero-CDC-lag structured and full-text query with facet aggregations
// directly over the local Raft state machine's workflow store.
func (s *Store) SearchWorkflows(q SearchQuery) SearchResult {
	start := time.Now()

	statusSet := make(map[flow.Status]bool, len(q.Statuses))
	for _, st := range q.Statuses {
		if st != "" {
			statusSet[flow.Status(strings.ToUpper(string(st)))] = true
		}
	}
	tokens := strings.Fields(strings.ToLower(strings.TrimSpace(q.Query)))

	statusFacet := make(map[string]int)
	flowFacet := make(map[string]int)
	errorCodeFacet := make(map[string]int)

	var matched []flow.TraceSummary
	s.mu.RLock()
	for _, wf := range s.workflows {
		if !matchesQuery(wf, q, statusSet, tokens) {
			continue
		}
		sum := wf.Summary()
		matched = append(matched, sum)

		statusFacet[string(wf.Status)]++
		flowFacet[wf.DefinitionName]++
		if wf.ErrorCode != "" {
			errorCodeFacet[wf.ErrorCode]++
		}
	}
	s.mu.RUnlock()

	// Sort newest first (tie-break by WorkflowID)
	sort.Slice(matched, func(i, j int) bool {
		if !matched[i].StartedAt.Equal(matched[j].StartedAt) {
			return matched[i].StartedAt.After(matched[j].StartedAt)
		}
		return matched[i].WorkflowID < matched[j].WorkflowID
	})

	total := len(matched)
	offset := max(q.Offset, 0)
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	var page []flow.TraceSummary
	if offset < total {
		end := min(offset+limit, total)
		page = matched[offset:end]
	} else {
		page = []flow.TraceSummary{}
	}

	return SearchResult{
		Total:  total,
		TookNS: time.Since(start).Nanoseconds(),
		Hits:   page,
		Facets: map[string][]SearchFacetCount{
			"status":     toSortedFacet(statusFacet),
			"flow":       toSortedFacet(flowFacet),
			"error_code": toSortedFacet(errorCodeFacet),
		},
	}
}

func matchesQuery(wf *flow.WorkflowInstance, q SearchQuery, statusSet map[flow.Status]bool, tokens []string) bool {
	if q.Flow != "" && wf.DefinitionName != q.Flow {
		return false
	}
	if len(statusSet) > 0 && !statusSet[wf.Status] {
		return false
	}
	if q.ErrorCode != "" && !strings.EqualFold(wf.ErrorCode, q.ErrorCode) {
		return false
	}
	if len(q.Metadata) > 0 {
		for k, v := range q.Metadata {
			if wf.Metadata == nil || wf.Metadata[k] != v {
				return false
			}
		}
	}
	if q.CreatedAfter != nil && wf.CreatedAt.Before(*q.CreatedAfter) {
		return false
	}
	if q.CreatedBefore != nil && wf.CreatedAt.After(*q.CreatedBefore) {
		return false
	}
	dur := wf.Summary().DurationMS
	if q.MinDurationMS > 0 && dur < q.MinDurationMS {
		return false
	}
	if q.MaxDurationMS > 0 && dur > q.MaxDurationMS {
		return false
	}
	if q.InputMatch != nil {
		cond := *q.InputMatch
		if cond.Source == "" {
			cond.Source = "input"
		}
		if !cond.Evaluate(wf) {
			return false
		}
	}
	for _, tok := range tokens {
		if !matchesToken(wf, tok) {
			return false
		}
	}
	return true
}

func matchesToken(wf *flow.WorkflowInstance, tok string) bool {
	// Support inline key:value syntax in query string (e.g. "channel:web" or "status:failed" or "flow:order")
	if k, v, hasColon := strings.Cut(tok, ":"); hasColon && k != "" && v != "" {
		switch k {
		case "status":
			return strings.EqualFold(string(wf.Status), v)
		case "flow":
			return strings.Contains(strings.ToLower(wf.DefinitionName), v)
		case "id":
			return strings.Contains(strings.ToLower(wf.WorkflowID), v)
		case "code", "error_code":
			return strings.EqualFold(wf.ErrorCode, v)
		default:
			if wf.Metadata != nil {
				for mk, mv := range wf.Metadata {
					if strings.EqualFold(mk, k) && strings.Contains(strings.ToLower(mv), v) {
						return true
					}
				}
			}
			// Also check top-level/nested JSON input field
			cond := flow.Condition{
				Source: "input",
				Field:  k,
				Op:     "eq",
				Value:  json.RawMessage(v),
			}
			if cond.Evaluate(wf) {
				return true
			}
		}
	}

	if strings.Contains(strings.ToLower(wf.WorkflowID), tok) ||
		strings.Contains(strings.ToLower(wf.DefinitionName), tok) ||
		strings.Contains(strings.ToLower(string(wf.Status)), tok) ||
		strings.Contains(strings.ToLower(wf.Error), tok) ||
		strings.Contains(strings.ToLower(wf.ErrorCode), tok) {
		return true
	}
	for k, v := range wf.Metadata {
		if strings.Contains(strings.ToLower(k), tok) || strings.Contains(strings.ToLower(v), tok) {
			return true
		}
	}
	if len(wf.Input) > 0 && bytes.Contains(bytes.ToLower(wf.Input), []byte(tok)) {
		return true
	}
	if len(wf.Output) > 0 && bytes.Contains(bytes.ToLower(wf.Output), []byte(tok)) {
		return true
	}
	return false
}

func toSortedFacet(counts map[string]int) []SearchFacetCount {
	out := make([]SearchFacetCount, 0, len(counts))
	for k, c := range counts {
		out = append(out, SearchFacetCount{Value: k, Count: c})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Value < out[j].Value
	})
	return out
}
