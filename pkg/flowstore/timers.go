package flowstore

import (
	"container/heap"
	"time"
)

type timerKey struct {
	workflowID string
	stepName   string
}

type timerEntry struct {
	key    timerKey
	runID  string
	wakeAt time.Time
	index  int
}

// timerHeap is an indexed min-heap of step wake deadlines.
// Operations (upsert, remove, pop due) run in O(log N) instead of scanning all workflows.
type timerHeap struct {
	entries []*timerEntry
	byKey   map[timerKey]*timerEntry
}

func newTimerHeap() *timerHeap {
	return &timerHeap{
		byKey: make(map[timerKey]*timerEntry),
	}
}

func (h *timerHeap) Len() int { return len(h.entries) }

func (h *timerHeap) Less(i, j int) bool {
	if !h.entries[i].wakeAt.Equal(h.entries[j].wakeAt) {
		return h.entries[i].wakeAt.Before(h.entries[j].wakeAt)
	}
	if h.entries[i].key.workflowID != h.entries[j].key.workflowID {
		return h.entries[i].key.workflowID < h.entries[j].key.workflowID
	}
	return h.entries[i].key.stepName < h.entries[j].key.stepName
}

func (h *timerHeap) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
	h.entries[i].index = i
	h.entries[j].index = j
}

func (h *timerHeap) Push(x any) {
	e := x.(*timerEntry)
	e.index = len(h.entries)
	h.entries = append(h.entries, e)
}

func (h *timerHeap) Pop() any {
	n := len(h.entries)
	e := h.entries[n-1]
	h.entries[n-1] = nil
	e.index = -1
	h.entries = h.entries[:n-1]
	return e
}

// set inserts or updates a timer deadline for (workflowID, stepName).
// If wakeAt is nil, any existing timer for that step is removed.
func (h *timerHeap) set(workflowID, runID, stepName string, wakeAt *time.Time) {
	if h.byKey == nil {
		h.byKey = make(map[timerKey]*timerEntry)
	}
	k := timerKey{workflowID: workflowID, stepName: stepName}
	if wakeAt == nil {
		h.remove(workflowID, stepName)
		return
	}
	if cur, ok := h.byKey[k]; ok {
		cur.runID = runID
		cur.wakeAt = *wakeAt
		heap.Fix(h, cur.index)
		return
	}
	e := &timerEntry{
		key:    k,
		runID:  runID,
		wakeAt: *wakeAt,
	}
	h.byKey[k] = e
	heap.Push(h, e)
}

// remove deletes a step's timer from the heap if present.
func (h *timerHeap) remove(workflowID, stepName string) {
	if h.byKey == nil {
		return
	}
	k := timerKey{workflowID: workflowID, stepName: stepName}
	if cur, ok := h.byKey[k]; ok {
		delete(h.byKey, k)
		if cur.index >= 0 && cur.index < len(h.entries) {
			heap.Remove(h, cur.index)
		}
	}
}

// removeWorkflow deletes all timers belonging to workflowID.
func (h *timerHeap) removeWorkflow(workflowID string) {
	if h.byKey == nil {
		return
	}
	for k, cur := range h.byKey {
		if k.workflowID == workflowID {
			delete(h.byKey, k)
			if cur.index >= 0 && cur.index < len(h.entries) {
				heap.Remove(h, cur.index)
			}
		}
	}
}

// due returns all timer entries with wakeAt <= now without mutating the heap.
// It traverses only the portion of the heap whose wakeAt <= now.
func (h *timerHeap) due(now time.Time) []TimerRef {
	if len(h.entries) == 0 {
		return nil
	}
	var out []TimerRef
	var visit func(idx int)
	visit = func(idx int) {
		if idx >= len(h.entries) {
			return
		}
		e := h.entries[idx]
		if e.wakeAt.After(now) {
			return
		}
		out = append(out, TimerRef{
			WorkflowID: e.key.workflowID,
			RunID:      e.runID,
			StepName:   e.key.stepName,
		})
		left := 2*idx + 1
		right := 2*idx + 2
		visit(left)
		visit(right)
	}
	visit(0)
	return out
}
