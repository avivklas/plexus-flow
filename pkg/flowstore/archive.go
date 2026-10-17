package flowstore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/avivklas/plexus-flow/pkg/flow"
)

// Archiver persists terminal workflows that have been compacted out of hot memory.
type Archiver interface {
	ArchiveWorkflows(workflows []*flow.WorkflowInstance) error
	LoadWorkflow(workflowID string) (*flow.WorkflowInstance, bool, error)
}

// MemoryArchiver stores compacted workflows in a separate cold memory map (useful for tests).
type MemoryArchiver struct {
	mu    sync.RWMutex
	store map[string]*flow.WorkflowInstance
}

// NewMemoryArchiver creates an in-memory cold workflow archive.
func NewMemoryArchiver() *MemoryArchiver {
	return &MemoryArchiver{
		store: make(map[string]*flow.WorkflowInstance),
	}
}

func (a *MemoryArchiver) ArchiveWorkflows(workflows []*flow.WorkflowInstance) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, wf := range workflows {
		if wf != nil {
			a.store[wf.WorkflowID] = wf.Clone()
		}
	}
	return nil
}

func (a *MemoryArchiver) LoadWorkflow(workflowID string) (*flow.WorkflowInstance, bool, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	wf, ok := a.store[workflowID]
	if !ok {
		return nil, false, nil
	}
	return wf.Clone(), true, nil
}

// Count returns the number of archived workflows.
func (a *MemoryArchiver) Count() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.store)
}

// FileArchiver persists compacted terminal workflows as individual JSON files on disk
// so they do not bloat the in-memory Raft state machine or Raft snapshots.
type FileArchiver struct {
	mu  sync.RWMutex
	dir string
}

// NewFileArchiver creates a directory-backed cold workflow archiver.
func NewFileArchiver(dir string) (*FileArchiver, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve archive dir: %w", err)
	}
	if err := os.MkdirAll(absDir, 0o755); err != nil {
		return nil, fmt.Errorf("create archive dir: %w", err)
	}
	return &FileArchiver{dir: absDir}, nil
}

func (a *FileArchiver) pathFor(workflowID string) (string, bool) {
	if workflowID == "" || strings.Contains(workflowID, "/") || strings.Contains(workflowID, "\\") || strings.Contains(workflowID, "..") {
		return "", false
	}
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, workflowID)
	absPath, err := filepath.Abs(filepath.Join(a.dir, safe+".json"))
	if err != nil || !strings.HasPrefix(absPath, a.dir+string(filepath.Separator)) {
		return "", false
	}
	return absPath, true
}

func (a *FileArchiver) ArchiveWorkflows(workflows []*flow.WorkflowInstance) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, wf := range workflows {
		if wf == nil {
			continue
		}
		p, ok := a.pathFor(wf.WorkflowID)
		if !ok {
			return fmt.Errorf("invalid workflow ID for archive: %q", wf.WorkflowID)
		}
		data, err := json.Marshal(wf)
		if err != nil {
			return err
		}
		tmp := p + ".tmp"
		if err := os.WriteFile(tmp, data, 0o644); err != nil {
			return err
		}
		if err := os.Rename(tmp, p); err != nil {
			return err
		}
	}
	return nil
}

func (a *FileArchiver) LoadWorkflow(workflowID string) (*flow.WorkflowInstance, bool, error) {
	if workflowID == "" || strings.Contains(workflowID, "/") || strings.Contains(workflowID, "\\") || strings.Contains(workflowID, "..") {
		return nil, false, nil
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	p, ok := a.pathFor(workflowID)
	if !ok {
		return nil, false, nil
	}
	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	var wf flow.WorkflowInstance
	if err := json.Unmarshal(data, &wf); err != nil {
		return nil, false, err
	}
	return &wf, true, nil
}
