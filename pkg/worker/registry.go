package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// ActivityFunc is the user-defined business logic for an activity.
type ActivityFunc func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)

// Registry manages registered activity functions.
type Registry struct {
	mu         sync.RWMutex
	activities map[string]ActivityFunc
}

// NewRegistry creates a new activity registry.
func NewRegistry() *Registry {
	return &Registry{
		activities: make(map[string]ActivityFunc),
	}
}

// Register registers an activity function under a unique name.
func (r *Registry) Register(name string, fn ActivityFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.activities[name] = fn
}

// Get retrieves an activity function by name.
func (r *Registry) Get(name string) (ActivityFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.activities[name]
	return fn, ok
}

// Names lists the registered activities.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.activities))
	for n := range r.activities {
		names = append(names, n)
	}
	return names
}

// RegisterTyped is a helper to register strongly-typed activity handlers.
func RegisterTyped[In any, Out any](r *Registry, name string, fn func(ctx context.Context, input In) (Out, error)) {
	r.Register(name, func(ctx context.Context, rawInput json.RawMessage) (json.RawMessage, error) {
		var in In
		if len(rawInput) > 0 {
			if err := json.Unmarshal(rawInput, &in); err != nil {
				return nil, fmt.Errorf("unmarshal activity input for %s: %w", name, err)
			}
		}
		out, err := fn(ctx, in)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	})
}
