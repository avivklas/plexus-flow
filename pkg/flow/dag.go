package flow

import (
	"fmt"
)

// Validate checks that the workflow definition is well-formed:
// - Non-empty name
// - At least one step
// - Unique step names
// - All dependencies exist and no self-dependencies
// - No cycles (must be a valid Directed Acyclic Graph)
func (d *WorkflowDefinition) Validate() error {
	if d.Name == "" {
		return fmt.Errorf("workflow definition name cannot be empty")
	}
	if len(d.Steps) == 0 {
		return fmt.Errorf("workflow definition must have at least one step")
	}

	names := make(map[string]bool, len(d.Steps))
	for _, s := range d.Steps {
		if s.Name == "" {
			return fmt.Errorf("step name cannot be empty")
		}
		if err := s.validateKind(); err != nil {
			return err
		}
		if names[s.Name] {
			return fmt.Errorf("duplicate step name %q", s.Name)
		}
		names[s.Name] = true
	}

	// Check if this is a pipeline with explicit dependencies or implicit sequential
	hasExplicitDeps := false
	for _, s := range d.Steps {
		if len(s.DependsOn) > 0 {
			hasExplicitDeps = true
			break
		}
	}

	if hasExplicitDeps {
		// Verify all dependencies exist and no self-dependency
		for _, s := range d.Steps {
			for _, dep := range s.DependsOn {
				if dep == s.Name {
					return fmt.Errorf("step %q cannot depend on itself", s.Name)
				}
				if !names[dep] {
					return fmt.Errorf("step %q depends on non-existent step %q", s.Name, dep)
				}
			}
		}

		// Cycle detection using Kahn's algorithm
		inDegree := make(map[string]int, len(d.Steps))
		adj := make(map[string][]string, len(d.Steps))

		for _, s := range d.Steps {
			inDegree[s.Name] = len(s.DependsOn)
			for _, dep := range s.DependsOn {
				adj[dep] = append(adj[dep], s.Name)
			}
		}

		var queue []string
		for _, s := range d.Steps {
			if inDegree[s.Name] == 0 {
				queue = append(queue, s.Name)
			}
		}

		visitedCount := 0
		for len(queue) > 0 {
			u := queue[0]
			queue = queue[1:]
			visitedCount++

			for _, v := range adj[u] {
				inDegree[v]--
				if inDegree[v] == 0 {
					queue = append(queue, v)
				}
			}
		}

		if visitedCount != len(d.Steps) {
			return fmt.Errorf("cycle detected in workflow steps")
		}
	}

	return nil
}

// Normalized returns a copy of the workflow definition where implicit sequential dependencies
// are made explicit if no steps defined any dependencies.
func (d WorkflowDefinition) Normalized() WorkflowDefinition {
	hasExplicitDeps := false
	for _, s := range d.Steps {
		if len(s.DependsOn) > 0 {
			hasExplicitDeps = true
			break
		}
	}

	norm := WorkflowDefinition{
		Name:  d.Name,
		Steps: make([]StepDefinition, len(d.Steps)),
	}

	for i, s := range d.Steps {
		copyStep := s
		if !hasExplicitDeps && i > 0 {
			copyStep.DependsOn = []string{d.Steps[i-1].Name}
		}
		norm.Steps[i] = copyStep
	}

	return norm
}

// TopologicalOrder returns the step names ordered such that every dependency appears before its dependent.
func (d *WorkflowDefinition) TopologicalOrder() ([]string, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}

	norm := d.Normalized()
	inDegree := make(map[string]int, len(norm.Steps))
	adj := make(map[string][]string, len(norm.Steps))

	for _, s := range norm.Steps {
		inDegree[s.Name] = len(s.DependsOn)
		for _, dep := range s.DependsOn {
			adj[dep] = append(adj[dep], s.Name)
		}
	}

	var queue []string
	for _, s := range norm.Steps {
		if inDegree[s.Name] == 0 {
			queue = append(queue, s.Name)
		}
	}

	var order []string
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		order = append(order, u)

		for _, v := range adj[u] {
			inDegree[v]--
			if inDegree[v] == 0 {
				queue = append(queue, v)
			}
		}
	}

	if len(order) != len(norm.Steps) {
		return nil, fmt.Errorf("cycle detected in workflow steps")
	}

	return order, nil
}

func (s StepDefinition) validateKind() error {
	switch s.Kind {
	case "", KindActivity:
		if s.Activity == "" {
			return fmt.Errorf("step %q must specify an activity", s.Name)
		}
	case KindWaitSignal:
		if s.Signal == "" {
			return fmt.Errorf("step %q waits for a signal and must name it", s.Name)
		}
		if s.CompensatingAction != "" {
			return fmt.Errorf("step %q: a wait_signal step cannot have a compensating action", s.Name)
		}
	case KindSleep:
		if s.Delay <= 0 {
			return fmt.Errorf("step %q sleeps and needs a positive delay", s.Name)
		}
		if s.CompensatingAction != "" {
			return fmt.Errorf("step %q: a sleep step cannot have a compensating action", s.Name)
		}
	default:
		return fmt.Errorf("step %q has unknown kind %q", s.Name, s.Kind)
	}
	return nil
}
