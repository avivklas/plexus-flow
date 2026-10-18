package flow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// Condition defines a declarative predicate evaluated before a step is dispatched.
// If a step's Condition evaluates to false once all of its dependencies have completed
// (or been skipped), the step is automatically marked StepStatusSkipped.
type Condition struct {
	// Source selects which JSON payload to inspect:
	//   - "" or "input": the workflow's initial Input payload
	//   - "metadata": the workflow's Metadata map[string]string
	//   - "<step-name>": the Output payload of a completed dependency step (or its status if Field == "$status")
	Source string `json:"source,omitempty"`
	// Field is a dot-separated JSON path (e.g. "payment.tier" or "approved").
	// If Source is a step name and Field is "$status", it inspects the dependency's StepStatus ("COMPLETED" or "SKIPPED").
	Field string `json:"field"`
	// Op is the comparison operator:
	//   - "eq" / "==" / "" (default): Field equals Value
	//   - "ne" / "!=": Field does not equal Value
	//   - "gt" / ">": numeric Field > Value
	//   - "gte" / ">=": numeric Field >= Value
	//   - "lt" / "<": numeric Field < Value
	//   - "lte" / "<=": numeric Field <= Value
	//   - "in": Field equals one of the elements in Value (JSON array)
	//   - "exists": Field is present and non-null in the JSON object
	//   - "not_exists": Field is absent or null in the JSON object
	Op string `json:"op,omitempty"`
	// Value is the JSON literal to compare against (e.g. `"gold"`, `true`, `100`, `["us","eu"]`).
	Value json.RawMessage `json:"value,omitempty"`
}

// Validate checks that the Condition has valid syntax and operators.
func (c *Condition) Validate(stepName string, deps []string) error {
	if c == nil {
		return nil
	}
	if strings.TrimSpace(c.Field) == "" {
		return fmt.Errorf("step %q condition: field cannot be empty", stepName)
	}
	switch c.Op {
	case "", "eq", "==", "ne", "!=", "gt", ">", "gte", ">=", "lt", "<", "lte", "<=", "in", "exists", "not_exists":
	default:
		return fmt.Errorf("step %q condition: unsupported operator %q", stepName, c.Op)
	}
	if c.Source != "" && c.Source != "input" && c.Source != "metadata" {
		found := false
		for _, d := range deps {
			if d == c.Source {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("step %q condition references source step %q which is not in depends_on", stepName, c.Source)
		}
	}
	return nil
}

// Evaluate checks whether the condition holds for the given workflow instance.
func (c *Condition) Evaluate(wf *WorkflowInstance) bool {
	if c == nil {
		return true
	}

	var val any
	var exists bool

	switch c.Source {
	case "", "input":
		val, exists = extractJSONField(wf.Input, c.Field)
	case "metadata":
		if wf.Metadata != nil {
			v, ok := wf.Metadata[c.Field]
			val, exists = v, ok
		}
	default:
		depStep, ok := wf.Steps[c.Source]
		if !ok {
			return false
		}
		if c.Field == "$status" {
			val, exists = string(depStep.Status), true
		} else {
			if depStep.Status != StepStatusCompleted {
				// Dependency was skipped or has no output
				exists = false
			} else {
				val, exists = extractJSONField(depStep.Output, c.Field)
			}
		}
	}

	op := c.Op
	if op == "" || op == "==" {
		op = "eq"
	} else if op == "!=" {
		op = "ne"
	} else if op == ">" {
		op = "gt"
	} else if op == ">=" {
		op = "gte"
	} else if op == "<" {
		op = "lt"
	} else if op == "<=" {
		op = "lte"
	}

	switch op {
	case "exists":
		return exists && val != nil
	case "not_exists":
		return !exists || val == nil
	}

	var target any
	if len(c.Value) > 0 {
		dec := json.NewDecoder(bytes.NewReader(c.Value))
		dec.UseNumber()
		if err := dec.Decode(&target); err != nil {
			// Fallback: treat raw unquoted string as string value
			target = strings.TrimSpace(string(c.Value))
		}
	}

	if !exists {
		if op == "ne" {
			return target != nil
		}
		return false
	}

	switch op {
	case "eq":
		return compareEqual(val, target)
	case "ne":
		return !compareEqual(val, target)
	case "gt", "gte", "lt", "lte":
		lhs, ok1 := toFloat64(val)
		rhs, ok2 := toFloat64(target)
		if !ok1 || !ok2 {
			return false
		}
		switch op {
		case "gt":
			return lhs > rhs
		case "gte":
			return lhs >= rhs
		case "lt":
			return lhs < rhs
		case "lte":
			return lhs <= rhs
		}
	case "in":
		arr, ok := target.([]any)
		if !ok {
			return false
		}
		for _, item := range arr {
			if compareEqual(val, item) {
				return true
			}
		}
		return false
	}

	return false
}

func extractJSONField(raw json.RawMessage, path string) (any, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var root any
	if err := dec.Decode(&root); err != nil {
		return nil, false
	}

	parts := strings.Split(path, ".")
	curr := root
	for _, p := range parts {
		m, ok := curr.(map[string]any)
		if !ok {
			return nil, false
		}
		next, exists := m[p]
		if !exists {
			return nil, false
		}
		curr = next
	}
	return curr, true
}

func compareEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if fa, ok1 := toFloat64(a); ok1 {
		if fb, ok2 := toFloat64(b); ok2 {
			return fa == fb
		}
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	}
	return 0, false
}
