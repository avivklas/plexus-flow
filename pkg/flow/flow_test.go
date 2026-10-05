package flow

import (
	"encoding/json"
	"testing"
	"time"
)

func TestWorkflowDefinitionValidation(t *testing.T) {
	tests := []struct {
		name    string
		def     WorkflowDefinition
		wantErr bool
	}{
		{
			name: "empty definition name",
			def: WorkflowDefinition{
				Name: "",
				Steps: []StepDefinition{
					{Name: "step-1", Activity: "act-1"},
				},
			},
			wantErr: true,
		},
		{
			name: "no steps",
			def: WorkflowDefinition{
				Name:  "wf-empty",
				Steps: []StepDefinition{},
			},
			wantErr: true,
		},
		{
			name: "duplicate step name",
			def: WorkflowDefinition{
				Name: "wf-dup",
				Steps: []StepDefinition{
					{Name: "step-1", Activity: "act-1"},
					{Name: "step-1", Activity: "act-2"},
				},
			},
			wantErr: true,
		},
		{
			name: "missing activity",
			def: WorkflowDefinition{
				Name: "wf-missing-act",
				Steps: []StepDefinition{
					{Name: "step-1", Activity: ""},
				},
			},
			wantErr: true,
		},
		{
			name: "non-existent dependency",
			def: WorkflowDefinition{
				Name: "wf-bad-dep",
				Steps: []StepDefinition{
					{Name: "step-1", Activity: "act-1", DependsOn: []string{"ghost-step"}},
				},
			},
			wantErr: true,
		},
		{
			name: "self-dependency",
			def: WorkflowDefinition{
				Name: "wf-self-dep",
				Steps: []StepDefinition{
					{Name: "step-1", Activity: "act-1", DependsOn: []string{"step-1"}},
				},
			},
			wantErr: true,
		},
		{
			name: "cycle in dependencies",
			def: WorkflowDefinition{
				Name: "wf-cycle",
				Steps: []StepDefinition{
					{Name: "A", Activity: "act-a", DependsOn: []string{"B"}},
					{Name: "B", Activity: "act-b", DependsOn: []string{"C"}},
					{Name: "C", Activity: "act-c", DependsOn: []string{"A"}},
				},
			},
			wantErr: true,
		},
		{
			name: "valid sequential implicit",
			def: WorkflowDefinition{
				Name: "wf-seq",
				Steps: []StepDefinition{
					{Name: "step-1", Activity: "act-1"},
					{Name: "step-2", Activity: "act-2"},
					{Name: "step-3", Activity: "act-3"},
				},
			},
			wantErr: false,
		},
		{
			name: "valid DAG diamond",
			def: WorkflowDefinition{
				Name: "wf-diamond",
				Steps: []StepDefinition{
					{Name: "start", Activity: "act-start"},
					{Name: "branchA", Activity: "act-a", DependsOn: []string{"start"}},
					{Name: "branchB", Activity: "act-b", DependsOn: []string{"start"}},
					{Name: "join", Activity: "act-join", DependsOn: []string{"branchA", "branchB"}},
				},
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.def.Validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestWorkflowExecutionDAG(t *testing.T) {
	def := WorkflowDefinition{
		Name: "diamond-pipeline",
		Steps: []StepDefinition{
			{Name: "start", Activity: "init", CompensatingAction: "undo-init"},
			{Name: "branchA", Activity: "processA", DependsOn: []string{"start"}, CompensatingAction: "undo-processA"},
			{Name: "branchB", Activity: "processB", DependsOn: []string{"start"}, CompensatingAction: "undo-processB"},
			{Name: "join", Activity: "aggregate", DependsOn: []string{"branchA", "branchB"}},
		},
	}

	input := json.RawMessage(`{"param": 42}`)
	wf, err := NewWorkflowInstance("wf-1", "run-1", def, input, nil)
	if err != nil {
		t.Fatalf("NewWorkflowInstance failed: %v", err)
	}

	// Initially, only 'start' should be ready
	ready := wf.NextReadySteps()
	if len(ready) != 1 || ready[0].Name != "start" {
		t.Fatalf("expected only 'start' ready, got %v", ready)
	}

	// Mark 'start' as running then completed
	now := time.Now().UTC()
	ready[0].Status = StepStatusRunning
	ready[0].StartedAt = &now

	t1 := now.Add(100 * time.Millisecond)
	ready[0].Status = StepStatusCompleted
	ready[0].CompletedAt = &t1

	// Now both branchA and branchB should be ready!
	ready = wf.NextReadySteps()
	if len(ready) != 2 {
		t.Fatalf("expected 2 branches ready, got %d", len(ready))
	}
	names := map[string]bool{ready[0].Name: true, ready[1].Name: true}
	if !names["branchA"] || !names["branchB"] {
		t.Fatalf("expected branchA and branchB ready, got %v", names)
	}

	// Mark branchA completed
	t2 := t1.Add(100 * time.Millisecond)
	wf.Steps["branchA"].Status = StepStatusCompleted
	wf.Steps["branchA"].CompletedAt = &t2

	// Join should NOT be ready yet because branchB is still pending
	ready = wf.NextReadySteps()
	if len(ready) != 1 || ready[0].Name != "branchB" {
		t.Fatalf("expected branchB ready, got %v", ready)
	}

	// Mark branchB completed
	t3 := t2.Add(100 * time.Millisecond)
	wf.Steps["branchB"].Status = StepStatusCompleted
	wf.Steps["branchB"].CompletedAt = &t3

	// Now join should be ready!
	ready = wf.NextReadySteps()
	if len(ready) != 1 || ready[0].Name != "join" {
		t.Fatalf("expected 'join' ready, got %v", ready)
	}
}

func TestSagaReverseCompensationOrder(t *testing.T) {
	def := WorkflowDefinition{
		Name: "order-saga",
		Steps: []StepDefinition{
			{Name: "reserve-inv", Activity: "reserve", CompensatingAction: "release-inv"},
			{Name: "charge-card", Activity: "charge", CompensatingAction: "refund-card"},
			{Name: "ship-item", Activity: "ship", CompensatingAction: "cancel-ship"},
		},
	}

	wf, err := NewWorkflowInstance("wf-order-1", "run-1", def, nil, nil)
	if err != nil {
		t.Fatalf("NewWorkflowInstance failed: %v", err)
	}

	// Complete step 1 at t1
	t1 := time.Now().UTC()
	wf.Steps["reserve-inv"].Status = StepStatusCompleted
	wf.Steps["reserve-inv"].CompletedAt = &t1

	// Complete step 2 at t2 (later than t1)
	t2 := t1.Add(1 * time.Second)
	wf.Steps["charge-card"].Status = StepStatusCompleted
	wf.Steps["charge-card"].CompletedAt = &t2

	// Step 3 fails!
	wf.Steps["ship-item"].Status = StepStatusFailed
	wf.Status = StatusCompensating

	// Next compensating step MUST be step 2 (charge-card) because it was completed latest!
	comp := wf.NextCompensatingStep()
	if comp == nil || comp.Name != "charge-card" {
		t.Fatalf("expected charge-card to be compensated first, got %v", comp)
	}

	// Mark charge-card as compensated
	now := time.Now().UTC()
	comp.Status = StepStatusCompensated
	comp.CompensatedAt = &now

	// Next compensating step MUST now be step 1 (reserve-inv)!
	comp2 := wf.NextCompensatingStep()
	if comp2 == nil || comp2.Name != "reserve-inv" {
		t.Fatalf("expected reserve-inv to be compensated next, got %v", comp2)
	}

	// Mark reserve-inv as compensated
	comp2.Status = StepStatusCompensated
	comp2.CompensatedAt = &now

	// No more compensating steps!
	if wf.NextCompensatingStep() != nil {
		t.Fatalf("expected no more compensating steps")
	}

	if !wf.AllCompensationsCompleted() {
		t.Fatalf("expected AllCompensationsCompleted to be true")
	}
}
