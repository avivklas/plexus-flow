# Plexus-Flow

[![Go Version](https://img.shields.io/badge/go-1.27+-blue.svg)](https://golang.org)
[![Consensus](https://img.shields.io/badge/consensus-Raft%20Graph-orange.svg)](https://github.com/avivklas/plexus)
[![Semantics](https://img.shields.io/badge/semantics-Exactly--Once%20(EOS)-green.svg)](https://github.com/avivklas/plexus)

**Distributed Sagas & Multi-Step Workflow Orchestration without Temporal, Kafka, or fragile Redis locks.**

`plexus-flow` is a distributed workflow engine built directly on the **Plexus Raft Graph & Command Topology**. Workflows are modeled as deterministic state machines with automatic compensating actions. As steps complete, the Raft Graph deterministically advances downstream activities and compensating rollbacks with guaranteed **Exactly-Once Semantics (EOS)**, even in the face of network retries, node crashes, and leader failovers.

---

## Why Plexus-Flow?

| Challenge | Traditional Approach (Temporal / Kafka / Redis) | Plexus-Flow |
|---|---|---|
| **Infrastructure Overhead** | Requires external Temporal Server clusters, Cassandra/PostgreSQL, Kafka brokers, ZooKeeper/KRaft. | **Zero external dependencies.** Embedded Raft machines and Go binaries in a single process or multi-node cluster. |
| **Deduplication & EOS** | Manual application-level idempotency keys in Redis, database unique indexes, fragile distributed locks. | **Native Exactly-Once Semantics (EOS).** Deterministic idempotency keys (`UpstreamID:Term:Index:Seq`) verified by embedded deduplication engines (`dedup.Store`). |
| **Saga Rollback** | Bespoke compensation logic, ad-hoc state polling, or complicated Zeebe/Camunda BPMN engines. | **First-class Saga state machine.** Automatic LIFO (reverse-order of completion) compensation with audit trail. |
| **Coordination** | Network calls between separate workflow workers, database state poles, and message brokers. | **Raft Graph Command Topology.** Upstream workflow commits deterministically pipe downstream activity dispatches via reactive consensus hooks. |

---

## Architecture Overview

```
                      PLEXUS RAFT GRAPH TOPOLOGY
  +-------------------------------------------------------------------+
  |                                                                   |
  |   +-----------------------------------------------------------+   |
  |   |             UPSTREAM: WORKFLOW MACHINE (Raft)             |   |
  |   |                                                           |   |
  |   |   +---------------------------------------------------+   |   |
  |   |   |                     FlowStore                     |   |   |
  |   |   |  - Workflow Instances (ID, RunID, Status)         |   |   |
  |   |   |  - DAG / Pipeline Execution State                 |   |   |
  |   |   |  - Step Executions (Attempt, Output, Error)       |   |   |
  |   |   |  - Audit History Event Log                        |   |   |
  |   |   +---------------------------------------------------+   |   |
  |   +-----------------------------+-----------------------------+   |
  |                                 |                                 |
  |                      Commit Observer Hook                         |
  |                                 v                                 |
  |   +-----------------------------------------------------------+   |
  |   |            GRAPH PIPE & EVENT TRANSFORMER                 |   |
  |   |   Transform(ctx, cmd, res) -> []Command                   |   |
  |   |   * If RUNNING: NextReadySteps() -> "activity.dispatch"   |   |
  |   |   * If COMPENSATING: NextCompensating() -> "activity.cmp" |   |
  |   |   * Deterministic EOS Key: UpstreamID:Term:Index:Seq      |   |
  |   +-----------------------------+-----------------------------+   |
  |                                 |                                 |
  |                      Reliable Dispatch Edge                       |
  |                                 v                                 |
  |   +-----------------------------------------------------------+   |
  |   |            DOWNSTREAM: ACTIVITY MACHINE (Raft)            |   |
  |   |                                                           |   |
  |   |   +---------------------------------------------------+   |   |
  |   |   |         AttachDedupInterceptor (dedup.Store)      |   |   |
  |   |   |   Checks Idempotency Key -> Rejects Duplicates    |   |   |
  |   |   +-------------------------+-------------------------+   |   |
  |   |                             v                             |   |
  |   |   +---------------------------------------------------+   |   |
  |   |   |                   ActivityStore                   |   |   |
  |   |   |   Records Dispatches & Emits Tasks to Workers     |   |   |
  |   |   +-------------------------+-------------------------+   |   |
  |   +-----------------------------|-----------------------------+   |
  +---------------------------------|---------------------------------+
                                    |
                                    v
  +-------------------------------------------------------------------+
  |                       WORKER EXECUTOR                             |
  |  - Registry of Go ActivityFuncs                                   |
  |  - Execution with Timeouts & Backoff Retries                      |
  |  - Proposes Completion / Failure back to Upstream Workflow Raft   |
  |                                                                   |
  |  On Success:  workflowMachine.Apply("flow.step.complete")         |
  |  On Failure:  workflowMachine.Apply("flow.step.fail")             |
  |  On Rollback: workflowMachine.Apply("flow.step.comp.complete")    |
  +-------------------------------------------------------------------+
```

---

## Core Features

1. **Stateful Workflow Models (`pkg/flow`)**:
   - Workflows with states: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `COMPENSATING`, `COMPENSATED`, `CANCELLED`.
   - Directed Acyclic Graph (DAG) validation with Kahn's algorithm cycle detection.
   - Sequential pipelines or fork-join parallel dependencies (`DependsOn`).
   - Audit event log with nanosecond timestamping.

2. **Raft State Store (`pkg/flowstore`)**:
   - Implements `plexus.Store`.
   - Replicated state machine with zero state divergence.
   - Snapshot & restore support for fast cluster recovery.

3. **Raft Graph Coordinator (`pkg/graphflow`)**:
   - Wires upstream workflow commits to downstream activity triggers using `plexus.NewGraph()`.
   - Uses `dedup.Store` and `AttachDedupInterceptor` for Exactly-Once Semantics (EOS).

4. **Activity Worker Pool (`pkg/worker`)**:
   - In-process or external activity registry (`ActivityFunc`).
   - Timeout context propagation and configurable exponential retry backoff.
   - Automatic execution of compensating actions during Saga rollback.

5. **REST API & Go SDK (`pkg/api`)**:
   - Clean HTTP endpoints:
     - `POST /api/v1/workflows/start`
     - `GET /api/v1/workflows/{id}`
     - `GET /api/v1/workflows`
     - `POST /api/v1/workflows/{id}/signal`
     - `POST /api/v1/workflows/{id}/cancel`
     - `GET /api/v1/cluster/status`
   - Complete Go SDK client (`api.NewClient`).

---

## Quick Start

### 1. Build the Binary

```bash
go build -o bin/plexus-flow ./cmd/plexus-flow
```

### 2. Run the Built-in Order Processing Saga Demo

#### Happy Path (Full Success):
```bash
./bin/plexus-flow --run-example
```

Output:
```text
  ____  _                      _____ _                 
 |  _ \| |                    |  ___| | _____      __  
 | |_) | | _____  ___   _ ___ | |_  | |/ _ \ \ /\ / /  
 |  __/| |/ _ \ \/ / | | / __||  _| | | (_) \ V  V /   
 |_|   |_|\___/_/\_\ |_| \__ \|_|   |_|\___/ \_/\_/    
                    \__,_|___/                         
   Distributed Sagas & Workflows without Temporal or Kafka

[Plexus-Flow] Node ID: node-1
[Plexus-Flow] Workflow Raft: 127.0.0.1:9000
[Plexus-Flow] Activity Raft: 127.0.0.1:9001
[Plexus-Flow] HTTP API:      http://127.0.0.1:8080
[Plexus-Flow] Data Dir:      ./data
[Consensus] Starting Raft consensus engines...
[Consensus] Waiting for Raft leadership...
[Consensus] Node elected leader on both machines! Raft quorum active.
[API] HTTP API listening on 127.0.0.1:8080

========================================================================
[DEMO] Starting Order Processing Saga Demonstration...
[DEMO] Mode: HAPPY PATH (All steps succeed)
========================================================================
[DEMO] Workflow started: ID=ORD-9901 RunID=run-1791203543749
  [ACTIVITY] reserve-inventory -> RESERVING item ITEM-42 for Order...
  [ACTIVITY] reserve-inventory -> SUCCESS: Inventory reserved!
  [ACTIVITY] charge-card -> CHARGING credit card $149.99...
  [ACTIVITY] charge-card -> SUCCESS: Card charged $149.99!
  [ACTIVITY] ship-item -> CREATING shipping label with courier...
  [ACTIVITY] ship-item -> SUCCESS: Package dispatched! Tracking: TRK-554433

========================================================================
[DEMO RESULT] SAGA FINISHED SUCCESSFULLY: Status = COMPLETED
[DEMO RESULT] All steps executed and committed via Raft Graph with EOS!
========================================================================
```

#### Automatic Saga Rollback Demo (Simulated Failure):

```bash
./bin/plexus-flow --run-example --simulate-failure --fail-step charge-card
```

Output:
```text
========================================================================
[DEMO] Starting Order Processing Saga Demonstration...
[DEMO] Mode: SIMULATED FAILURE on step 'charge-card' (Triggering Automatic Saga Compensation)
========================================================================
[DEMO] Workflow started: ID=ORD-9902 RunID=run-1791203543881
  [ACTIVITY] reserve-inventory -> RESERVING item ITEM-42 for Order...
  [ACTIVITY] reserve-inventory -> SUCCESS: Inventory reserved!
  [ACTIVITY] charge-card -> CHARGING credit card $149.99...
  [ACTIVITY] charge-card -> ERROR: Card declined: insufficient funds!
  [COMPENSATION] release-inventory -> RELEASING item ITEM-42 back to stock...
  [COMPENSATION] release-inventory -> SUCCESS: Inventory released back to stock!

========================================================================
[DEMO RESULT] SAGA ROLLED BACK SAFELY: Status = COMPENSATED
[DEMO RESULT] Failed step triggered automatic reverse-order compensation!
[DEMO RESULT] No orphaned transactions. System returned to clean state.
========================================================================
```

---

## Code Example: Defining a Saga in Go

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/graphflow"
	"github.com/avivklas/plexus-flow/pkg/worker"
)

func main() {
	// 1. Register activities and compensating actions
	registry := worker.NewRegistry()

	// Activity 1: Reserve Inventory
	registry.Register("reserve-inventory", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		log.Println("Reserving inventory...")
		return json.RawMessage(`{"reserved": true}`), nil
	})
	// Compensation 1: Release Inventory
	registry.Register("release-inventory", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		log.Println("Compensating: Releasing inventory...")
		return json.RawMessage(`{"released": true}`), nil
	})

	// Activity 2: Charge Card
	registry.Register("charge-card", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		log.Println("Charging credit card...")
		return json.RawMessage(`{"charged": true}`), nil
	})
	// Compensation 2: Refund Card
	registry.Register("refund-card", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		log.Println("Compensating: Refunding card...")
		return json.RawMessage(`{"refunded": true}`), nil
	})

	// 2. Initialize embedded Raft Graph coordinator
	coord, err := graphflow.NewEmbeddedCoordinator(registry)
	if err != nil {
		log.Fatalf("init coordinator: %v", err)
	}
	defer coord.Close()

	// 3. Define the multi-step Saga
	def := flow.WorkflowDefinition{
		Name: "ecommerce-order-saga",
		Steps: []flow.StepDefinition{
			{
				Name:               "step-inventory",
				Activity:           "reserve-inventory",
				CompensatingAction: "release-inventory",
			},
			{
				Name:               "step-payment",
				Activity:           "charge-card",
				CompensatingAction: "refund-card",
			},
		},
	}

	// 4. Start the workflow
	ctx := context.Background()
	wf, err := coord.StartWorkflow(ctx, flowstore.StartWorkflowRequest{
		WorkflowID: "order-1001",
		Definition: def,
		Input:      json.RawMessage(`{"amount": 149.99}`),
	})
	if err != nil {
		log.Fatalf("start workflow: %v", err)
	}

	fmt.Printf("Workflow %s running with RunID %s\n", wf.WorkflowID, wf.RunID)
}
```

---

## Running the Test Suite

Run all unit tests, graph integration tests, and saga rollback scenarios:

```bash
go test -v ./...
```

All packages pass:
- `pkg/flow`: DAG cycle validation, topological ordering, and reverse-order compensation candidate selection.
- `pkg/flowstore`: Raft-replicated state mutations, step transitions, audit trail, and snapshot recovery.
- `pkg/worker`: Activity execution, timeouts, exponential backoff retries, and task deduplication.
- `pkg/graphflow`: Raft Graph coordinator, edge dispatching, and Exactly-Once Semantics (EOS).
- `pkg/api`: HTTP REST API and Go SDK client.
- `test`: End-to-end integration tests (Full Success Saga, Reverse-Order Rollback Saga, DAG Fork-Join, Snapshot Mid-Execution Recovery).

---

## License

MIT License. Built on top of [Plexus](https://github.com/avivklas/plexus).
