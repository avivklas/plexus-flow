# Plexus-Flow Showcase: Polyglot Distributed Saga & AI Risk Orchestration

This showcase demonstrates a real-world, production-style distributed application orchestrated by **Plexus-Flow**. It features **polyglot microservices written in Go and Python** executing activities concurrently across a Directed Acyclic Graph (DAG) with **Exactly-Once Semantics (EOS)** and **automatic Saga compensations**.

---

## Why this Architecture is a Perfect Fit for Plexus-Flow

In modern cloud platforms, backend architectures are frequently split by domain strengths:
- **Go Services** excel at high-throughput transactional state machines, database holds, payment gateways, and courier APIs.
- **Python Services** excel at Machine Learning inference, AI risk assessment, document intelligence, and data enrichment.

### Traditional Pain Points (Temporal / Kafka / Redis locks)
- **Massive Infrastructure Footprint**: Requires running multi-node Temporal clusters, Cassandra/PostgreSQL, Kafka brokers, and ZooKeeper.
- **Fragile Distributed Locks & Ad-Hoc Compensations**: Developers write custom compensation scripts and error-prone Redis locks to clean up partial failures.
- **Divergent Idempotency Logic**: Each service attempts to implement its own deduplication layer, leading to duplicate charges or orphaned inventory holds.

### The Plexus-Flow Solution
- **Zero External Infrastructure**: Built-in Raft consensus machines and gRPC worker broker in a single lightweight engine.
- **First-Class Sagas**: Automatic LIFO (reverse-order of completion) compensation when any activity fails or flags an anomaly.
- **Polyglot & Language-Agnostic**: Go workers and Python workers connect over gRPC and exchange typed data via DAG dependency pipes without knowing each other's addresses or implementations.
- **Visual DAG & Trace Observability**: Out-of-the-box web dashboard showing real-time execution graphs, compensating rollback edges, durations, and audit histories.

---

## Architecture & Workflow Topology

```
                              PLEXUS-FLOW ENGINE (Raft Graph + gRPC Broker)
                                                   │
                       ┌───────────────────────────┴───────────────────────────┐
                       ▼                                                       ▼
           GO FULFILLMENT SERVICE                                   PYTHON AI & DATA SERVICE
         (Inventory, Payment, Shipping)                            (Risk Scoring, Telemetry, PDF)
                       │                                                       │
                       └───────────────────────────┬───────────────────────────┘
                                                   │
                                     WORKFLOW DAG TOPOLOGY
                                                   │
                                      [1. step-reserve-inventory]
                                             (Go Worker)
                                      Compensate: release-inventory
                                                   │
                                                   ▼
                                      [2. step-authorize-payment]
                                             (Go Worker)
                                      Compensate: void-authorization
                                                   │
                                ┌──────────────────┴──────────────────┐
                                ▼                                     ▼
                   [3a. step-evaluate-fraud-risk]       [3b. step-enrich-order-telemetry]
                          (Python AI Worker)                 (Python Intelligence)
                                │                                     │
                                └──────────────────┬──────────────────┘
                                                   ▼
                                      [4. step-dispatch-shipment]
                                             (Go Worker)
                                      Compensate: cancel-shipment
                                                   │
                                                   ▼
                                      [5. step-generate-receipt]
                                           (Python Worker)
```

### Data Pipeline & Cross-Language Inputs
1. **`step-reserve-inventory` (Go)**: Receives initial order input, reserves warehouse hold for SKU `AI-ACCELERATOR-H200`, and emits `reservation_id` and `subtotal`.
2. **`step-authorize-payment` (Go)**: Consumes output of step 1, pre-authorizes funds with the payment gateway, and emits `transaction_id`.
3. **Fork-Join Parallel Execution**:
   - **`step-evaluate-fraud-risk` (Python)**: Reads `dependency_outputs["step-authorize-payment"]`, extracts customer behavioral telemetry, and evaluates an ML fraud model.
   - **`step-enrich-order-telemetry` (Python)**: Concurrently resolves loyalty tier, state tax jurisdiction, and selects expedited courier service level.
4. **`step-dispatch-shipment` (Go)**: Joins outputs from both parallel steps, verifies fraud clearance, books carrier airway bill, and emits tracking number.
5. **`step-generate-receipt` (Python)**: Reads final fulfillment outputs and compiles an order invoice and audit summary.

---

## Automatic Saga Rollback Scenario

When the Python AI risk service flags an anomalous transaction (`simulate_fraud=true`):
1. The Python worker raises `PermanentFailure("anomalous device fingerprint", code="FRAUD_DETECTED")`.
2. Plexus-Flow immediately halts downstream steps and transitions the workflow into `COMPENSATING` mode.
3. Plexus-Flow automatically executes compensating actions in reverse order of completion:
   - **Compensate Step 2**: Calls `void-authorization` on the Go worker to release the customer's credit card hold.
   - **Compensate Step 1**: Calls `release-inventory` on the Go worker to restock the warehouse item.
4. The workflow completes as **`COMPENSATED`** with `error_code: FRAUD_DETECTED` and zero orphaned holds.

---

## Quick Start: One-Command Automated Showcase

Run the complete polyglot demonstration (starts server, boots Go & Python workers, and runs both Happy Path and Saga Rollback scenarios):

```bash
./examples/showcase/run_showcase.sh
```

---

## Running Manually in Separate Terminals

If you want to observe each microservice's logs in real time:

### Terminal 1: Start Plexus-Flow Engine
```bash
./bin/plexus-flow \
    --http-addr=127.0.0.1:8080 \
    --grpc-addr=127.0.0.1:9090 \
    --raft-addr=127.0.0.1:9000 \
    --data-dir=/tmp/plexus-flow-data
```
*Web Dashboard will be live at `http://127.0.0.1:8080`.*

### Terminal 2: Start Go Fulfillment Service
```bash
go run ./examples/showcase/go worker --grpc-addr=127.0.0.1:9090 --domain=fulfillment
```

### Terminal 3: Start Python AI & Intelligence Service
```bash
cd sdk/python
uv run python ../../examples/showcase/python/services.py --grpc-addr=127.0.0.1:9090 --domain=ai
```

### Terminal 4: Trigger Workflows

#### 1. Happy Path (Success Flow):
Using Go launcher:
```bash
go run ./examples/showcase/go submit --order-id=ORD-HAPPY-1001
```
Or using Python launcher:
```bash
cd sdk/python
uv run python ../../examples/showcase/python/launcher.py --order-id=ORD-HAPPY-1002
```

#### 2. Simulated AI Fraud Detection (Automatic Saga Rollback):
```bash
go run ./examples/showcase/go submit --order-id=ORD-FRAUD-2001 --simulate-fraud
```
Or with Python:
```bash
cd sdk/python
uv run python ../../examples/showcase/python/launcher.py --order-id=ORD-FRAUD-2002 --simulate-fraud
```

#### 3. Simulated Payment Failure (Pre-authorization decline):
```bash
go run ./examples/showcase/go submit --order-id=ORD-PAY-3001 --simulate-payment-fail
```

---

## Pure Go Mode & Pure Python Mode

Both worker services include implementations of all 6 activities so you can also run pure single-language deployments:

- **100% Go Fleet**:
  ```bash
  go run ./examples/showcase/go worker --domain=all
  ```
- **100% Python Fleet**:
  ```bash
  cd sdk/python
  uv run python ../../examples/showcase/python/services.py --domain=all
  ```

---

## Live Dashboard & Observability

Visit **`http://127.0.0.1:8080`** while workflows are executing or after completion:
- **Flow View**: Shows the static DAG blueprint, dependency branches, and rollback compensation paths.
- **Traces View**: Overlays actual workflow runs onto the graph, showing step attempts, latency, errors, and live execution timelines.
