# Showcase: ML Model Release Pipeline

A model release is a long-running, multi-step process with real side effects (data staged, models trained, registry updated) and a **human decision** in the middle. It is a natural fit for `plexus-flow`: no Temporal cluster, no Kafka, no Redis locks. It is a single Go process.

```
ingest-data ─┬─► train-xgboost ──────┐
 (retries)   └─► train-transformer ──┴─► evaluate ─► await-approval ─► publish-model
                 (parallel fork-join)                 (human signal)
```

## What it shows

| Feature | Where in the demo |
|---|---|
| Fork-join DAG (`DependsOn`) | Both trainers start at the same instant; `evaluate` waits for both |
| Retries | `ingest-data` fails twice (warehouse timeout) and succeeds on attempt 3 |
| Human-in-the-loop via signals | `await-approval` blocks until a reviewer calls `SignalWorkflow` (`approve` / `reject`) |
| Saga rollback (LIFO) | On `reject`, trained models and staged data are cleaned up in reverse order of completion |
| Replicated audit trail | Every transition is a Raft-committed event, printed at the end |

## Run it

```bash
# Reviewer approves: model is published
go run ./examples/mlrelease --decision approve

# Reviewer rejects: automatic rollback
go run ./examples/mlrelease --decision reject
```

## Reject scenario (abridged)

```text
  1.01s ACTIVITY     xgboost: training started
  1.01s ACTIVITY     transformer: training started        <- parallel
  2.21s APPROVAL     waiting for a human reviewer signal (approve/reject)...
  3.74s REVIEWER     submitting decision: reject
  3.78s APPROVAL     rejected
  3.78s ROLLBACK     delete-artifacts: removing trained model {"model":"transformer",...}
  3.88s ROLLBACK     delete-artifacts: removing trained model {"model":"xgboost",...}
  3.99s ROLLBACK     purge-staging: deleting s3://ml-staging/run-7
  4.24s RESULT       COMPENSATED: rejected model rolled back in reverse order
```

## How the approval gate works

`await-approval` is an ordinary activity. It polls the workflow's replicated history for a `WORKFLOW_SIGNALED` event. Because signals are committed through Raft, the decision survives restarts and leader changes. Returning an error from the activity is what triggers compensation.
