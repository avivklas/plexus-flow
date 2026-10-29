# Showcase: E-Commerce Order Fulfillment Saga

Demonstrates automatic LIFO Saga compensation and the built-in Flow & Trace Dashboard using `plexus-flow`.

```
reserve-inventory ──► charge-card ──► ship-item
release-inventory ◄── refund-card ◄── cancel-shipment  (automatic LIFO rollback on failure)
```

## Run It

```bash
# 1. Happy path (all steps succeed)
go run ./examples/ordersaga

# 2. Simulated failure on ship-item -> automatic reverse-order compensation (refund-card -> release-inventory)
go run ./examples/ordersaga --simulate-failure --fail-step ship-item

# 3. Run and keep the Flow & Trace Dashboard open at http://127.0.0.1:8080
go run ./examples/ordersaga --simulate-failure --fail-step ship-item --serve
```
