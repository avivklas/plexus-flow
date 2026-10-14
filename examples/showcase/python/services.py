#!/usr/bin/env python3
"""Python Activity Worker for Plexus-Flow Showcase.

Runs AI risk assessment, customer intelligence telemetry enrichment, and
receipt generation activities. In '--domain=all' mode, also serves inventory,
payment, and shipping activities for a pure Python demonstration.
"""

import argparse
import asyncio
import logging
import sys
import time
from typing import Any

from plexus_flow import ActivityFailure, PermanentFailure, TaskContext, Worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("showcase-python")

# ANSI color codes
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
PURPLE = "\033[35m"
RED = "\033[31m"
BOLD = "\033[1m"
RESET = "\033[0m"


def sleep_duration_s(ctx: TaskContext, payload: Any, default_s: float = 0.1) -> float:
    """Get sleep duration in seconds from input or metadata. 0 means zero sleep."""
    if isinstance(payload, dict) and "sleep_ms" in payload:
        val = payload["sleep_ms"]
        if val is not None:
            return max(0.0, float(val) / 1000.0)
    if ctx.metadata and "sleep_ms" in ctx.metadata:
        try:
            return max(0.0, float(ctx.metadata["sleep_ms"]) / 1000.0)
        except (ValueError, TypeError):
            pass
    return default_s


async def activity_sleep(ctx: TaskContext, payload: Any, default_s: float = 0.1) -> None:
    d = sleep_duration_s(ctx, payload, default_s)
    if d > 0:
        await asyncio.sleep(d)


def create_worker(grpc_addr: str, worker_id: str, domain: str) -> Worker:
    worker = Worker(grpc_addr, worker_id=worker_id, max_concurrency=8)

    # -------------------------------------------------------------------------
    # AI & Intelligence Activities (Primary Python Domain)
    # -------------------------------------------------------------------------

    @worker.activity("evaluate-fraud-risk")
    async def evaluate_fraud_risk(ctx: TaskContext, payload: dict[str, Any]) -> dict[str, Any]:
        task_prefix = ctx.task_id[:8]
        order_id = payload.get("order_id", "UNKNOWN")
        customer_id = payload.get("customer_id", "UNKNOWN")

        auth_dep = ctx.dependency_outputs.get("step-authorize-payment", {})
        auth_amount = auth_dep.get("authorized_amount", payload.get("unit_price", 0.0))

        logger.info(
            f"{CYAN}[Python AI Risk Service]{RESET} Task {task_prefix}: "
            f"Evaluating ML fraud model for Order {order_id} (Customer: {customer_id}, Amount: ${auth_amount:.2f})..."
        )
        await activity_sleep(ctx, payload, default_s=0.18)

        # Trigger simulated permanent failure when requested
        if payload.get("simulate_fraud"):
            logger.error(
                f"{RED}[Python AI Risk Service]{RESET} PERMANENT FAILURE: "
                f"Fraud anomaly score 0.96 exceeds safe threshold! Rejecting transaction."
            )
            raise PermanentFailure(
                f"Order {order_id} flagged by AI Fraud Detection model (risk_score=0.96, anomalous device fingerprint)",
                code="FRAUD_DETECTED",
            )

        logger.info(
            f"{GREEN}[Python AI Risk Service]{RESET} SUCCESS: "
            f"Order {order_id} cleared! (Risk Score: 0.02, Model: XGBoost-v4-prod)"
        )
        return {
            "risk_score": 0.02,
            "risk_level": "LOW",
            "cleared": True,
            "evaluated_by": "python-ai-risk-service",
            "model_version": "v4.2.1-ensemble",
        }

    @worker.activity("enrich-order-telemetry")
    async def enrich_order_telemetry(ctx: TaskContext, payload: dict[str, Any]) -> dict[str, Any]:
        task_prefix = ctx.task_id[:8]
        customer_id = payload.get("customer_id", "UNKNOWN")

        logger.info(
            f"{CYAN}[Python Customer Intelligence]{RESET} Task {task_prefix}: "
            f"Looking up loyalty tier & tax matrix for Customer {customer_id}..."
        )
        await activity_sleep(ctx, payload, default_s=0.12)

        data = {
            "tier": "VIP_PLATINUM",
            "discount_pct": 10.0,
            "tax_jurisdiction": "US_CA",
            "tax_rate": 0.0825,
            "carrier": "DHL Express Priority",
            "service_level": "Next-Day Air",
            "sleep_ms": payload.get("sleep_ms"),
        }
        logger.info(
            f"{GREEN}[Python Customer Intelligence]{RESET} SUCCESS: "
            f"Customer {customer_id} is {data['tier']}, assigned {data['carrier']} ({data['service_level']})"
        )
        return data

    @worker.activity("generate-order-receipt")
    async def generate_order_receipt(ctx: TaskContext, payload: dict[str, Any]) -> dict[str, Any]:
        task_prefix = ctx.task_id[:8]
        order_id = payload.get("order_id", "UNKNOWN")
        shipping_dep = ctx.dependency_outputs.get("step-dispatch-shipment", {})
        tracking = shipping_dep.get("tracking_number", "TRK-PENDING")

        logger.info(
            f"{CYAN}[Python Document & Invoice Service]{RESET} Task {task_prefix}: "
            f"Synthesizing audit receipt & packing slip for Order {order_id}..."
        )
        await activity_sleep(ctx, payload, default_s=0.10)

        receipt_id = f"RCPT-PY-{int(time.time() * 1000) % 1000000}"
        logger.info(
            f"{GREEN}[Python Document & Invoice Service]{RESET} SUCCESS: "
            f"Receipt {receipt_id} compiled with tracking {tracking}."
        )
        return {
            "receipt_id": receipt_id,
            "order_id": order_id,
            "tracking_number": tracking,
            "status": "COMPLETED",
            "invoice_url": f"https://invoices.showcase.internal/{receipt_id}.pdf",
        }

    # -------------------------------------------------------------------------
    # Optional Full Python Activities (for all-Python demonstration)
    # -------------------------------------------------------------------------
    if domain == "all":
        logger.info(f"{YELLOW}[Python Worker]{RESET} Registering all activities (pure-Python mode)...")

        @worker.activity("reserve-inventory")
        async def reserve_inventory(ctx: TaskContext, payload: dict[str, Any]) -> dict[str, Any]:
            await activity_sleep(ctx, payload, default_s=0.10)
            res_id = f"RES-PY-{int(time.time() * 1000) % 1000000}"
            sku = payload.get("item_sku", "QUANTUM-GPU-9000")
            qty = payload.get("quantity", 1)
            price = payload.get("unit_price", 1299.99)
            logger.info(f"{GREEN}[Python Inventory]{RESET} Reserved {qty}x {sku} (Hold: {res_id})")
            return {"reservation_id": res_id, "sku": sku, "quantity": qty, "subtotal": qty * price, "sleep_ms": payload.get("sleep_ms")}

        @worker.activity("release-inventory")
        async def release_inventory(ctx: TaskContext, payload: dict[str, Any]) -> dict[str, Any]:
            await activity_sleep(ctx, payload, default_s=0.08)
            logger.info(f"{PURPLE}[Python Inventory - COMPENSATION]{RESET} Restocked inventory hold {payload}")
            return {"released": True, "restocked": True}

        @worker.activity("authorize-payment")
        async def authorize_payment(ctx: TaskContext, payload: dict[str, Any]) -> dict[str, Any]:
            await activity_sleep(ctx, payload, default_s=0.12)
            if payload.get("simulate_payment_fail"):
                raise ActivityFailure("insufficient funds on credit card", code="CARD_DECLINED")
            tx_id = f"TX-AUTH-PY-{int(time.time() * 1000) % 1000000}"
            logger.info(f"{GREEN}[Python Payment]{RESET} Authorized transaction {tx_id}")
            return {"transaction_id": tx_id, "status": "AUTHORIZED", "currency": "USD", "sleep_ms": payload.get("sleep_ms")}

        @worker.activity("void-authorization")
        async def void_authorization(ctx: TaskContext, payload: dict[str, Any]) -> dict[str, Any]:
            await activity_sleep(ctx, payload, default_s=0.08)
            logger.info(f"{PURPLE}[Python Payment - COMPENSATION]{RESET} Voided authorization hold {payload}")
            return {"voided": True}

        @worker.activity("dispatch-shipment")
        async def dispatch_shipment(ctx: TaskContext, payload: dict[str, Any]) -> dict[str, Any]:
            await activity_sleep(ctx, payload, default_s=0.14)
            if payload.get("simulate_shipping_fail"):
                raise ActivityFailure("courier warehouse unreachable", code="COURIER_UNAVAILABLE")
            tracking = f"TRK-PY-{int(time.time() * 1000) % 10000000}"
            logger.info(f"{GREEN}[Python Logistics]{RESET} Dispatched package {tracking}")
            return {"tracking_number": tracking, "carrier": "DHL Express", "status": "DISPATCHED", "sleep_ms": payload.get("sleep_ms")}

        @worker.activity("cancel-shipment")
        async def cancel_shipment(ctx: TaskContext, payload: dict[str, Any]) -> dict[str, Any]:
            await activity_sleep(ctx, payload, default_s=0.07)
            logger.info(f"{PURPLE}[Python Logistics - COMPENSATION]{RESET} Cancelled courier pickup {payload}")
            return {"cancelled": True}

    return worker


def main() -> None:
    parser = argparse.ArgumentParser(description="Plexus-Flow Python Worker")
    parser.add_argument("--grpc-addr", default="127.0.0.1:9090", help="gRPC address of plexus-flow")
    parser.add_argument("--worker-id", default="python-ai-risk-service", help="Unique worker ID")
    parser.add_argument(
        "--domain",
        default="ai",
        choices=["ai", "all"],
        help="'ai' for AI risk & telemetry; 'all' for full set of activities",
    )
    args = parser.parse_args()

    print(
        f"{CYAN}[Python Worker]{RESET} Starting worker {BOLD}{args.worker_id}{RESET} "
        f"(domain={args.domain}, target={args.grpc_addr})..."
    )

    worker = create_worker(args.grpc_addr, args.worker_id, args.domain)

    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        print(f"\n{YELLOW}[Python Worker]{RESET} Shutting down...")
        worker.stop()


if __name__ == "__main__":
    main()
