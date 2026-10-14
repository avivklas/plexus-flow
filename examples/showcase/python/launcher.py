#!/usr/bin/env python3
"""Python Workflow Launcher & Monitor for Plexus-Flow Showcase.

Submits the Smart Fulfillment Saga to the Plexus-Flow HTTP REST API, polls the
state machine until terminal status, prints live execution traces, and
demonstrates automatic Saga compensation.
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from typing import Any

CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
PURPLE = "\033[35m"
RED = "\033[31m"
BOLD = "\033[1m"
RESET = "\033[0m"


def http_post_json(url: str, data: dict[str, Any]) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        data=json.dumps(data).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))


def http_get_json(url: str) -> dict[str, Any]:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))


def get_workflow_definition() -> dict[str, Any]:
    return {
        "name": "smart-fulfillment-saga",
        "steps": [
            {
                "name": "step-reserve-inventory",
                "activity": "reserve-inventory",
                "compensating_action": "release-inventory",
                "retries": 2,
                "timeout": 5000000000,
            },
            {
                "name": "step-authorize-payment",
                "activity": "authorize-payment",
                "compensating_action": "void-authorization",
                "depends_on": ["step-reserve-inventory"],
                "retries": 2,
                "timeout": 5000000000,
            },
            {
                "name": "step-evaluate-fraud-risk",
                "activity": "evaluate-fraud-risk",
                "depends_on": ["step-authorize-payment"],
                "retries": 1,
                "timeout": 5000000000,
            },
            {
                "name": "step-enrich-order-telemetry",
                "activity": "enrich-order-telemetry",
                "depends_on": ["step-authorize-payment"],
                "retries": 2,
                "timeout": 5000000000,
            },
            {
                "name": "step-dispatch-shipment",
                "activity": "dispatch-shipment",
                "compensating_action": "cancel-shipment",
                "depends_on": ["step-evaluate-fraud-risk", "step-enrich-order-telemetry"],
                "retries": 2,
                "timeout": 5000000000,
            },
            {
                "name": "step-generate-receipt",
                "activity": "generate-order-receipt",
                "depends_on": ["step-dispatch-shipment"],
                "retries": 2,
                "timeout": 5000000000,
            },
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Plexus-Flow Python Workflow Launcher")
    parser.add_argument("--http-addr", default="http://127.0.0.1:8080", help="HTTP API base URL")
    parser.add_argument("--order-id", default="", help="Custom order identifier")
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=100,
        help="Activity simulation sleep in milliseconds (pass 0 for instant execution)",
    )
    parser.add_argument(
        "--simulate-fraud",
        action="store_true",
        help="Simulate AI fraud detection rejection (triggers Saga rollback)",
    )
    parser.add_argument(
        "--simulate-payment-fail",
        action="store_true",
        help="Simulate payment gateway decline",
    )
    parser.add_argument(
        "--simulate-shipping-fail",
        action="store_true",
        help="Simulate courier dispatch error",
    )
    args = parser.parse_args()

    order_id = args.order_id or f"ORD-PY-{int(time.time()) % 100000}"

    payload = {
        "order_id": order_id,
        "customer_id": "CUST-9921",
        "item_sku": "QUANTUM-ACCELERATOR-Q1",
        "quantity": 1,
        "unit_price": 3499.00,
        "shipping_address": "500 Howard Street, San Francisco, CA",
        "simulate_fraud": args.simulate_fraud,
        "simulate_payment_fail": args.simulate_payment_fail,
        "simulate_shipping_fail": args.simulate_shipping_fail,
        "sleep_ms": args.sleep_ms,
    }

    start_req = {
        "workflow_id": order_id,
        "definition": get_workflow_definition(),
        "input": payload,
        "metadata": {
            "initiated_by": "python-launcher",
            "region": "us-west",
            "sleep_ms": str(args.sleep_ms),
        },
    }

    print(f"\n{CYAN}========================================================================{RESET}")
    print(f"{BOLD}[Plexus-Flow Python Showcase]{RESET} Starting Saga: {YELLOW}{order_id}{RESET}")
    if args.simulate_fraud:
        print(f"{BOLD}[Scenario]{RESET} {RED}SIMULATING AI FRAUD DETECTION FAILURE{RESET} (Triggers Reverse Saga Rollback)")
    elif args.simulate_payment_fail:
        print(f"{BOLD}[Scenario]{RESET} {RED}SIMULATING PAYMENT DECLINE{RESET} (Triggers Reverse Saga Rollback)")
    elif args.simulate_shipping_fail:
        print(f"{BOLD}[Scenario]{RESET} {RED}SIMULATING SHIPPING FAILURE{RESET} (Triggers Reverse Saga Rollback)")
    else:
        print(f"{BOLD}[Scenario]{RESET} {GREEN}HAPPY PATH{RESET} (Polyglot Go + Python services run in unison)")
    print(f"{CYAN}========================================================================{RESET}\n")

    api_base = args.http_addr.rstrip("/")
    try:
        wf = http_post_json(f"{api_base}/api/v1/workflows/start", start_req)
    except Exception as e:
        print(f"{RED}Error starting workflow: {e}{RESET}")
        sys.exit(1)

    print(f"{CYAN}[Engine]{RESET} Workflow initiated: ID={wf.get('workflow_id')} RunID={wf.get('run_id')}")
    print(f"{PURPLE}[Dashboard]{RESET} Live graph & traces: {BOLD}{api_base}{RESET}\n")

    terminal_statuses = {"COMPLETED", "FAILED", "COMPENSATED", "CANCELLED", "COMPENSATION_FAILED"}
    seen_step_status: dict[str, str] = {}
    deadline = time.time() + 25.0

    while time.time() < deadline:
        try:
            current = http_get_json(f"{api_base}/api/v1/workflows/{order_id}")
        except Exception:
            time.sleep(0.15)
            continue

        steps = current.get("steps", {})
        for name, step in steps.items():
            st = step.get("status")
            if seen_step_status.get(name) != st:
                seen_step_status[name] = st
                color = CYAN
                if st == "COMPLETED":
                    color = GREEN
                elif st == "FAILED":
                    color = RED
                elif st == "COMPENSATING":
                    color = PURPLE
                elif st == "COMPENSATED":
                    color = YELLOW

                attempt = step.get("attempt", 1)
                print(f"  • {name:<30} -> {color}{st:<12}{RESET} (Attempt: {attempt})")
                if step.get("error"):
                    code = step.get("error_code", "")
                    print(f"    {RED}└─ Error: {step['error']} (Code: {code}){RESET}")

        if current.get("status") in terminal_statuses:
            final_status = current.get("status")
            print(f"\n{CYAN}========================================================================{RESET}")
            if final_status == "COMPLETED":
                print(f"{GREEN}[RESULT]{RESET} SAGA FINISHED SUCCESSFULLY: {BOLD}{final_status}{RESET}")
                print("All DAG steps completed across Go & Python services with Exactly-Once Semantics!")
            elif final_status == "COMPENSATED":
                print(f"{YELLOW}[RESULT]{RESET} SAGA ROLLED BACK CLEANLY: {BOLD}{final_status}{RESET}")
                print(f"Failed step triggered automatic reverse-order compensation!")
                print(f"Error Code: {RED}{current.get('error_code')}{RESET} | System returned to clean state.")
            else:
                print(f"{YELLOW}[RESULT]{RESET} Workflow finished with status: {final_status}")
            print(f"{CYAN}========================================================================{RESET}\n")
            return

        time.sleep(0.15)

    print(f"{YELLOW}[Warning] Timed out waiting for workflow to finish.{RESET}")


if __name__ == "__main__":
    main()
