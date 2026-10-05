package blueprint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/worker"
)

// RoleView represents which persona perspective to prioritize.
type RoleView string

const (
	RoleProductManager RoleView = "product_manager"
	RoleEngineer       RoleView = "engineer"
)

// StepMeta provides role-focused documentation and specifications for a step.
type StepMeta struct {
	Title            string        `json:"title"`
	Category         string        `json:"category"` // e.g. "Inventory", "Payment", "Risk", "Logistics"
	Summary          string        `json:"summary"`  // PM explanation of business value
	RollbackNote     string        `json:"rollback_note,omitempty"` // PM explanation of compensation
	SLA              time.Duration `json:"sla"`
	EngineeringNotes string        `json:"engineering_notes,omitempty"`
	SampleInput      any           `json:"sample_input,omitempty"`
	SampleOutput     any           `json:"sample_output,omitempty"`
}

// Scenario provides a predefined simulation scenario (e.g. happy path vs failure).
type Scenario struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Mode        string          `json:"mode"` // "success", "failure", "retry", "signal"
	FailStep    string          `json:"fail_step,omitempty"`
	FailReason  string          `json:"fail_reason,omitempty"`
	Input       json.RawMessage `json:"input"`
}

// Blueprint defines a complete, self-documenting workflow template.
type Blueprint struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Version     string                 `json:"version"`
	Domain      string                 `json:"domain"` // e.g. "E-Commerce", "Fintech", "MLOps"
	Description string                 `json:"description"`
	Definition  flow.WorkflowDefinition `json:"definition"`
	StepMeta    map[string]StepMeta    `json:"step_meta"`
	Scenarios   []Scenario             `json:"scenarios"`
	DefaultInput json.RawMessage       `json:"default_input"`
}

// Registry manages available workflow blueprints.
type Registry struct {
	mu         sync.RWMutex
	blueprints map[string]Blueprint
}

// NewRegistry creates a new blueprint registry.
func NewRegistry() *Registry {
	r := &Registry{
		blueprints: make(map[string]Blueprint),
	}
	r.registerDefaults()
	return r
}

// Register adds or updates a blueprint in the registry.
func (r *Registry) Register(bp Blueprint) error {
	if err := bp.Definition.Validate(); err != nil {
		return fmt.Errorf("invalid blueprint workflow definition: %w", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blueprints[bp.ID] = bp
	return nil
}

// Get returns a blueprint by ID.
func (r *Registry) Get(id string) (Blueprint, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	bp, ok := r.blueprints[id]
	return bp, ok
}

// List returns all registered blueprints.
func (r *Registry) List() []Blueprint {
	r.mu.RLock()
	defer r.mu.RUnlock()
	list := make([]Blueprint, 0, len(r.blueprints))
	for _, bp := range r.blueprints {
		list = append(list, bp)
	}
	return list
}

// registerDefaults sets up the canonical e-commerce saga and other classic workflows.
func (r *Registry) registerDefaults() {
	// 1. Classic E-Commerce Order Fulfillment Saga
	ecom := Blueprint{
		ID:          "order-fulfillment-saga",
		Name:        "E-Commerce Order Fulfillment Saga",
		Version:     "1.2.0",
		Domain:      "E-Commerce & Retail",
		Description: "The canonical distributed transaction saga. Coordinates inventory, payment, fraud analysis, and logistics with automatic LIFO compensating transactions.",
		DefaultInput: json.RawMessage(`{
			"order_id": "ORD-7821",
			"customer_id": "CUST-992",
			"customer_name": "Sophia Martinez",
			"items": [
				{"sku": "SKU-MACBOOK-M4", "qty": 1, "price": 1999.00}
			],
			"total_amount": 1999.00,
			"shipping_address": "742 Evergreen Terrace, Springfield, OR"
		}`),
		Definition: flow.WorkflowDefinition{
			Name: "order-fulfillment-saga",
			Steps: []flow.StepDefinition{
				{
					Name:               "reserve-inventory",
					Activity:           "reserve-inventory",
					CompensatingAction: "release-inventory",
					Retries:            2,
					Timeout:            5 * time.Second,
				},
				{
					Name:               "charge-payment",
					Activity:           "charge-payment",
					CompensatingAction: "refund-payment",
					Retries:            1,
					Timeout:            8 * time.Second,
					DependsOn:          []string{"reserve-inventory"},
				},
				{
					Name:               "fraud-risk-score",
					Activity:           "fraud-risk-score",
					CompensatingAction: "flag-for-manual-review",
					Retries:            2,
					Timeout:            4 * time.Second,
					DependsOn:          []string{"charge-payment"},
				},
				{
					Name:               "dispatch-shipment",
					Activity:           "dispatch-shipment",
					CompensatingAction: "cancel-shipment",
					Retries:            2,
					Timeout:            6 * time.Second,
					DependsOn:          []string{"fraud-risk-score"},
				},
			},
		},
		StepMeta: map[string]StepMeta{
			"reserve-inventory": {
				Title:        "Reserve Inventory",
				Category:     "Warehouse",
				Summary:      "Locks physical stock in the local distribution center to prevent over-allocation while customer payment processes.",
				RollbackNote: "If payment or fraud check fails, reserved units are restored to the active catalog in <200ms.",
				SLA:          150 * time.Millisecond,
				EngineeringNotes: "Calls Inventory Store API with stock lock lease. Idempotency enforced via Upstream Raft ID.",
				SampleInput:  map[string]any{"sku": "SKU-MACBOOK-M4", "qty": 1},
				SampleOutput: map[string]any{"reserved": true, "reservation_id": "RES-55109", "warehouse_id": "WH-WEST-1"},
			},
			"charge-payment": {
				Title:        "Process Payment Authorization",
				Category:     "Billing",
				Summary:      "Captures funds from customer payment method (credit card / Apple Pay). Critical transaction point.",
				RollbackNote: "On downstream failure, issues an immediate full refund API call to payment gateway.",
				SLA:          250 * time.Millisecond,
				EngineeringNotes: "Guaranteed Exactly-Once (EOS) charge. Downstream Dedup Store drops retries.",
				SampleInput:  map[string]any{"amount": 1999.00, "currency": "USD", "method": "card_visa"},
				SampleOutput: map[string]any{"charged": true, "txn_id": "TXN-8871629", "gateway": "Stripe"},
			},
			"fraud-risk-score": {
				Title:        "Anti-Fraud & Velocity Scoring",
				Category:     "Risk Engine",
				Summary:      "Evaluates transaction heuristics, IP geolocation, and device fingerprints for suspicious patterns.",
				RollbackNote: "Rolls back prior payment & stock, and flags customer account for compliance audit.",
				SLA:          120 * time.Millisecond,
				EngineeringNotes: "Stateless ML model scoring. Fast failover if model latency exceeds 4s timeout.",
				SampleInput:  map[string]any{"customer_id": "CUST-992", "amount": 1999.00, "ip": "198.51.100.42"},
				SampleOutput: map[string]any{"risk_score": 12, "decision": "PASS", "max_allowed": 70},
			},
			"dispatch-shipment": {
				Title:        "Create Shipping Label & Dispatch",
				Category:     "Logistics",
				Summary:      "Generates carrier tracking number and transmits dispatch manifest to 3PL fulfillment team.",
				RollbackNote: "Sends label void API call to carrier and halts warehouse pick-pack automation.",
				SLA:          300 * time.Millisecond,
				EngineeringNotes: "Communicates with external carrier API (FedEx/UPS) with exponential retry backoff.",
				SampleInput:  map[string]any{"package_weight_kg": 2.1, "carrier": "FedEx", "service": "Priority Overnight"},
				SampleOutput: map[string]any{"tracking_number": "TRK-9904128", "label_url": "https://shipping.cdn/labels/9904128.pdf"},
			},
		},
		Scenarios: []Scenario{
			{
				ID:          "happy-path",
				Name:        "Happy Path (Full Order Success)",
				Description: "All steps succeed cleanly: inventory reserved, card charged, fraud cleared, tracking label generated.",
				Mode:        "success",
				Input: json.RawMessage(`{
					"order_id": "ORD-7821",
					"customer_name": "Sophia Martinez",
					"total_amount": 1999.00,
					"simulate_fail": ""
				}`),
			},
			{
				ID:          "payment-failure",
				Name:        "Payment Declined (Automatic Saga Rollback)",
				Description: "Card is declined with insufficient funds. Plexus-Flow triggers immediate reverse compensation to release held inventory.",
				Mode:        "failure",
				FailStep:    "charge-payment",
				FailReason:  "card_declined: insufficient funds on debit account",
				Input: json.RawMessage(`{
					"order_id": "ORD-7822-FAIL",
					"customer_name": "Marcus Vance",
					"total_amount": 4500.00,
					"simulate_fail": "charge-payment",
					"fail_reason": "card_declined: insufficient funds on debit account"
				}`),
			},
			{
				ID:          "carrier-failure",
				Name:        "Shipping Carrier Outage (Multi-Step Rollback)",
				Description: "Logistics provider returns 503 error. Plexus-Flow automatically executes LIFO rollback: refunds payment first, then releases inventory.",
				Mode:        "failure",
				FailStep:    "dispatch-shipment",
				FailReason:  "shipping_carrier_unavailable: 503 Service Unavailable",
				Input: json.RawMessage(`{
					"order_id": "ORD-7823-SHIPFAIL",
					"customer_name": "Elena Rostova",
					"total_amount": 850.00,
					"simulate_fail": "dispatch-shipment",
					"fail_reason": "shipping_carrier_unavailable: 503 Service Unavailable"
				}`),
			},
		},
	}
	_ = r.Register(ecom)

	// 2. Fintech Loan Application & KYC Pipeline (Fork-Join + Human Gate)
	fintech := Blueprint{
		ID:          "loan-underwriting-pipeline",
		Name:        "Consumer Loan Underwriting & Approval",
		Version:     "2.0.1",
		Domain:      "Fintech & Banking",
		Description: "Fork-join DAG with parallel credit checks and sanctions screening, followed by a human loan officer review gate and fund disbursement.",
		DefaultInput: json.RawMessage(`{
			"application_id": "LOAN-40291",
			"applicant_name": "David Chen",
			"requested_amount": 35000.00,
			"loan_purpose": "Home Improvement",
			"annual_income": 115000.00
		}`),
		Definition: flow.WorkflowDefinition{
			Name: "loan-underwriting-pipeline",
			Steps: []flow.StepDefinition{
				{
					Name:               "verify-identity",
					Activity:           "verify-identity",
					CompensatingAction: "revoke-applicant-profile",
					Retries:            1,
					Timeout:            4 * time.Second,
				},
				{
					Name:               "credit-bureau-check",
					Activity:           "credit-bureau-check",
					CompensatingAction: "record-inquiry-cancellation",
					Retries:            2,
					Timeout:            5 * time.Second,
					DependsOn:          []string{"verify-identity"},
				},
				{
					Name:               "aml-sanctions-screen",
					Activity:           "aml-sanctions-screen",
					CompensatingAction: "clear-compliance-flags",
					Retries:            2,
					Timeout:            4 * time.Second,
					DependsOn:          []string{"verify-identity"},
				},
				{
					Name:      "calculate-risk-score",
					Activity:  "calculate-risk-score",
					Retries:   0,
					Timeout:   3 * time.Second,
					DependsOn: []string{"credit-bureau-check", "aml-sanctions-screen"},
				},
				{
					Name:      "underwriter-approval",
					Activity:  "underwriter-approval",
					Timeout:   5 * time.Minute,
					DependsOn: []string{"calculate-risk-score"},
				},
				{
					Name:               "disburse-funds",
					Activity:           "disburse-funds",
					CompensatingAction: "freeze-escrow-transfer",
					Retries:            1,
					Timeout:            6 * time.Second,
					DependsOn:          []string{"underwriter-approval"},
				},
			},
		},
		StepMeta: map[string]StepMeta{
			"verify-identity": {
				Title:        "Verify Applicant Identity (KYC)",
				Category:     "Identity",
				Summary:      "Validates government ID and SSN against official registry records.",
				RollbackNote: "Purges uploaded biometric tokens if application is aborted.",
				SLA:          200 * time.Millisecond,
			},
			"credit-bureau-check": {
				Title:        "Credit Bureau Pull (Experian)",
				Category:     "Credit Risk",
				Summary:      "Concurrent pull of FICO score and existing debt-to-income obligations.",
				RollbackNote: "Marks credit inquiry as cancelled if rejected to protect applicant score.",
				SLA:          350 * time.Millisecond,
			},
			"aml-sanctions-screen": {
				Title:        "AML & OFAC Sanctions Screening",
				Category:     "Compliance",
				Summary:      "Runs concurrent name matching against global sanctions and PEP databases.",
				RollbackNote: "Clears audit hold markers.",
				SLA:          150 * time.Millisecond,
			},
			"calculate-risk-score": {
				Title:        "Consolidated Risk Scoring (Join)",
				Category:     "Decision Engine",
				Summary:      "Merges credit and compliance results to recommend approval tier and interest rate.",
				SLA:          50 * time.Millisecond,
			},
			"underwriter-approval": {
				Title:        "Human Underwriter Review Gate",
				Category:     "Human-in-the-Loop",
				Summary:      "Pauses execution and waits for loan officer signal (approve/reject) via workflow signal.",
				SLA:          24 * time.Hour,
			},
			"disburse-funds": {
				Title:        "Automated Clearing House (ACH) Payout",
				Category:     "Treasury",
				Summary:      "Transfers approved loan capital to applicant's verified bank account.",
				RollbackNote: "Triggers immediate clawback / transfer recall if post-approval freeze occurs.",
				SLA:          400 * time.Millisecond,
			},
		},
		Scenarios: []Scenario{
			{
				ID:          "loan-approved",
				Name:        "Fast-Track Approval & Disbursement",
				Description: "Credit and AML checks pass concurrently, officer reviews and approves, funds disbursed.",
				Mode:        "success",
				Input: json.RawMessage(`{
					"application_id": "LOAN-40291",
					"applicant_name": "David Chen",
					"requested_amount": 35000.00,
					"auto_approve": true
				}`),
			},
		},
	}
	_ = r.Register(fintech)
}

// RegisterBlueprintActivities registers standard simulation activity implementations
// for all blueprints into the worker registry.
func RegisterBlueprintActivities(r *worker.Registry) {
	var (
		inventoryStock atomic.Int64
	)
	inventoryStock.Store(500)

	// --- E-Commerce Activities ---

	r.Register("reserve-inventory", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		var req struct {
			SimulateFail string `json:"simulate_fail"`
			FailReason   string `json:"fail_reason"`
		}
		_ = json.Unmarshal(in, &req)

		time.Sleep(120 * time.Millisecond)
		if req.SimulateFail == "reserve-inventory" {
			reason := req.FailReason
			if reason == "" {
				reason = "inventory out of stock for requested SKU"
			}
			return nil, errors.New(reason)
		}
		remaining := inventoryStock.Add(-1)
		return json.RawMessage(fmt.Sprintf(`{"reserved": true, "reservation_id": "RES-%d", "remaining_stock": %d}`, time.Now().UnixNano()%100000, remaining)), nil
	})

	r.Register("release-inventory", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		time.Sleep(80 * time.Millisecond)
		newStock := inventoryStock.Add(1)
		return json.RawMessage(fmt.Sprintf(`{"released": true, "restored_stock": %d}`, newStock)), nil
	})

	r.Register("charge-payment", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		var req struct {
			SimulateFail string `json:"simulate_fail"`
			FailReason   string `json:"fail_reason"`
			TotalAmount  float64 `json:"total_amount"`
		}
		_ = json.Unmarshal(in, &req)

		time.Sleep(160 * time.Millisecond)
		if req.SimulateFail == "charge-payment" {
			reason := req.FailReason
			if reason == "" {
				reason = "card_declined: insufficient funds on account"
			}
			return nil, errors.New(reason)
		}
		amount := req.TotalAmount
		if amount <= 0 {
			amount = 1999.00
		}
		return json.RawMessage(fmt.Sprintf(`{"charged": true, "transaction_id": "TXN-%d", "amount": %.2f, "processor": "Stripe Direct"}`, time.Now().UnixNano()%1000000, amount)), nil
	})

	r.Register("refund-payment", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		time.Sleep(90 * time.Millisecond)
		return json.RawMessage(fmt.Sprintf(`{"refunded": true, "refund_id": "REF-%d", "status": "settled"}`, time.Now().UnixNano()%1000000)), nil
	})

	r.Register("fraud-risk-score", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		var req struct {
			SimulateFail string `json:"simulate_fail"`
			FailReason   string `json:"fail_reason"`
		}
		_ = json.Unmarshal(in, &req)

		time.Sleep(110 * time.Millisecond)
		if req.SimulateFail == "fraud-risk-score" {
			reason := req.FailReason
			if reason == "" {
				reason = "fraud_score_exceeded: high risk transaction threshold violated (score 94 > 70)"
			}
			return nil, errors.New(reason)
		}
		return json.RawMessage(`{"risk_score": 14, "decision": "PASS", "model_version": "fraud-v4.2"}`), nil
	})

	r.Register("flag-for-manual-review", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		time.Sleep(60 * time.Millisecond)
		return json.RawMessage(`{"flagged": true, "case_id": "REV-901"}`), nil
	})

	r.Register("dispatch-shipment", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		var req struct {
			SimulateFail string `json:"simulate_fail"`
			FailReason   string `json:"fail_reason"`
		}
		_ = json.Unmarshal(in, &req)

		time.Sleep(150 * time.Millisecond)
		if req.SimulateFail == "dispatch-shipment" {
			reason := req.FailReason
			if reason == "" {
				reason = "shipping_carrier_unavailable: 503 Service Unavailable"
			}
			return nil, errors.New(reason)
		}
		tracking := fmt.Sprintf("TRK-FEDEX-%d", time.Now().UnixNano()%1000000)
		return json.RawMessage(fmt.Sprintf(`{"dispatched": true, "carrier": "FedEx Express", "tracking_number": %q}`, tracking)), nil
	})

	r.Register("cancel-shipment", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		time.Sleep(70 * time.Millisecond)
		return json.RawMessage(`{"cancelled": true, "label_voided": true}`), nil
	})

	// --- Fintech Loan Activities ---

	r.Register("verify-identity", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		time.Sleep(100 * time.Millisecond)
		return json.RawMessage(`{"verified": true, "kyc_tier": 2, "match_confidence": 0.99}`), nil
	})
	r.Register("revoke-applicant-profile", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		time.Sleep(50 * time.Millisecond)
		return json.RawMessage(`{"revoked": true}`), nil
	})

	r.Register("credit-bureau-check", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		time.Sleep(180 * time.Millisecond)
		return json.RawMessage(`{"bureau": "Experian", "fico_score": 768, "dti_ratio": 0.24}`), nil
	})
	r.Register("record-inquiry-cancellation", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		time.Sleep(50 * time.Millisecond)
		return json.RawMessage(`{"cancelled": true}`), nil
	})

	r.Register("aml-sanctions-screen", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		time.Sleep(120 * time.Millisecond)
		return json.RawMessage(`{"screened": true, "ofac_match": false, "pep_match": false}`), nil
	})
	r.Register("clear-compliance-flags", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		time.Sleep(40 * time.Millisecond)
		return json.RawMessage(`{"cleared": true}`), nil
	})

	r.Register("calculate-risk-score", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		time.Sleep(90 * time.Millisecond)
		return json.RawMessage(`{"composite_grade": "A+", "recommended_apr": 0.0649, "max_approved_amount": 40000.00}`), nil
	})

	r.Register("underwriter-approval", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		var req struct {
			AutoApprove bool `json:"auto_approve"`
		}
		_ = json.Unmarshal(in, &req)

		// If auto_approve is set, simulate immediate positive officer decision
		if req.AutoApprove {
			time.Sleep(200 * time.Millisecond)
			return json.RawMessage(`{"decision": "APPROVED", "officer": "E. Miller", "notes": "FICO 768, clean KYC"}`), nil
		}
		// Otherwise brief sleep before normal approval
		time.Sleep(400 * time.Millisecond)
		return json.RawMessage(`{"decision": "APPROVED", "officer": "Automated FastTrack"}`), nil
	})

	r.Register("disburse-funds", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		time.Sleep(150 * time.Millisecond)
		return json.RawMessage(fmt.Sprintf(`{"disbursed": true, "ach_trace": "ACH-%d", "status": "settling"}`, time.Now().UnixNano()%1000000)), nil
	})
	r.Register("freeze-escrow-transfer", func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		time.Sleep(80 * time.Millisecond)
		return json.RawMessage(`{"transfer_recalled": true}`), nil
	})
}
