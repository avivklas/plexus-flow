package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/avivklas/plexus-flow/pkg/worker"
	"github.com/avivklas/plexus-flow/pkg/workerclient"
)

// EvaluateFraudRiskGo is the Go implementation of the AI Fraud Risk activity (fallback or all-Go mode).
func EvaluateFraudRiskGo(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	task, _ := workerclient.TaskFromContext(ctx)

	var in OrderInput
	_ = json.Unmarshal(input, &in)

	log.Printf("\033[36m[AI Risk & Fraud Service (Go)]\033[0m Task %s: Analyzing behavioral telemetry & fraud anomaly score for Order %s...",
		task.TaskID[:8], in.OrderID)
	time.Sleep(130 * time.Millisecond)

	if in.SimulateFraud {
		log.Printf("\033[31m[AI Risk & Fraud Service (Go)]\033[0m PERMANENT FAILURE: High fraud probability detected (score=0.94, code=FRAUD_DETECTED)\033[0m")
		return nil, worker.NonRetryable(worker.WithCode("FRAUD_DETECTED", fmt.Errorf("order %s flagged by fraud detection model: anomalous device fingerprint and velocity", in.OrderID)))
	}

	eval := FraudEvaluation{
		RiskScore: 0.03,
		RiskLevel: "LOW",
		Cleared:   true,
	}
	log.Printf("\033[32m[AI Risk & Fraud Service (Go)]\033[0m SUCCESS: Order cleared (Risk Score: %.2f, Level: %s)",
		eval.RiskScore, eval.RiskLevel)
	return json.Marshal(eval)
}

// EnrichOrderTelemetryGo is the Go implementation of the telemetry enrichment activity.
func EnrichOrderTelemetryGo(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	task, _ := workerclient.TaskFromContext(ctx)

	var in OrderInput
	_ = json.Unmarshal(input, &in)

	log.Printf("\033[36m[Customer Intelligence (Go)]\033[0m Task %s: Enriching telemetry & loyalty tier for Customer %s...",
		task.TaskID[:8], in.CustomerID)
	time.Sleep(100 * time.Millisecond)

	data := TelemetryData{
		Tier:         "VIP_PLATINUM",
		TaxRate:      0.0825,
		Carrier:      "DHL Express Priority",
		ServiceLevel: "Next-Day Air",
	}

	log.Printf("\033[32m[Customer Intelligence (Go)]\033[0m SUCCESS: Customer tier %s, Carrier %s selected",
		data.Tier, data.Carrier)
	return json.Marshal(data)
}

// GenerateOrderReceiptGo is the Go implementation of the order receipt generation activity.
func GenerateOrderReceiptGo(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	task, _ := workerclient.TaskFromContext(ctx)

	var in OrderInput
	_ = json.Unmarshal(input, &in)

	var dispatch ShipmentDispatch
	if dep, ok := task.DependencyOutputs["step-dispatch-shipment"]; ok && len(dep) > 0 {
		_ = json.Unmarshal(dep, &dispatch)
	}

	log.Printf("\033[36m[Notification & Document Service (Go)]\033[0m Task %s: Generating final order receipt & dispatch audit...",
		task.TaskID[:8])
	time.Sleep(90 * time.Millisecond)

	receiptID := fmt.Sprintf("RCPT-%d", time.Now().Unix()%1000000)
	receipt := map[string]any{
		"receipt_id":      receiptID,
		"order_id":        in.OrderID,
		"customer_id":     in.CustomerID,
		"tracking_number": dispatch.TrackingNumber,
		"status":          "COMPLETED",
		"generated_at":    time.Now().UTC().Format(time.RFC3339),
		"receipt_url":     fmt.Sprintf("https://receipts.example.com/%s.pdf", receiptID),
	}

	log.Printf("\033[32m[Notification & Document Service (Go)]\033[0m SUCCESS: Receipt %s generated! All steps complete.",
		receiptID)
	return json.Marshal(receipt)
}
