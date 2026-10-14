package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/avivklas/plexus-flow/pkg/workerclient"
)

type ShipmentDispatch struct {
	TrackingNumber    string    `json:"tracking_number"`
	OrderID           string    `json:"order_id"`
	Carrier           string    `json:"carrier"`
	ServiceLevel      string    `json:"service_level"`
	EstimatedDelivery string    `json:"estimated_delivery"`
	DispatchedAt      time.Time `json:"dispatched_at"`
	Status            string    `json:"status"`
	SleepMs           *int      `json:"sleep_ms,omitempty"`
}

type TelemetryData struct {
	Tier         string  `json:"tier"`
	TaxRate      float64 `json:"tax_rate"`
	Carrier      string  `json:"carrier"`
	ServiceLevel string  `json:"service_level"`
}

type FraudEvaluation struct {
	RiskScore float64 `json:"risk_score"`
	RiskLevel string  `json:"risk_level"`
	Cleared   bool    `json:"cleared"`
}

func DispatchShipment(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	task, _ := workerclient.TaskFromContext(ctx)

	var in OrderInput
	_ = json.Unmarshal(input, &in)

	carrier := "DHL Express Priority"
	serviceLevel := "Next-Day Air"

	if dep, ok := task.DependencyOutputs["step-enrich-order-telemetry"]; ok && len(dep) > 0 {
		var tel TelemetryData
		if err := json.Unmarshal(dep, &tel); err == nil && tel.Carrier != "" {
			carrier = tel.Carrier
			serviceLevel = tel.ServiceLevel
		}
	}

	var fraud FraudEvaluation
	if dep, ok := task.DependencyOutputs["step-evaluate-fraud-risk"]; ok && len(dep) > 0 {
		_ = json.Unmarshal(dep, &fraud)
	}

	log.Printf("\033[34m[Go Logistics & Shipping]\033[0m Task %s: Dispatching Order %s via %s (%s)...",
		task.TaskID[:8], in.OrderID, carrier, serviceLevel)
	Sleep(ctx, input, 150*time.Millisecond)

	if in.SimulateShipFail {
		log.Printf("\033[31m[Go Logistics & Shipping]\033[0m ERROR: Courier API timeout: warehouse pickup unreachable\033[0m")
		return nil, errors.New("courier dispatch timeout: failed to generate airway bill")
	}

	tracking := fmt.Sprintf("TRK-%s-%d", carrier[:3], time.Now().UnixNano()%10000000)
	dispatch := ShipmentDispatch{
		TrackingNumber:    tracking,
		OrderID:           in.OrderID,
		Carrier:           carrier,
		ServiceLevel:      serviceLevel,
		EstimatedDelivery: time.Now().Add(24 * time.Hour).Format("2006-01-02 15:04 MST"),
		DispatchedAt:      time.Now().UTC(),
		Status:            "DISPATCHED",
		SleepMs:           in.SleepMs,
	}

	log.Printf("\033[32m[Go Logistics & Shipping]\033[0m SUCCESS: Package dispatched! Tracking: %s", tracking)
	return json.Marshal(dispatch)
}

func CancelShipment(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	var dispatch ShipmentDispatch
	_ = json.Unmarshal(input, &dispatch)

	task, _ := workerclient.TaskFromContext(ctx)
	log.Printf("\033[35m[Go Logistics & Shipping - SAGA COMPENSATION]\033[0m Task %s: CANCELLING courier shipment %s (%s)...",
		task.TaskID[:8], dispatch.TrackingNumber, dispatch.Carrier)
	Sleep(ctx, input, 70*time.Millisecond)

	log.Printf("\033[32m[Go Logistics & Shipping - SAGA COMPENSATION]\033[0m SUCCESS: Courier pickup cancelled, airway bill voided!",
	)
	return json.RawMessage(`{"cancelled": true, "airway_bill_voided": true}`), nil
}
