package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/avivklas/plexus-flow/pkg/workerclient"
)

type InventoryReservation struct {
	ReservationID string    `json:"reservation_id"`
	OrderID       string    `json:"order_id"`
	CustomerID    string    `json:"customer_id"`
	SKU           string    `json:"sku"`
	Quantity      int       `json:"quantity"`
	Subtotal      float64   `json:"subtotal"`
	Status        string    `json:"status"`
	ReservedAt    time.Time `json:"reserved_at"`
}

type OrderInput struct {
	OrderID         string  `json:"order_id"`
	CustomerID      string  `json:"customer_id"`
	ItemSKU         string  `json:"item_sku"`
	Quantity        int     `json:"quantity"`
	UnitPrice       float64 `json:"unit_price"`
	ShippingAddress string  `json:"shipping_address"`
	SimulateFraud   bool    `json:"simulate_fraud,omitempty"`
	SimulatePayFail bool    `json:"simulate_payment_fail,omitempty"`
	SimulateShipFail bool   `json:"simulate_shipping_fail,omitempty"`
}

func ReserveInventory(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	var in OrderInput
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, fmt.Errorf("invalid order input: %w", err)
	}
	if in.Quantity <= 0 {
		in.Quantity = 1
	}
	if in.UnitPrice <= 0 {
		in.UnitPrice = 1299.99
	}
	if in.ItemSKU == "" {
		in.ItemSKU = "QUANTUM-GPU-9000"
	}

	task, _ := workerclient.TaskFromContext(ctx)
	log.Printf("\033[34m[Go Inventory Service]\033[0m Task %s: Reserving %d x %s for Order %s...",
		task.TaskID[:8], in.Quantity, in.ItemSKU, in.OrderID)
	time.Sleep(120 * time.Millisecond)

	subtotal := float64(in.Quantity) * in.UnitPrice
	resID := fmt.Sprintf("RES-%d", time.Now().UnixNano()%1000000)

	res := InventoryReservation{
		ReservationID: resID,
		OrderID:       in.OrderID,
		CustomerID:    in.CustomerID,
		SKU:           in.ItemSKU,
		Quantity:      in.Quantity,
		Subtotal:      subtotal,
		Status:        "RESERVED",
		ReservedAt:    time.Now().UTC(),
	}

	log.Printf("\033[32m[Go Inventory Service]\033[0m SUCCESS: Inventory reserved (Hold ID: %s, Subtotal: $%.2f)",
		resID, subtotal)
	return json.Marshal(res)
}

func ReleaseInventory(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	var res InventoryReservation
	_ = json.Unmarshal(input, &res)

	task, _ := workerclient.TaskFromContext(ctx)
	log.Printf("\033[35m[Go Inventory Service - SAGA COMPENSATION]\033[0m Task %s: RELEASING inventory hold %s (SKU: %s, Qty: %d)...",
		task.TaskID[:8], res.ReservationID, res.SKU, res.Quantity)
	time.Sleep(80 * time.Millisecond)

	log.Printf("\033[32m[Go Inventory Service - SAGA COMPENSATION]\033[0m SUCCESS: Inventory restocked and released safely!",
	)
	return json.RawMessage(`{"released": true, "restocked": true}`), nil
}
