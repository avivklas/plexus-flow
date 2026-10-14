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

type PaymentAuthorization struct {
	TransactionID    string    `json:"transaction_id"`
	OrderID          string    `json:"order_id"`
	AuthorizedAmount float64   `json:"authorized_amount"`
	Currency         string    `json:"currency"`
	Status           string    `json:"status"`
	AuthCode         string    `json:"auth_code"`
	AuthorizedAt     time.Time `json:"authorized_at"`
}

func AuthorizePayment(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	task, _ := workerclient.TaskFromContext(ctx)

	var in OrderInput
	_ = json.Unmarshal(input, &in)

	amount := in.UnitPrice * float64(in.Quantity)
	if dep, ok := task.DependencyOutputs["step-reserve-inventory"]; ok && len(dep) > 0 {
		var res InventoryReservation
		if err := json.Unmarshal(dep, &res); err == nil && res.Subtotal > 0 {
			amount = res.Subtotal
		}
	}
	if amount <= 0 {
		amount = 1299.99
	}

	log.Printf("\033[34m[Go Payment Gateway]\033[0m Task %s: Pre-authorizing $%.2f USD for Order %s...",
		task.TaskID[:8], amount, in.OrderID)
	time.Sleep(140 * time.Millisecond)

	if in.SimulatePayFail {
		log.Printf("\033[31m[Go Payment Gateway]\033[0m ERROR: Payment pre-authorization declined: insufficient funds\033[0m")
		return nil, errors.New("card pre-authorization declined: insufficient credit limit")
	}

	txID := fmt.Sprintf("TX-AUTH-%d", time.Now().UnixNano()%1000000)
	auth := PaymentAuthorization{
		TransactionID:    txID,
		OrderID:          in.OrderID,
		AuthorizedAmount: amount,
		Currency:         "USD",
		Status:           "AUTHORIZED",
		AuthCode:         fmt.Sprintf("AUTH-%d", time.Now().Unix()%100000),
		AuthorizedAt:     time.Now().UTC(),
	}

	log.Printf("\033[32m[Go Payment Gateway]\033[0m SUCCESS: Pre-authorization locked! (TxID: %s, AuthCode: %s)",
		txID, auth.AuthCode)
	return json.Marshal(auth)
}

func VoidAuthorization(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	var auth PaymentAuthorization
	_ = json.Unmarshal(input, &auth)

	task, _ := workerclient.TaskFromContext(ctx)
	log.Printf("\033[35m[Go Payment Gateway - SAGA COMPENSATION]\033[0m Task %s: VOIDING authorization %s ($%.2f USD)...",
		task.TaskID[:8], auth.TransactionID, auth.AuthorizedAmount)
	time.Sleep(90 * time.Millisecond)

	log.Printf("\033[32m[Go Payment Gateway - SAGA COMPENSATION]\033[0m SUCCESS: Pre-authorization voided. Funds released back to customer card!",
	)
	return json.RawMessage(`{"voided": true, "funds_released": true}`), nil
}
