package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/avivklas/plexus-flow/examples/showcase/go/services"
	"github.com/avivklas/plexus-flow/pkg/api"
	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/workerclient"
)

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	switch cmd {
	case "worker":
		runWorker(os.Args[2:])
	case "submit":
		runSubmit(os.Args[2:])
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Usage: go run ./examples/showcase/go [command] [options]

Commands:
  worker    Start the Go activity worker service (connects to plexus-flow over gRPC)
            Options:
              --grpc-addr=127.0.0.1:9090
              --worker-id=go-fulfillment-service
              --domain=fulfillment (fulfillment | all)

  submit    Submit a new order workflow through the Plexus-Flow HTTP API and monitor it
            Options:
              --http-addr=http://127.0.0.1:8080
              --order-id=ORD-9901
              --sleep-ms=100 (pass 0 for instant execution with no delay)
              --simulate-fraud (trigger AI fraud detection rollback)
              --simulate-payment-fail (trigger payment decline rollback)
              --simulate-shipping-fail (trigger courier failure rollback)`)
}

func runWorker(args []string) {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	grpcAddr := fs.String("grpc-addr", "127.0.0.1:9090", "Plexus-Flow gRPC server address")
	workerID := fs.String("worker-id", "go-fulfillment-service", "Worker identifier")
	domain := fs.String("domain", "fulfillment", "Activities domain: 'fulfillment' or 'all'")
	_ = fs.Parse(args)

	fmt.Printf("%s[Go Worker]%s Starting worker %s (domain=%s, target=%s)...\n",
		colorCyan, colorReset, *workerID, *domain, *grpcAddr)

	w := workerclient.New(*grpcAddr, workerclient.Options{
		WorkerID:       *workerID,
		MaxConcurrency: 8,
	})

	reg := w.Registry()

	// Fulfillment activities & compensations
	reg.Register("reserve-inventory", services.ReserveInventory)
	reg.Register("release-inventory", services.ReleaseInventory)
	reg.Register("authorize-payment", services.AuthorizePayment)
	reg.Register("void-authorization", services.VoidAuthorization)
	reg.Register("dispatch-shipment", services.DispatchShipment)
	reg.Register("cancel-shipment", services.CancelShipment)

	if *domain == "all" {
		fmt.Printf("%s[Go Worker]%s Registering AI risk, telemetry, and receipt activities (all-in-one Go mode)...\n",
			colorYellow, colorReset)
		reg.Register("evaluate-fraud-risk", services.EvaluateFraudRiskGo)
		reg.Register("enrich-order-telemetry", services.EnrichOrderTelemetryGo)
		reg.Register("generate-order-receipt", services.GenerateOrderReceiptGo)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Printf("\n%s[Go Worker]%s Shutting down...\n", colorYellow, colorReset)
		cancel()
	}()

	fmt.Printf("%s[Go Worker]%s Connected to Plexus-Flow. Serving activities: %v\n",
		colorGreen, colorReset, reg.Names())

	if err := w.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("Worker exited with error: %v", err)
	}
}

func runSubmit(args []string) {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	httpAddr := fs.String("http-addr", "http://127.0.0.1:8080", "Plexus-Flow HTTP API address")
	orderID := fs.String("order-id", "", "Workflow Order ID")
	sleepMs := fs.Int("sleep-ms", 100, "Activity simulation sleep in milliseconds (0 for instant execution)")
	simFraud := fs.Bool("simulate-fraud", false, "Simulate AI fraud detection failure")
	simPay := fs.Bool("simulate-payment-fail", false, "Simulate payment authorization failure")
	simShip := fs.Bool("simulate-shipping-fail", false, "Simulate shipping courier failure")
	_ = fs.Parse(args)

	if *orderID == "" {
		*orderID = fmt.Sprintf("ORD-%d", time.Now().Unix()%100000)
	}

	client := api.NewClient(*httpAddr)
	ctx := context.Background()

	input := services.OrderInput{
		OrderID:          *orderID,
		CustomerID:       "CUST-8831",
		ItemSKU:          "AI-ACCELERATOR-H200",
		Quantity:         1,
		UnitPrice:        2499.00,
		ShippingAddress:  "742 Evergreen Terrace, Springfield, OR",
		SimulateFraud:    *simFraud,
		SimulatePayFail:  *simPay,
		SimulateShipFail: *simShip,
		SleepMs:          sleepMs,
	}
	rawInput, _ := json.Marshal(input)

	def := flow.WorkflowDefinition{
		Name: "smart-fulfillment-saga",
		Steps: []flow.StepDefinition{
			{
				Name:               "step-reserve-inventory",
				Activity:           "reserve-inventory",
				CompensatingAction: "release-inventory",
				Retries:            2,
				Timeout:            5 * time.Second,
			},
			{
				Name:               "step-authorize-payment",
				Activity:           "authorize-payment",
				CompensatingAction: "void-authorization",
				DependsOn:          []string{"step-reserve-inventory"},
				Retries:            2,
				Timeout:            5 * time.Second,
			},
			{
				Name:      "step-evaluate-fraud-risk",
				Activity:  "evaluate-fraud-risk",
				DependsOn: []string{"step-authorize-payment"},
				Retries:   1,
				Timeout:   5 * time.Second,
			},
			{
				Name:      "step-enrich-order-telemetry",
				Activity:  "enrich-order-telemetry",
				DependsOn: []string{"step-authorize-payment"},
				Retries:   2,
				Timeout:   5 * time.Second,
			},
			{
				Name:               "step-dispatch-shipment",
				Activity:           "dispatch-shipment",
				CompensatingAction: "cancel-shipment",
				DependsOn:          []string{"step-evaluate-fraud-risk", "step-enrich-order-telemetry"},
				Retries:            2,
				Timeout:            5 * time.Second,
			},
			{
				Name:      "step-generate-receipt",
				Activity:  "generate-order-receipt",
				DependsOn: []string{"step-dispatch-shipment"},
				Retries:   2,
				Timeout:   5 * time.Second,
			},
		},
	}

	fmt.Printf("\n%s========================================================================%s\n", colorCyan, colorReset)
	fmt.Printf("%s[Plexus-Flow Showcase]%s Submitting Workflow: %s%s%s\n", colorBold, colorReset, colorYellow, *orderID, colorReset)
	if *simFraud {
		fmt.Printf("%s[Mode]%s %sSIMULATING AI FRAUD DETECTION FAILURE%s (Triggers Automatic Reverse Saga Rollback)\n",
			colorBold, colorReset, colorRed, colorReset)
	} else if *simPay {
		fmt.Printf("%s[Mode]%s %sSIMULATING PAYMENT DECLINE%s (Triggers Automatic Saga Rollback)\n",
			colorBold, colorReset, colorRed, colorReset)
	} else if *simShip {
		fmt.Printf("%s[Mode]%s %sSIMULATING COURIER DISPATCH FAILURE%s (Triggers Automatic Saga Rollback)\n",
			colorBold, colorReset, colorRed, colorReset)
	} else {
		fmt.Printf("%s[Mode]%s %sHAPPY PATH%s (All Go & Python services cooperate to full success)\n",
			colorBold, colorReset, colorGreen, colorReset)
	}
	fmt.Printf("%s========================================================================%s\n\n", colorCyan, colorReset)

	wf, err := client.StartWorkflow(ctx, flowstore.StartWorkflowRequest{
		WorkflowID: *orderID,
		Definition: def,
		Input:      rawInput,
		Metadata: map[string]string{
			"source":   "showcase-launcher",
			"tier":     "enterprise",
			"sleep_ms": strconv.Itoa(*sleepMs),
		},
	})
	if err != nil {
		log.Fatalf("Failed to start workflow: %v", err)
	}

	fmt.Printf("%s[Engine]%s Workflow created: ID=%s RunID=%s Status=%s\n",
		colorBlue, colorReset, wf.WorkflowID, wf.RunID, wf.Status)
	fmt.Printf("%s[Dashboard]%s Inspect live execution at: %shttp://127.0.0.1:8080%s\n\n",
		colorPurple, colorReset, colorBold, colorReset)

	// Poll until terminal
	prevSteps := map[string]flow.StepStatus{}
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		current, err := client.GetWorkflow(ctx, *orderID)
		if err == nil {
			for name, step := range current.Steps {
				if prev, ok := prevSteps[name]; !ok || prev != step.Status {
					prevSteps[name] = step.Status
					statusColor := colorBlue
					switch step.Status {
					case flow.StepStatusCompleted:
						statusColor = colorGreen
					case flow.StepStatusFailed:
						statusColor = colorRed
					case flow.StepStatusCompensating:
						statusColor = colorPurple
					case flow.StepStatusCompensated:
						statusColor = colorYellow
					}
					fmt.Printf("  • %-28s -> %s%-12s%s (Attempt: %d)\n",
						name, statusColor, step.Status, colorReset, step.Attempt)
					if step.Error != "" {
						fmt.Printf("    %s└─ Error: %s (Code: %s)%s\n",
							colorRed, step.Error, step.ErrorCode, colorReset)
					}
				}
			}

			if current.IsTerminal() {
				fmt.Printf("\n%s========================================================================%s\n", colorCyan, colorReset)
				switch current.Status {
				case flow.StatusCompleted:
					fmt.Printf("%s[RESULT]%s SAGA FINISHED SUCCESSFULLY: %s%s%s\n",
						colorGreen, colorReset, colorBold, current.Status, colorReset)
					fmt.Printf("All 6 steps executed across Go and Python workers with Exactly-Once Semantics!\n")
				case flow.StatusCompensated:
					fmt.Printf("%s[RESULT]%s SAGA ROLLED BACK CLEANLY: %s%s%s\n",
						colorYellow, colorReset, colorBold, current.Status, colorReset)
					fmt.Printf("Failed step triggered automatic reverse-order compensation!\n")
					fmt.Printf("Error Code: %s%s%s | Zero orphaned state.\n",
						colorRed, current.ErrorCode, colorReset)
				default:
					fmt.Printf("%s[RESULT]%s Workflow ended with terminal status: %s\n",
						colorYellow, colorReset, current.Status)
				}
				fmt.Printf("%s========================================================================%s\n\n", colorCyan, colorReset)
				return
			}
		}
		time.Sleep(150 * time.Millisecond)
	}

	fmt.Printf("%s[Warning]%s Timed out waiting for workflow completion.\n", colorYellow, colorReset)
}
