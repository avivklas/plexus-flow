package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/avivklas/plexus-flow/pkg/api"
	"github.com/avivklas/plexus-flow/pkg/blueprint"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/graphflow"
	"github.com/avivklas/plexus-flow/pkg/worker"
)

const (
	reset  = "\033[0m"
	bold   = "\033[1m"
	green  = "\033[32m"
	cyan   = "\033[36m"
	yellow = "\033[33m"
	purple = "\033[35m"
)

func main() {
	httpAddr := flag.String("http", "127.0.0.1:8080", "HTTP server address for the visualizer")
	flag.Parse()

	banner := `
  ____  _                      _____ _                 
 |  _ \| |                    |  ___| | _____      __  
 | |_) | | _____  ___   _ ___ | |_  | |/ _ \ \ /\ / /  
 |  __/| |/ _ \ \/ / | | / __||  _| | | (_) \ V  V /   
 |_|   |_|\___/_/\_\ |_| \__ \|_|   |_|\___/ \_/\_/    
                    \__,_|___/                         
       Workflow Blueprints & Runtime Observability
`
	fmt.Printf("%s%s%s\n", cyan, banner, reset)

	// 1. Initialize activity registry and register blueprint activities
	reg := worker.NewRegistry()
	blueprint.RegisterBlueprintActivities(reg)

	// 2. Initialize embedded Raft coordinator
	log.Printf("%s[Coordinator]%s Bootstrapping embedded Raft consensus cluster...", cyan, reset)
	coord, err := graphflow.NewEmbeddedCoordinator(reg)
	if err != nil {
		log.Fatalf("failed to initialize embedded coordinator: %v", err)
	}
	defer coord.Close()
	log.Printf("%s[Consensus]%s Quorum established with sequential local read consistency.", green, reset)

	// 3. Initialize HTTP API & Visualizer Server
	bpReg := blueprint.NewRegistry()
	srv := api.NewServer(*httpAddr, coord, "node-showcase")
	srv.SetBlueprintRegistry(bpReg)

	if err := srv.Start(); err != nil {
		log.Fatalf("failed to start server on %s: %v", *httpAddr, err)
	}
	defer srv.Close()

	log.Printf("%s[Visualizer]%s Web Dashboard ready at: %s%shttp://%s%s", green, reset, bold, cyan, srv.Addr(), reset)
	log.Printf("%s[Visualizer]%s Available Blueprints: 🛒 E-Commerce Order Fulfillment Saga, 🏦 Consumer Loan Underwriting", yellow, reset)

	// 4. Seed initial runs (1 Happy Path + 1 Compensated Rollback) so the dashboard is immediately populated
	ctx := context.Background()
	ecomBp, _ := bpReg.Get("order-fulfillment-saga")

	log.Printf("%s[Showcase]%s Seeding live happy path run (ORD-7821)...", cyan, reset)
	_, _ = coord.StartWorkflow(ctx, flowstore.StartWorkflowRequest{
		WorkflowID: "ORD-7821",
		Definition: ecomBp.Definition,
		Input:      ecomBp.DefaultInput,
	})

	time.Sleep(1200 * time.Millisecond)

	log.Printf("%s[Showcase]%s Seeding simulated failure & auto-rollback run (ORD-7822-FAIL)...", purple, reset)
	failPayload := json.RawMessage(`{
		"order_id": "ORD-7822-FAIL",
		"customer_name": "Marcus Vance",
		"total_amount": 4500.00,
		"simulate_fail": "charge-payment",
		"fail_reason": "card_declined: insufficient funds on account"
	}`)
	_, _ = coord.StartWorkflow(ctx, flowstore.StartWorkflowRequest{
		WorkflowID: "ORD-7822-FAIL",
		Definition: ecomBp.Definition,
		Input:      failPayload,
	})

	// Wait for terminal state on seeded runs
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		inst1, ok1 := coord.GetWorkflow("ORD-7821")
		inst2, ok2 := coord.GetWorkflow("ORD-7822-FAIL")
		if ok1 && ok2 && inst1.IsTerminal() && inst2.IsTerminal() {
			log.Printf("%s[Showcase]%s Seed runs completed! Ready for exploration.", green, reset)
			log.Printf("  • ORD-7821:       Status = %s%s%s (All steps passed)", green, inst1.Status, reset)
			log.Printf("  • ORD-7822-FAIL:  Status = %s%s%s (Reverse LIFO compensation executed)", purple, inst2.Status, reset)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("\n%s%s🚀 Open http://%s in your browser to interact with Blueprints and Live Runtime!%s\n\n", bold, green, srv.Addr(), reset)

	// Graceful shutdown on SIGINT/SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Printf("\n%s[Shutdown]%s Closing Plexus-Flow Showcase server...", yellow, reset)
}
