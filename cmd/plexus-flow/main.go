package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/avivklas/plexus-flow/pkg/api"
	"github.com/avivklas/plexus-flow/pkg/flow"
	"github.com/avivklas/plexus-flow/pkg/flowstore"
	"github.com/avivklas/plexus-flow/pkg/graphflow"
	"github.com/avivklas/plexus-flow/pkg/worker"
	"github.com/avivklas/plexus/pkg/dedup"
	"github.com/avivklas/plexus/pkg/machine"
)

// ANSI color codes for rich CLI presentation
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
	var (
		nodeID          = flag.String("id", "node-1", "Unique node identifier in cluster")
		httpAddr        = flag.String("http-addr", "127.0.0.1:8080", "HTTP REST API listen address")
		raftAddr        = flag.String("raft-addr", "127.0.0.1:9000", "Raft consensus listen address for workflow state")
		actRaftAddr     = flag.String("act-raft-addr", "", "Raft consensus listen address for activity dispatcher (default: raft-addr + 1)")
		dataDir         = flag.String("data-dir", "./data", "Directory for persistent Raft logs and dedup records")
		joinAddrs       = flag.String("join", "", "Comma-separated list of peer Raft addresses to join")
		bootstrap       = flag.Bool("bootstrap", false, "Bootstrap this node as the initial cluster leader")
		runExample      = flag.Bool("run-example", false, "Run the built-in Order Processing Saga demonstration")
		simulateFailure = flag.Bool("simulate-failure", false, "Simulate a step failure to demonstrate automatic Saga rollback")
		failStep        = flag.String("fail-step", "charge-card", "Which step to fail when simulating: 'charge-card' or 'ship-item'")
	)
	flag.Parse()

	// Compute activity raft address if not specified
	if *actRaftAddr == "" {
		host, portStr, err := net.SplitHostPort(*raftAddr)
		if err == nil {
			if port, err := strconv.Atoi(portStr); err == nil {
				*actRaftAddr = net.JoinHostPort(host, strconv.Itoa(port+1))
			}
		}
		if *actRaftAddr == "" {
			*actRaftAddr = "127.0.0.1:9001"
		}
	}

	// Auto-bootstrap if no join addresses given
	if !*bootstrap && *joinAddrs == "" {
		*bootstrap = true
	}

	printBanner()

	log.Printf("%s[Plexus-Flow]%s Node ID: %s%s%s", colorCyan, colorReset, colorBold, *nodeID, colorReset)
	log.Printf("%s[Plexus-Flow]%s Workflow Raft: %s", colorCyan, colorReset, *raftAddr)
	log.Printf("%s[Plexus-Flow]%s Activity Raft: %s", colorCyan, colorReset, *actRaftAddr)
	log.Printf("%s[Plexus-Flow]%s HTTP API:      http://%s", colorCyan, colorReset, *httpAddr)
	log.Printf("%s[Plexus-Flow]%s Data Dir:      %s", colorCyan, colorReset, *dataDir)

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("failed to create data dir: %v", err)
	}

	// 1. Initialize Activity Registry
	registry := worker.NewRegistry()
	registerOrderSagaActivities(registry, *simulateFailure, *failStep)

	// 2. Initialize Stores
	flowStore := flowstore.New()
	actStore := worker.NewActivityStore()

	dedupPath := filepath.Join(*dataDir, "dedup.db")
	var dedupStore dedup.Store
	fStore, err := dedup.NewFileStore(dedupPath)
	if err != nil {
		log.Printf("%s[Warning]%s Failed to init persistent dedup store, falling back to memory: %v", colorYellow, colorReset, err)
		dedupStore = dedup.NewMemoryStore()
	} else {
		dedupStore = fStore
	}

	// 3. Initialize Upstream Workflow Raft Machine
	upDir := filepath.Join(*dataDir, "workflow-raft")
	upCfg := machine.DefaultConfig(
		machine.MachineID(*nodeID+"-wf"),
		&machine.Node{ID: *nodeID + "-wf", Address: *raftAddr, Voter: true},
		upDir,
	)
	upCfg.Bootstrap = *bootstrap
	if *joinAddrs != "" {
		for _, addr := range strings.Split(*joinAddrs, ",") {
			trimmed := strings.TrimSpace(addr)
			if trimmed != "" {
				upCfg.JoinAddrs = append(upCfg.JoinAddrs, trimmed)
			}
		}
	}
	upMachine := machine.NewRaftMachine(upCfg)
	upMachine.Register(flowStore)

	// 4. Initialize Downstream Activity Raft Machine
	downDir := filepath.Join(*dataDir, "activity-raft")
	downCfg := machine.DefaultConfig(
		machine.MachineID(*nodeID+"-act"),
		&machine.Node{ID: *nodeID + "-act", Address: *actRaftAddr, Voter: true},
		downDir,
	)
	downCfg.Bootstrap = *bootstrap
	downMachine := machine.NewRaftMachine(downCfg)
	downMachine.Register(actStore)

	// Start consensus engines
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("%s[Consensus]%s Starting Raft consensus engines...", colorBlue, colorReset)
	if err := upMachine.Start(ctx); err != nil {
		log.Fatalf("failed to start workflow raft machine: %v", err)
	}
	defer upMachine.Stop()

	if err := downMachine.Start(ctx); err != nil {
		log.Fatalf("failed to start activity raft machine: %v", err)
	}
	defer downMachine.Stop()

	// Wait for leader election
	log.Printf("%s[Consensus]%s Waiting for Raft leadership...", colorBlue, colorReset)
	deadline := time.Now().Add(10 * time.Second)
	for !upMachine.IsLeader() || !downMachine.IsLeader() {
		if time.Now().After(deadline) {
			log.Fatalf("timed out waiting for Raft leader election")
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.Printf("%s[Consensus]%s Node elected leader on both machines! Raft quorum active.", colorGreen, colorReset)

	// 5. Initialize Raft Graph Coordinator
	coord, err := graphflow.NewCoordinator(graphflow.Config{
		GraphName:     "plexus-flow-" + *nodeID,
		Upstream:      upMachine,
		Downstream:    downMachine,
		FlowStore:     flowStore,
		ActivityStore: actStore,
		DedupStore:    dedupStore,
		Registry:      registry,
	})
	if err != nil {
		log.Fatalf("failed to initialize Raft Graph coordinator: %v", err)
	}
	defer coord.Close()

	if err := coord.RegisterFlow(orderSagaDefinition()); err != nil {
		log.Fatalf("failed to register flow: %v", err)
	}

	// 6. Start HTTP API Server
	apiServer := api.NewServer(*httpAddr, coord, *nodeID)
	if err := apiServer.Start(); err != nil {
		log.Fatalf("failed to start HTTP API server on %s: %v", *httpAddr, err)
	}
	defer apiServer.Close()

	log.Printf("%s[API]%s HTTP API listening on %s%s%s", colorGreen, colorReset, colorBold, apiServer.Addr(), colorReset)

	// 7. If --run-example is set, launch the Order Processing Saga!
	if *runExample {
		go executeExampleSaga(coord, *simulateFailure, *failStep)
	}

	// Wait for shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Printf("\n%s[Shutdown]%s Shutting down Plexus-Flow gracefully...", colorYellow, colorReset)
}

func printBanner() {
	banner := `
  ____  _                      _____ _                 
 |  _ \| |                    |  ___| | _____      __  
 | |_) | | _____  ___   _ ___ | |_  | |/ _ \ \ /\ / /  
 |  __/| |/ _ \ \/ / | | / __||  _| | | (_) \ V  V /   
 |_|   |_|\___/_/\_\ |_| \__ \|_|   |_|\___/ \_/\_/    
                    \__,_|___/                         
   Distributed Sagas & Workflows without Temporal or Kafka
`
	fmt.Printf("%s%s%s\n", colorCyan, banner, colorReset)
}

func registerOrderSagaActivities(r *worker.Registry, simFail bool, failStepName string) {
	// Step 1: Reserve Inventory
	r.Register("reserve-inventory", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		log.Printf("%s  [ACTIVITY] reserve-inventory -> RESERVING item ITEM-42 for Order...%s", colorBlue, colorReset)
		time.Sleep(100 * time.Millisecond)
		log.Printf("%s  [ACTIVITY] reserve-inventory -> SUCCESS: Inventory reserved!%s", colorGreen, colorReset)
		return json.RawMessage(`{"reserved": true, "sku": "ITEM-42", "qty": 1}`), nil
	})

	// Compensation 1: Release Inventory
	r.Register("release-inventory", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		log.Printf("%s  [COMPENSATION] release-inventory -> RELEASING item ITEM-42 back to stock...%s", colorPurple, colorReset)
		time.Sleep(80 * time.Millisecond)
		log.Printf("%s  [COMPENSATION] release-inventory -> SUCCESS: Inventory released back to stock!%s", colorGreen, colorReset)
		return json.RawMessage(`{"released": true}`), nil
	})

	// Step 2: Charge Card
	r.Register("charge-card", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		log.Printf("%s  [ACTIVITY] charge-card -> CHARGING credit card $149.99...%s", colorBlue, colorReset)
		time.Sleep(120 * time.Millisecond)
		if simFail && failStepName == "charge-card" {
			log.Printf("%s  [ACTIVITY] charge-card -> ERROR: Card declined: insufficient funds!%s", colorRed, colorReset)
			return nil, errors.New("card declined: insufficient funds")
		}
		log.Printf("%s  [ACTIVITY] charge-card -> SUCCESS: Card charged $149.99!%s", colorGreen, colorReset)
		return json.RawMessage(`{"charged": true, "amount": 149.99, "tx_id": "TX-998877"}`), nil
	})

	// Compensation 2: Refund Card
	r.Register("refund-card", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		log.Printf("%s  [COMPENSATION] refund-card -> REFUNDING $149.99 to credit card...%s", colorPurple, colorReset)
		time.Sleep(80 * time.Millisecond)
		log.Printf("%s  [COMPENSATION] refund-card -> SUCCESS: Full refund processed!%s", colorGreen, colorReset)
		return json.RawMessage(`{"refunded": true}`), nil
	})

	// Step 3: Ship Item
	r.Register("ship-item", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		log.Printf("%s  [ACTIVITY] ship-item -> CREATING shipping label with courier...%s", colorBlue, colorReset)
		time.Sleep(100 * time.Millisecond)
		if simFail && failStepName == "ship-item" {
			log.Printf("%s  [ACTIVITY] ship-item -> ERROR: Shipping courier service unavailable!%s", colorRed, colorReset)
			return nil, errors.New("shipping courier service unavailable")
		}
		log.Printf("%s  [ACTIVITY] ship-item -> SUCCESS: Package dispatched! Tracking: TRK-554433%s", colorGreen, colorReset)
		return json.RawMessage(`{"shipped": true, "tracking": "TRK-554433"}`), nil
	})

	// Compensation 3: Cancel Shipment
	r.Register("cancel-shipment", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		log.Printf("%s  [COMPENSATION] cancel-shipment -> VOIDING shipping label...%s", colorPurple, colorReset)
		time.Sleep(60 * time.Millisecond)
		log.Printf("%s  [COMPENSATION] cancel-shipment -> SUCCESS: Shipping label voided!%s", colorGreen, colorReset)
		return json.RawMessage(`{"cancelled": true}`), nil
	})
}

// orderSagaDefinition is the demo flow: reserve stock, charge, then ship, with
// every step undone in reverse order if a later one fails.
func orderSagaDefinition() flow.WorkflowDefinition {
	return flow.WorkflowDefinition{
		Name: "order-fulfillment-saga",
		Steps: []flow.StepDefinition{
			{Name: "reserve-inventory", Activity: "reserve-inventory", CompensatingAction: "release-inventory"},
			{Name: "charge-card", Activity: "charge-card", CompensatingAction: "refund-card"},
			{Name: "ship-item", Activity: "ship-item", CompensatingAction: "cancel-shipment"},
		},
	}
}

func executeExampleSaga(coord *graphflow.Coordinator, simFail bool, failStepName string) {
	time.Sleep(500 * time.Millisecond)

	fmt.Println()
	log.Printf("%s========================================================================%s", colorCyan, colorReset)
	log.Printf("%s[DEMO] Starting Order Processing Saga Demonstration...%s", colorBold, colorReset)
	if simFail {
		log.Printf("%s[DEMO] Mode: SIMULATED FAILURE on step '%s' (Triggering Automatic Saga Compensation)%s", colorYellow, failStepName, colorReset)
	} else {
		log.Printf("%s[DEMO] Mode: HAPPY PATH (All steps succeed)%s", colorGreen, colorReset)
	}
	log.Printf("%s========================================================================%s", colorCyan, colorReset)

	def := orderSagaDefinition()

	orderID := fmt.Sprintf("ORD-%d", time.Now().Unix()%10000)
	inputData := map[string]any{
		"order_id": orderID,
		"customer": "Alice Smith",
		"item":     "MacBook Pro M4",
		"price":    149.99,
	}
	inputBytes, _ := json.Marshal(inputData)

	ctx := context.Background()
	wf, err := coord.StartWorkflow(ctx, flowstore.StartWorkflowRequest{
		WorkflowID: orderID,
		Definition: def,
		Input:      inputBytes,
		Metadata: map[string]string{
			"channel": "web",
		},
	})
	if err != nil {
		log.Printf("[DEMO] Error starting workflow: %v", err)
		return
	}

	log.Printf("%s[DEMO] Workflow started: ID=%s RunID=%s%s", colorCyan, wf.WorkflowID, wf.RunID, colorReset)

	// Poll until terminal status
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		inst, exists := coord.GetWorkflow(orderID)
		if exists && inst.IsTerminal() {
			fmt.Println()
			log.Printf("%s========================================================================%s", colorCyan, colorReset)
			if inst.Status == flow.StatusCompleted {
				log.Printf("%s[DEMO RESULT] SAGA FINISHED SUCCESSFULLY: Status = %s%s", colorGreen, inst.Status, colorReset)
				log.Printf("%s[DEMO RESULT] All steps executed and committed via Raft Graph with EOS!%s", colorGreen, colorReset)
			} else if inst.Status == flow.StatusCompensated {
				log.Printf("%s[DEMO RESULT] SAGA ROLLED BACK SAFELY: Status = %s%s", colorPurple, inst.Status, colorReset)
				log.Printf("%s[DEMO RESULT] Failed step triggered automatic reverse-order compensation!%s", colorPurple, colorReset)
				log.Printf("%s[DEMO RESULT] No orphaned transactions. System returned to clean state.%s", colorPurple, colorReset)
			} else {
				log.Printf("%s[DEMO RESULT] Status: %s (Error: %s)%s", colorRed, inst.Status, inst.Error, colorReset)
			}
			log.Printf("%s========================================================================%s\n", colorCyan, colorReset)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("[DEMO] Timed out waiting for saga to finish")
}
