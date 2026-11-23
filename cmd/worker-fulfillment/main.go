package main

import (
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/config"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/worker"
)

func main() {
	cfg := config.Load()
	worker.RunStandaloneWorker("Fulfillment", cfg.FulfillmentWorkerAddr)
}
