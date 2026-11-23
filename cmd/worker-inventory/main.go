package main

import (
	"github.com/DaniloNogueira-Silva/go-distributed_workflow-orchestrator/config"
	"github.com/DaniloNogueira-Silva/go-distributed_workflow-orchestrator/internal/worker"
)

func main() {
	cfg := config.Load()
	worker.RunStandaloneWorker("Inventory", cfg.InventoryWorkerAddr)
}
