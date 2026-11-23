package app

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/config"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/eventsourcing"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/gateway"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/limiter"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/orchestrator"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/worker"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const workerRoleEnv = "ORCHESTRATOR_WORKER_ROLE"

// Run launches the cluster: if invoked as a worker child process, runs that isolated worker;
// otherwise starts/connects to the 4 separate worker OS processes and boots the Gateway + Orchestrator.
func Run() {
	if role := os.Getenv(workerRoleEnv); role != "" {
		cfg := config.Load()
		worker.RunStandaloneWorker(role, cfg.WorkerAddresses()[role])
		return
	}
	RunGateway(true)
}

// RunGateway boots the Edge API Gateway and Orchestrator Engine, connecting over TCP/gRPC
// to the 4 isolated worker servers (:50051, :50052, :50053, :50054).
// When spawnMissingWorkers is true, any worker port not already listening is launched as a separate OS process.
func RunGateway(spawnMissingWorkers bool) {
	cfg := config.Load()

	// 1. Connect to Redis for Inbound Rate Limiting
	log.Printf("[Bootstrap | PID:%d] Connecting to Redis at %s...", os.Getpid(), cfg.RedisAddr)
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       0,
	})
	defer func() { _ = rdb.Close() }()

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer pingCancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Printf("[Bootstrap Warning] Redis ping failed (%v). Ensure Redis is running.", err)
	} else {
		log.Println("[Bootstrap] Connected to Redis.")
	}

	// 2. Connect to PostgreSQL for Event Sourcing & Domain Store inspection
	var repo eventsourcing.Repository
	var domainStore worker.DomainStore

	db, err := sql.Open("postgres", cfg.PostgresDSN())
	if err == nil {
		dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dbCancel()
		if pingErr := db.PingContext(dbCtx); pingErr == nil {
			pgRepo := eventsourcing.NewPostgresRepository(db)
			pgDomain := worker.NewPostgresDomainStore(db)

			if schemaErr := pgRepo.EnsureSchema(dbCtx); schemaErr != nil {
				log.Printf("[Bootstrap Warning] Failed to apply Event Sourcing schema: %v", schemaErr)
			} else if domainErr := pgDomain.EnsureSchemaAndSeed(dbCtx); domainErr != nil {
				log.Printf("[Bootstrap Warning] Failed to apply Domain Store schema & seed: %v", domainErr)
			} else {
				log.Println("[Bootstrap] Connected to PostgreSQL (Event Sourcing & Domain Worker tables verified and seeded).")
				repo = pgRepo
				domainStore = pgDomain
			}
		} else {
			log.Printf("[Bootstrap Warning] PostgreSQL unavailable (%v). Falling back to In-Memory Event & Domain Stores.", pingErr)
			_ = db.Close()
		}
	}
	if repo == nil || domainStore == nil {
		repo = eventsourcing.NewInMemoryRepository()
		domainStore = worker.NewInMemoryDomainStore()
	} else if db != nil {
		defer func() { _ = db.Close() }()
	}

	// 3. Configure Inbound Rate Limiter
	inboundLimiter := limiter.NewTokenBucketLimiter(rdb, limiter.Config{
		Capacity:         cfg.RateLimitCapacity,
		RefillRatePerSec: cfg.RateLimitRefill,
		TTLSeconds:       60,
	})

	// 4. Ensure the 4 separate Worker Processes are running (or spawn them as distinct OS processes)
	workerAddrs := cfg.WorkerAddresses()
	orderedSteps := []string{"Payment", "Inventory", "Fulfillment", "Notification"}
	var spawnedProcesses []*exec.Cmd

	if spawnMissingWorkers {
		exePath, exeErr := os.Executable()
		for _, stepName := range orderedSteps {
			addr := workerAddrs[stepName]
			if isTCPPortOpen(addr) {
				log.Printf("[Bootstrap] Detected external %s worker already listening on %s", stepName, addr)
				continue
			}
			if exeErr != nil {
				log.Fatalf("[Bootstrap] Cannot determine executable path to spawn worker %s: %v", stepName, exeErr)
			}

			cmd := exec.Command(exePath)
			cmd.Env = append(os.Environ(), workerRoleEnv+"="+stepName)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr

			if startErr := cmd.Start(); startErr != nil {
				log.Fatalf("[Bootstrap] Failed to spawn isolated OS process for %s worker: %v", stepName, startErr)
			}
			spawnedProcesses = append(spawnedProcesses, cmd)
			log.Printf("[Bootstrap] Spawned isolated OS process for %s worker (Child PID:%d -> %s)",
				stepName, cmd.Process.Pid, addr)
		}

		// Wait briefly for spawned worker processes to bind their TCP ports
		for _, stepName := range orderedSteps {
			waitForTCPPort(workerAddrs[stepName], 3*time.Second)
		}
	}

	defer func() {
		for _, cmd := range spawnedProcesses {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
				_, _ = cmd.Process.Wait()
			}
		}
	}()

	// 5. Dial 4 independent gRPC Client Connections (one per domain worker process)
	grpcConns := make(map[string]grpc.ClientConnInterface, len(orderedSteps))
	var rawConns []*grpc.ClientConn

	for _, stepName := range orderedSteps {
		addr := workerAddrs[stepName]
		conn, dialErr := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if dialErr != nil {
			log.Fatalf("[Bootstrap] Failed to dial %s gRPC worker at %s: %v", stepName, addr, dialErr)
		}
		grpcConns[stepName] = conn
		rawConns = append(rawConns, conn)
		log.Printf("[Bootstrap] Orchestrator wired to %s Worker gRPC endpoint -> %s", stepName, addr)
	}
	defer func() {
		for _, c := range rawConns {
			_ = c.Close()
		}
	}()

	workerClient := worker.NewMultiGRPCClientAdapter(grpcConns)
	blueprint := orchestrator.NewCheckoutSagaBlueprint(100 * time.Millisecond)
	engine := orchestrator.NewEngine(repo, blueprint, workerClient)

	// 6. Start Edge API Gateway HTTP Server
	mux := http.NewServeMux()
	gwHandler := gateway.NewHandlerWithEngine(inboundLimiter, engine, domainStore)
	gwHandler.RegisterRoutes(mux)

	httpServer := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[Bootstrap | PID:%d] Edge API Gateway listening on http://localhost:%s", os.Getpid(), cfg.Port)
		if serveErr := httpServer.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Fatalf("[Bootstrap] HTTP server error: %v", serveErr)
		}
	}()

	<-shutdownChan
	log.Println("[Bootstrap] Shutting down Gateway and worker processes gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	_ = httpServer.Shutdown(shutdownCtx)
	log.Println("[Bootstrap] All services stopped cleanly.")
}

func isTCPPortOpen(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func waitForTCPPort(addr string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if isTCPPortOpen(addr) {
			return
		}
		time.Sleep(40 * time.Millisecond)
	}
}
