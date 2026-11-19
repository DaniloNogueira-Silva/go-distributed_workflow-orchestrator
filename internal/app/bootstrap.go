package app

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/config"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/eventsourcing"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/gateway"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/limiter"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/orchestrator"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/worker"
	workerv1 "github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/proto/worker/v1"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Run wires and runs the Edge API Gateway, Orchestrator Engine, Event Store, Domain Store, and gRPC Workers.
func Run() {
	cfg := config.Load()

	// 1. Connect to Redis for Inbound & Outbound Rate Limiting
	log.Printf("[Bootstrap] Connecting to Redis at %s...", cfg.RedisAddr)
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

	// 2. Connect to PostgreSQL for Event Sourcing & Worker Domain Tables (fallback to In-Memory if unavailable)
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

	// 3. Configure Inbound & Outbound Token Bucket Rate Limiters
	inboundLimiter := limiter.NewTokenBucketLimiter(rdb, limiter.Config{
		Capacity:         cfg.RateLimitCapacity,
		RefillRatePerSec: cfg.RateLimitRefill,
		TTLSeconds:       60,
	})

	outboundBucket := limiter.NewTokenBucketLimiter(rdb, limiter.Config{
		Capacity:         cfg.OutboundRateLimitCapacity,
		RefillRatePerSec: cfg.OutboundRateLimitRefill,
		TTLSeconds:       60,
	})
	outboundLimiter := limiter.NewOutboundLimiter(outboundBucket, 50*time.Millisecond, 200)

	// 4. Start gRPC Worker Server (Payment, Inventory, Fulfillment, Notification backed by DomainStore)
	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("[Bootstrap] Failed to listen on gRPC address %s: %v", cfg.GRPCAddr, err)
	}

	grpcServer := grpc.NewServer()
	workerGRPCService := worker.NewGRPCServer(outboundLimiter, domainStore)
	workerv1.RegisterWorkerServiceServer(grpcServer, workerGRPCService)

	go func() {
		log.Printf("[Bootstrap] gRPC Worker Server listening on %s", cfg.GRPCAddr)
		if serveErr := grpcServer.Serve(grpcListener); serveErr != nil {
			log.Fatalf("[Bootstrap] gRPC server error: %v", serveErr)
		}
	}()

	// 5. Connect Orchestrator Engine to gRPC Workers
	grpcConn, err := grpc.NewClient(cfg.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[Bootstrap] Failed to create gRPC client connection: %v", err)
	}
	defer func() { _ = grpcConn.Close() }()

	workerClient := worker.NewGRPCClientAdapter(grpcConn)
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
		log.Printf("[Bootstrap] Edge API Gateway listening on http://localhost:%s", cfg.Port)
		if serveErr := httpServer.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Fatalf("[Bootstrap] HTTP server error: %v", serveErr)
		}
	}()

	<-shutdownChan
	log.Println("[Bootstrap] Shutting down services gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	_ = httpServer.Shutdown(shutdownCtx)
	grpcServer.GracefulStop()
	log.Println("[Bootstrap] All services stopped cleanly.")
}
