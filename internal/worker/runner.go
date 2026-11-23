package worker

import (
	"context"
	"database/sql"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/config"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/limiter"
	workerv1 "github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/proto/worker/v1"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

// RunStandaloneWorker boots an isolated OS process running a single domain worker gRPC server.
func RunStandaloneWorker(stepName string, listenAddr string) {
	cfg := config.Load()
	pid := os.Getpid()

	if listenAddr == "" {
		listenAddr = cfg.WorkerAddresses()[stepName]
	}

	log.Printf("[Worker:%s | PID:%d] Initializing standalone worker process on %s...", stepName, pid, listenAddr)

	// 1. Connect to Redis for Outbound Rate Limiting
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       0,
	})
	defer func() { _ = rdb.Close() }()

	outboundBucket := limiter.NewTokenBucketLimiter(rdb, limiter.Config{
		Capacity:         cfg.OutboundRateLimitCapacity,
		RefillRatePerSec: cfg.OutboundRateLimitRefill,
		TTLSeconds:       60,
	})
	outboundLimiter := limiter.NewOutboundLimiter(outboundBucket, 50*time.Millisecond, 200)

	// 2. Connect to PostgreSQL Domain Store (with In-Memory fallback)
	var domainStore DomainStore
	db, err := sql.Open("postgres", cfg.PostgresDSN())
	if err == nil {
		dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dbCancel()
		if pingErr := db.PingContext(dbCtx); pingErr == nil {
			pgDomain := NewPostgresDomainStore(db)
			if seedErr := pgDomain.EnsureSchemaAndSeed(dbCtx); seedErr == nil {
				domainStore = pgDomain
				log.Printf("[Worker:%s | PID:%d] Connected to PostgreSQL Domain Store.", stepName, pid)
			}
		} else {
			_ = db.Close()
		}
	}
	if domainStore == nil {
		log.Printf("[Worker:%s | PID:%d] PostgreSQL unavailable; using isolated In-Memory Domain Store.", stepName, pid)
		domainStore = NewInMemoryDomainStore()
	} else if db != nil {
		defer func() { _ = db.Close() }()
	}

	// 3. Create dedicated gRPC Server for this single step
	srv, err := NewSingleStepGRPCServer(stepName, outboundLimiter, domainStore)
	if err != nil {
		log.Fatalf("[Worker:%s | PID:%d] Failed to create gRPC handler: %v", stepName, pid, err)
	}

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("[Worker:%s | PID:%d] Failed to bind TCP address %s: %v", stepName, pid, listenAddr, err)
	}

	grpcServer := grpc.NewServer()
	workerv1.RegisterWorkerServiceServer(grpcServer, srv)

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[Worker:%s | PID:%d] gRPC server listening on %s", stepName, pid, listenAddr)
		if serveErr := grpcServer.Serve(lis); serveErr != nil {
			log.Fatalf("[Worker:%s | PID:%d] gRPC server error: %v", stepName, pid, serveErr)
		}
	}()

	<-shutdownChan
	log.Printf("[Worker:%s | PID:%d] Shutting down gRPC server...", stepName, pid)
	grpcServer.GracefulStop()
	log.Printf("[Worker:%s | PID:%d] Exited cleanly.", stepName, pid)
}
