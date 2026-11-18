package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/config"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/gateway"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/limiter"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.Load()

	log.Printf("[Main] Connecting to Redis at %s...", cfg.RedisAddr)
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("[Main Warning] Could not ping Redis immediately: %v (Gateway will retry on request)", err)
	} else {
		log.Println("[Main] Successfully connected to Redis.")
	}

	rateLimiter := limiter.NewTokenBucketLimiter(rdb, limiter.Config{
		Capacity:         cfg.RateLimitCapacity,
		RefillRatePerSec: cfg.RateLimitRefill,
		TTLSeconds:       60,
	})

	mux := http.NewServeMux()
	gwHandler := gateway.NewHandler(rateLimiter)
	gwHandler.RegisterRoutes(mux)

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown channel
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[Main] Edge API Gateway listening on port %s", cfg.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[Main] Server error: %v", err)
		}
	}()

	<-shutdownChan
	log.Println("[Main] Shutting down gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[Main] Server forced to shutdown: %v", err)
	}

	if err := rdb.Close(); err != nil {
		log.Printf("[Main] Error closing Redis connection: %v", err)
	}

	log.Println("[Main] Server exited cleanly.")
}
