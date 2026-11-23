package limiter_test

import (
	"context"
	"testing"
	"time"

	"github.com/DaniloNogueira-Silva/go-distributed_workflow-orchestrator/internal/limiter"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	s := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	return s, client
}

func TestTokenBucketLimiter_Allow(t *testing.T) {
	s, client := setupTestRedis(t)
	defer s.Close()
	defer client.Close()

	ctx := context.Background()
	currentTime := time.Now()

	// Capacity = 3, Refill = 1 token/sec
	tb := limiter.NewTokenBucketLimiter(client, limiter.Config{
		Capacity:         3,
		RefillRatePerSec: 1,
		TTLSeconds:       60,
		NowFunc: func() time.Time {
			return currentTime
		},
	})

	key := "test-client"

	// First 3 requests must succeed (consuming burst)
	for i := 0; i < 3; i++ {
		res, err := tb.Allow(ctx, key)
		if err != nil {
			t.Fatalf("unexpected error on request %d: %v", i+1, err)
		}
		if !res.Allowed {
			t.Fatalf("expected request %d to be allowed", i+1)
		}
	}

	// 4th request must be blocked because capacity is exhausted
	res, err := tb.Allow(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Allowed {
		t.Fatalf("expected request 4 to be rejected (rate limited)")
	}

	// Advance mock time and miniredis by 1.1 seconds
	currentTime = currentTime.Add(1100 * time.Millisecond)
	s.FastForward(1100 * time.Millisecond)

	// Now 1 token should have refilled
	res, err = tb.Allow(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error after refill: %v", err)
	}
	if !res.Allowed {
		t.Fatalf("expected request to be allowed after token refill")
	}
}
