package limiter

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/DaniloNogueira-Silva/go-distributed_workflow-orchestrator/internal/metrics"
)

// OutboundLimiter wraps TokenBucketLimiter to throttle outbound calls to external third-party APIs
// (e.g., Payment Gateways, Shipping Carriers). Instead of failing immediately when rate limited,
// it delays and retries until a token is available or the context expires.
type OutboundLimiter struct {
	bucket       *TokenBucketLimiter
	retryDelay   time.Duration
	maxWaitLoops int
}

// NewOutboundLimiter creates a new OutboundLimiter.
func NewOutboundLimiter(bucket *TokenBucketLimiter, retryDelay time.Duration, maxWaitLoops int) *OutboundLimiter {
	if retryDelay <= 0 {
		retryDelay = 100 * time.Millisecond
	}
	if maxWaitLoops <= 0 {
		maxWaitLoops = 50
	}
	return &OutboundLimiter{
		bucket:       bucket,
		retryDelay:   retryDelay,
		maxWaitLoops: maxWaitLoops,
	}
}

// Acquire blocks and retries with delay if the outbound token bucket is depleted,
// returning the number of delayed retries that occurred before acquiring a token.
func (ol *OutboundLimiter) Acquire(ctx context.Context, externalService string) (int, error) {
	if ol == nil || ol.bucket == nil {
		return 0, nil
	}

	key := "rate:outbound:" + externalService
	delayedRetries := 0

	for i := 0; i < ol.maxWaitLoops; i++ {
		res, err := ol.bucket.Allow(ctx, key)
		if err != nil {
			return delayedRetries, fmt.Errorf("outbound rate limiter check failed for %s: %w", externalService, err)
		}

		if res.Allowed {
			if delayedRetries > 0 {
				metrics.OutboundRateLimitRequestsTotal.WithLabelValues(externalService, "true").Inc()
			} else {
				metrics.OutboundRateLimitRequestsTotal.WithLabelValues(externalService, "false").Inc()
			}
			return delayedRetries, nil
		}

		delayedRetries++
		metrics.OutboundRateLimitDelaysTotal.WithLabelValues(externalService).Inc()
		log.Printf("[OutboundLimiter] Outbound rate limit reached for '%s'. Delaying %v and retrying (retry #%d)...",
			externalService, ol.retryDelay, delayedRetries)

		select {
		case <-ctx.Done():
			return delayedRetries, ctx.Err()
		case <-time.After(ol.retryDelay):
		}
	}

	return delayedRetries, fmt.Errorf("outbound rate limiter exceeded max wait retries (%d) for %s", ol.maxWaitLoops, externalService)
}
