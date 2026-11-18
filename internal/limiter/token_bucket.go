package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// tokenBucketLuaScript performs atomic token bucket rate limiting in Redis.
// ARGV[1]: capacity (max tokens)
// ARGV[2]: refill rate (tokens per millisecond)
// ARGV[3]: current unix timestamp in milliseconds
// ARGV[4]: tokens requested (usually 1)
// ARGV[5]: TTL in seconds for key expiration
const tokenBucketLuaScript = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate_per_ms = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local data = redis.call('HMGET', key, 'tokens', 'last_updated')
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if tokens == nil or last_updated == nil then
    tokens = capacity
    last_updated = now
else
    local delta = math.max(0, now - last_updated)
    local refilled = delta * refill_rate_per_ms
    tokens = math.min(capacity, tokens + refilled)
    last_updated = now
end

local allowed = 0
if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
end

redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
redis.call('EXPIRE', key, ttl)

return { allowed, math.floor(tokens) }
`

// TokenBucketLimiter manages rate limiting using Redis and Token Bucket algorithm.
type TokenBucketLimiter struct {
	client     *redis.Client
	script     *redis.Script
	capacity   float64
	refillRate float64 // tokens per second
	ttl        int     // seconds
	nowFunc    func() time.Time
}

// Config holds configuration parameters for TokenBucketLimiter.
type Config struct {
	Capacity         float64           // Max burst size
	RefillRatePerSec float64           // Sustained token generation rate per second
	TTLSeconds       int               // Key expiration TTL
	NowFunc          func() time.Time  // Optional mockable clock (defaults to time.Now)
}

// Result holds the decision and remaining token state.
type Result struct {
	Allowed   bool
	Remaining int64
}

// NewTokenBucketLimiter creates a new TokenBucketLimiter instance.
func NewTokenBucketLimiter(client *redis.Client, cfg Config) *TokenBucketLimiter {
	if cfg.Capacity <= 0 {
		cfg.Capacity = 10
	}
	if cfg.RefillRatePerSec <= 0 {
		cfg.RefillRatePerSec = 2
	}
	if cfg.TTLSeconds <= 0 {
		// Calculate reasonable TTL: time to fill bucket + buffer
		cfg.TTLSeconds = int((cfg.Capacity/cfg.RefillRatePerSec)*2) + 60
	}
	nowFunc := cfg.NowFunc
	if nowFunc == nil {
		nowFunc = time.Now
	}

	return &TokenBucketLimiter{
		client:     client,
		script:     redis.NewScript(tokenBucketLuaScript),
		capacity:   cfg.Capacity,
		refillRate: cfg.RefillRatePerSec,
		ttl:        cfg.TTLSeconds,
		nowFunc:    nowFunc,
	}
}

// Allow checks if 1 token can be consumed for the given key.
func (tbl *TokenBucketLimiter) Allow(ctx context.Context, key string) (Result, error) {
	return tbl.AllowN(ctx, key, 1)
}

// AllowN checks if n tokens can be consumed for the given key.
func (tbl *TokenBucketLimiter) AllowN(ctx context.Context, key string, n int64) (Result, error) {
	nowMs := tbl.nowFunc().UnixNano() / int64(time.Millisecond)
	refillRatePerMs := tbl.refillRate / 1000.0

	keys := []string{key}
	args := []interface{}{
		tbl.capacity,
		refillRatePerMs,
		nowMs,
		n,
		tbl.ttl,
	}

	res, err := tbl.script.Run(ctx, tbl.client, keys, args...).Slice()
	if err != nil {
		return Result{}, fmt.Errorf("redis script execution error: %w", err)
	}

	if len(res) < 2 {
		return Result{}, fmt.Errorf("unexpected script response length: %d", len(res))
	}

	allowedInt, ok1 := res[0].(int64)
	remainingTokens, ok2 := res[1].(int64)

	if !ok1 || !ok2 {
		return Result{}, fmt.Errorf("unexpected return types from lua script: %T, %T", res[0], res[1])
	}

	return Result{
		Allowed:   allowedInt == 1,
		Remaining: remainingTokens,
	}, nil
}
