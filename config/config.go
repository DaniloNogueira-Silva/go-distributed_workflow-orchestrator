package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port              string
	RedisAddr         string
	RedisPassword     string
	RateLimitCapacity float64
	RateLimitRefill   float64
}

func Load() *Config {
	return &Config{
		Port:              getEnv("PORT", "8080"),
		RedisAddr:         getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:     getEnv("REDIS_PASSWORD", ""),
		RateLimitCapacity: getEnvFloat("RATE_LIMIT_CAPACITY", 5.0),    // 5 tokens capacity
		RateLimitRefill:   getEnvFloat("RATE_LIMIT_REFILL_RATE", 2.0), // 2 tokens/sec refill
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvFloat(key string, defaultVal float64) float64 {
	valStr := os.Getenv(key)
	if valStr == "" {
		return defaultVal
	}
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return defaultVal
	}
	return val
}
