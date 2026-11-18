package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Port                      string
	GRPCAddr                  string
	RedisAddr                 string
	RedisPassword             string
	RateLimitCapacity         float64
	RateLimitRefill           float64
	OutboundRateLimitCapacity float64
	OutboundRateLimitRefill   float64
	DBHost                    string
	DBPort                    string
	DBUser                    string
	DBPassword                string
	DBName                    string
	DBSSLMode                 string
}

func Load() *Config {
	return &Config{
		Port:                      getEnv("PORT", "8080"),
		GRPCAddr:                  getEnv("GRPC_ADDR", "localhost:50051"),
		RedisAddr:                 getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:             getEnv("REDIS_PASSWORD", ""),
		RateLimitCapacity:         getEnvFloat("RATE_LIMIT_CAPACITY", 5.0),
		RateLimitRefill:           getEnvFloat("RATE_LIMIT_REFILL_RATE", 2.0),
		OutboundRateLimitCapacity: getEnvFloat("OUTBOUND_RATE_LIMIT_CAPACITY", 2.0),
		OutboundRateLimitRefill:   getEnvFloat("OUTBOUND_RATE_LIMIT_REFILL_RATE", 5.0),
		DBHost:                    getEnv("DB_HOST", "localhost"),
		DBPort:                    getEnv("DB_PORT", "5432"),
		DBUser:                    getEnv("DB_USER", "postgres"),
		DBPassword:                getEnv("DB_PASSWORD", "postgrespassword"),
		DBName:                    getEnv("DB_NAME", "orchestrator_db"),
		DBSSLMode:                 getEnv("DB_SSLMODE", "disable"),
	}
}

func (c *Config) PostgresDSN() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		c.DBHost, c.DBPort, c.DBUser, c.DBPassword, c.DBName, c.DBSSLMode)
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
