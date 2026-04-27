package config

import (
	"os"
	"strconv"
	"strings"
)

// Config holds configuration for telemetry API
type Config struct {
	ServiceName string
	ServiceID   string
	Port        int
	LogLevel    string

	// TSDB configuration
	TSDBURL    string
	TSDBToken  string
	TSDBOrg    string
	TSDBBucket string
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *Config {
	return &Config{
		ServiceName: getEnv("SERVICE_NAME", "telemetry-api"),
		ServiceID:   getEnv("SERVICE_ID", "1"),
		Port:        getEnvInt("PORT", 8080),
		LogLevel:    getEnv("LOG_LEVEL", "info"),

		TSDBURL:    getEnv("TSDB_URL", "http://localhost:8086"),
		TSDBToken:  getEnv("TSDB_TOKEN", ""),
		TSDBOrg:    getEnv("TSDB_ORG", "telemetry"),
		TSDBBucket: getEnv("TSDB_BUCKET", "gpu_metrics_raw"),
	}
}

// Helper functions
func getEnv(key, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	valStr := getEnv(key, "")
	if val, err := strconv.Atoi(valStr); err == nil {
		return val
	}
	return defaultVal
}

func parseBool(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	return s == "true" || s == "1" || s == "yes"
}
