package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"telemetry-api/pkg/api"
	"telemetry-api/pkg/config"
	"telemetry-api/pkg/repository"
)

const (
	swaggerHTML = `<!DOCTYPE html>
<html>
<head>
  <title>Telemetry API - Swagger UI</title>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700">
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@3/swagger-ui.css">
  <style>
    html{box-sizing:border-box;overflow:visible}*,*:before,*:after{box-sizing:inherit}body{margin:0;padding:0;background:#fafafa}
  </style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@3/swagger-ui.js"></script>
  <script>
  SwaggerUIBundle({
    url: "/openapi.yaml",
    dom_id: '#swagger-ui',
    presets: [
      SwaggerUIBundle.presets.apis,
      SwaggerUIBundle.SwaggerUIStandalonePreset
    ],
    layout: "BaseLayout"
  })
  </script>
</body>
</html>`

	openAPISpec = `openapi: 3.0.3
info:
  title: Telemetry API
  version: 1.0.0
  description: REST API for querying GPU telemetry metrics from InfluxDB TSDB
servers:
  - url: http://localhost:8082
  - url: http://localhost:8080
tags:
  - name: Health
    description: Health check endpoints
  - name: GPUs
    description: GPU information and telemetry queries
paths:
  /health:
    get:
      summary: Health check
      tags:
        - Health
      operationId: getHealth
      responses:
        '200':
          description: Service is healthy
          content:
            application/json:
              schema:
                type: object
                properties:
                  status:
                    type: string
                  timestamp:
                    type: string
                    format: date-time
  /ready:
    get:
      summary: Readiness probe
      tags:
        - Health
      operationId: getReady
      responses:
        '200':
          description: Service is ready
  /api/v1/gpus:
    get:
      summary: List all GPU IDs
      tags:
        - GPUs
      operationId: getGPUs
      responses:
        '200':
          description: Successfully retrieved list of GPUs
          content:
            application/json:
              schema:
                type: object
                properties:
                  gpus:
                    type: array
                    items:
                      type: string
                  count:
                    type: integer
        '500':
          description: Internal server error
  /api/v1/gpus/{id}/telemetry:
    get:
      summary: Get telemetry data for a GPU
      tags:
        - GPUs
      operationId: getGPUTelemetry
      parameters:
        - name: id
          in: path
          required: true
          schema:
            type: string
        - name: start_date
          in: query
          schema:
            type: string
            format: date-time
        - name: end_date
          in: query
          schema:
            type: string
            format: date-time
      responses:
        '200':
          description: Successfully retrieved telemetry data
          content:
            application/json:
              schema:
                type: object
        '500':
          description: Internal server error`
)

func main() {
	// Load configuration
	cfg := config.LoadConfig()

	// Setup logger
	logger := setupLogger(cfg.LogLevel)
	defer logger.Sync()

	logger.Info("Starting telemetry API service",
		zap.String("service_name", cfg.ServiceName),
		zap.String("service_id", cfg.ServiceID),
		zap.Int("port", cfg.Port),
		zap.String("tsdb_url", cfg.TSDBURL),
		zap.String("tsdb_org", cfg.TSDBOrg),
		zap.String("tsdb_bucket", cfg.TSDBBucket))

	// Create TSDB repository
	repo, err := repository.NewTSDBRepository(
		cfg.TSDBURL,
		cfg.TSDBToken,
		cfg.TSDBOrg,
		cfg.TSDBBucket,
		logger,
	)
	if err != nil {
		logger.Error(err.Error())
		logger.Fatal("failed to create TSDB repository", zap.Error(err))
	}
	defer repo.Close()

	logger.Info("Connected to TSDB successfully")

	// Create API handler
	handler := api.NewGPUHandler(repo, logger)

	// Setup router
	router := mux.NewRouter()

	// API endpoints
	router.HandleFunc("/api/v1/gpus", handler.GetGPUs).Methods("GET")
	router.HandleFunc("/api/v1/gpus/{id}/telemetry", handler.GetGPUTelemetry).Methods("GET")

	// Swagger/OpenAPI documentation endpoints
	router.HandleFunc("/docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, swaggerHTML)
	}).Methods("GET")

	router.HandleFunc("/openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		w.WriteHeader(http.StatusOK)
		// Serve OpenAPI spec - in production, read from file or embed
		fmt.Fprint(w, openAPISpec)
	}).Methods("GET")

	router.HandleFunc("/api-docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, openAPISpec)
	}).Methods("GET")

	// Health check endpoints
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status": "healthy", "timestamp": "%s"}`, time.Now().Format(time.RFC3339Nano))
	}).Methods("GET")

	router.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"ready": true}`)
	}).Methods("GET")

	// Start HTTP server
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start server in a goroutine
	go func() {
		logger.Info("telemetry API server starting",
			zap.String("address", server.Addr))

		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", zap.Error(err))
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	logger.Info("received shutdown signal")

	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", zap.Error(err))
	}

	logger.Info("telemetry API server stopped")
}

// setupLogger creates and configures logger
func setupLogger(level string) *zap.Logger {
	var logLevel zapcore.Level
	switch level {
	case "debug":
		logLevel = zapcore.DebugLevel
	case "info":
		logLevel = zapcore.InfoLevel
	case "warn":
		logLevel = zapcore.WarnLevel
	case "error":
		logLevel = zapcore.ErrorLevel
	default:
		logLevel = zapcore.InfoLevel
	}

	config := zap.Config{
		Level:       zap.NewAtomicLevelAt(logLevel),
		Development: false,
		Encoding:    "json",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "timestamp",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "message",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.RFC3339NanoTimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	logger, err := config.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		os.Exit(1)
	}

	return logger
}
