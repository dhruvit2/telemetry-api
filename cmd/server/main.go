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
