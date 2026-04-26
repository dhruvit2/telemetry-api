package repository

import (
	"context"
	"fmt"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/domain"
	"go.uber.org/zap"
)

// GPU represents a GPU device
type GPU struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Model string `json:"model"`
	Host  string `json:"host"`
}

// Telemetry represents a telemetry data point
type Telemetry struct {
	Timestamp  time.Time              `json:"timestamp"`
	MetricName string                 `json:"metric_name"`
	GPUId      string                 `json:"gpu_id"`
	DeviceID   string                 `json:"device_id"`
	UUID       string                 `json:"uuid"`
	Model      string                 `json:"model"`
	Host       string                 `json:"host"`
	Container  string                 `json:"container"`
	Value      float64                `json:"value"`
	Labels     map[string]interface{} `json:"labels"`
}

// TSDBRepository handles TSDB operations
type TSDBRepository struct {
	client   influxdb2.Client
	org      string
	bucket   string
	queryAPI api.QueryAPI
	logger   *zap.Logger
}

// NewTSDBRepository creates a new TSDB repository
func NewTSDBRepository(url, token, org, bucket string, logger *zap.Logger) (*TSDBRepository, error) {
	if url == "" {
		return nil, fmt.Errorf("tsdb url cannot be empty")
	}
	if token == "" {
		return nil, fmt.Errorf("tsdb token cannot be empty")
	}
	if org == "" {
		return nil, fmt.Errorf("tsdb org cannot be empty")
	}
	if bucket == "" {
		return nil, fmt.Errorf("tsdb bucket cannot be empty")
	}

	client := influxdb2.NewClient(url, token)

	// Test connection
	health, err := client.Health(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to TSDB: %w", err)
	}

	if health.Status != domain.HealthCheckStatusPass {
		msg := "no message"
		if health.Message != nil {
			msg = *health.Message
		}
		return nil, fmt.Errorf("tsdb health status not ok: %s (status: %s)", msg, health.Status)
	}
	/*if health.Status != "ok" {
		return nil, fmt.Errorf("tsdb health status not ok: %s", health.Message)
	}*/

	return &TSDBRepository{
		client:   client,
		org:      org,
		bucket:   bucket,
		queryAPI: client.QueryAPI(org),
		logger:   logger,
	}, nil
}

// GetGPUs retrieves all unique GPU IDs and their details
func (r *TSDBRepository) GetGPUs(ctx context.Context) ([]GPU, error) {
	query := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: -30d)
		|> filter(fn: (r) => r._measurement == "gpu_metrics")
		|> group(columns: ["gpu_id", "model_name", "host_name"])
		|> distinct(column: "gpu_id")
	`, r.bucket)

	result, err := r.queryAPI.Query(ctx, query)
	if err != nil {
		r.logger.Error("failed to query GPUs", zap.Error(err))
		return nil, err
	}
	defer result.Close()

	gpuMap := make(map[string]*GPU)

	for result.Next() {
		record := result.Record()
		gpuID := record.ValueByKey("gpu_id")
		model := record.ValueByKey("model_name")
		host := record.ValueByKey("host_name")

		if gpuID == nil {
			continue
		}

		gpuIDStr := fmt.Sprintf("%v", gpuID)
		if _, exists := gpuMap[gpuIDStr]; !exists {
			gpuMap[gpuIDStr] = &GPU{
				ID:    gpuIDStr,
				Name:  gpuIDStr,
				Model: fmt.Sprintf("%v", model),
				Host:  fmt.Sprintf("%v", host),
			}
		}
	}

	if err := result.Err(); err != nil {
		r.logger.Error("query error", zap.Error(err))
		return nil, err
	}

	gpus := make([]GPU, 0, len(gpuMap))
	for _, gpu := range gpuMap {
		gpus = append(gpus, *gpu)
	}

	return gpus, nil
}

// GetGPUTelemetry retrieves telemetry data for a specific GPU
func (r *TSDBRepository) GetGPUTelemetry(ctx context.Context, gpuID string) ([]Telemetry, error) {
	return r.GetGPUTelemetryByDateRange(ctx, gpuID, nil, nil)
}

// GetGPUTelemetryByDateRange retrieves telemetry data for a GPU within a date range
func (r *TSDBRepository) GetGPUTelemetryByDateRange(
	ctx context.Context,
	gpuID string,
	startDate, endDate *time.Time,
) ([]Telemetry, error) {
	// Default to last 24 hours if not specified
	startTime := "-24h"
	endTime := "now()"

	if startDate != nil {
		startTime = startDate.Format(time.RFC3339)
	}
	if endDate != nil {
		endTime = endDate.Format(time.RFC3339)
	}

	query := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: %s, stop: %s)
		|> filter(fn: (r) => r._measurement == "gpu_metrics")
		|> filter(fn: (r) => r.gpu_id == "%s")
		|> sort(columns: ["_time"], desc: false)
	`, r.bucket, startTime, endTime, gpuID)

	result, err := r.queryAPI.Query(ctx, query)
	if err != nil {
		r.logger.Error("failed to query GPU telemetry", zap.String("gpu_id", gpuID), zap.Error(err))
		return nil, err
	}
	defer result.Close()

	telemetryList := make([]Telemetry, 0)

	for result.Next() {
		record := result.Record()

		value := 0.0
		if v := record.Value(); v != nil {
			if f, ok := v.(float64); ok {
				value = f
			}
		}

		telemetry := Telemetry{
			Timestamp:  record.Time(),
			MetricName: fmt.Sprintf("%v", record.ValueByKey("metric_name")),
			GPUId:      fmt.Sprintf("%v", record.ValueByKey("gpu_id")),
			DeviceID:   fmt.Sprintf("%v", record.ValueByKey("device_id")),
			UUID:       fmt.Sprintf("%v", record.ValueByKey("uuid")),
			Model:      fmt.Sprintf("%v", record.ValueByKey("model_name")),
			Host:       fmt.Sprintf("%v", record.ValueByKey("host_name")),
			Container:  fmt.Sprintf("%v", record.ValueByKey("container")),
			Value:      value,
			Labels:     make(map[string]interface{}),
		}

		telemetryList = append(telemetryList, telemetry)
	}

	if err := result.Err(); err != nil {
		r.logger.Error("query error", zap.Error(err))
		return nil, err
	}

	return telemetryList, nil
}

// Close closes the TSDB client connection
func (r *TSDBRepository) Close() error {
	r.client.Close()
	return nil
}
