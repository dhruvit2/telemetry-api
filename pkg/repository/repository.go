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
	// 1. We look at all data
	// 2. We filter to our measurement
	// 3. We group() to merge all internal tables into one
	// 4. We use distinct() to get one row per unique ID
	query := fmt.Sprintf(`
        from(bucket: "%s")
            |> range(start: 0)
            |> filter(fn: (r) => r._measurement == "gpu_metrics")
            |> filter(fn: (r) => r.gpu_id != "unknown")
            |> group() 
            |> distinct(column: "gpu_id")
            |> keep(columns: ["_value"]) 
    `, r.bucket)

	result, err := r.queryAPI.Query(ctx, query)
	if err != nil {
		r.logger.Error("failed to query GPUs", zap.Error(err))
		return nil, err
	}
	defer result.Close()

	var gpus []GPU

	for result.Next() {
		// When using distinct(), the result is placed in the "_value" column
		val := result.Record().Value()
		if val == nil {
			continue
		}

		gpuID := fmt.Sprintf("%v", val)
		gpus = append(gpus, GPU{
			ID:   gpuID,
			Name: "GPU " + gpuID,
		})
	}

	return gpus, result.Err()
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
	// 1. Handle Time Logic
	// If startDate is nil, use 0 (Unix epoch) to get all historical data
	startTime := "0"
	endTime := "now()"

	if startDate != nil {
		startTime = startDate.Format(time.RFC3339)
	}
	if endDate != nil {
		endTime = endDate.Format(time.RFC3339)
	}

	// 2. Build the Query
	// Added pivot() to group all fields (temp, power, etc.) into a single row per timestamp
	query := fmt.Sprintf(`
        from(bucket: "%s")
        |> range(start: %s, stop: %s)
        |> filter(fn: (r) => r._measurement == "gpu_metrics")
        |> filter(fn: (r) => r.gpu_id == "%s")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
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

		// Helper to safely extract string values
		getStr := func(key string) string {
			if v, ok := record.ValueByKey(key).(string); ok {
				return v
			}
			return ""
		}

		// 3. Extract the numeric value.
		// Replace "value" with the specific field name from your CSV if it's different (e.g., "usage")
		metricValue := 0.0
		if v, ok := record.ValueByKey("value").(float64); ok {
			metricValue = v
		}

		telemetry := Telemetry{
			Timestamp:  record.Time(),
			MetricName: getStr("_measurement"),
			GPUId:      getStr("gpu_id"),
			DeviceID:   getStr("device_id"),
			UUID:       getStr("uuid"),
			Model:      getStr("model_name"),
			Host:       getStr("host_name"),
			Container:  getStr("container"),
			Value:      metricValue,
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
