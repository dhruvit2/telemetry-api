package repository

import (
	"context"
	"testing"
	"time"
)

// MockTSDBRepository is a mock repository for testing
type MockTSDBRepository struct {
	GPUList           []GPU
	TelemetryList     []Telemetry
	GetGPUsError      error
	GetTelemetryError error
}

func (m *MockTSDBRepository) GetGPUs(ctx context.Context) ([]GPU, error) {
	if m.GetGPUsError != nil {
		return nil, m.GetGPUsError
	}
	return m.GPUList, nil
}

func (m *MockTSDBRepository) GetGPUTelemetry(ctx context.Context, gpuID string) ([]Telemetry, error) {
	if m.GetTelemetryError != nil {
		return nil, m.GetTelemetryError
	}
	return m.TelemetryList, nil
}

func (m *MockTSDBRepository) GetGPUTelemetryByDateRange(
	ctx context.Context,
	gpuID string,
	startDate, endDate *time.Time,
) ([]Telemetry, error) {
	if m.GetTelemetryError != nil {
		return nil, m.GetTelemetryError
	}
	return m.TelemetryList, nil
}

func (m *MockTSDBRepository) Close() error {
	return nil
}

// TestGPUStruct validates GPU struct
func TestGPUStruct(t *testing.T) {
	gpu := GPU{
		ID:    "0",
		Name:  "GPU-0",
		Model: "A100",
		Host:  "server1",
	}

	if gpu.ID != "0" {
		t.Errorf("GPU.ID = %v, want 0", gpu.ID)
	}

	if gpu.Model != "A100" {
		t.Errorf("GPU.Model = %v, want A100", gpu.Model)
	}
}

// TestTelemetryStruct validates Telemetry struct
func TestTelemetryStruct(t *testing.T) {
	now := time.Now()
	telemetry := Telemetry{
		Timestamp:  now,
		MetricName: "utilization",
		GPUId:      "0",
		DeviceID:   "dev001",
		UUID:       "abc-123",
		Model:      "A100",
		Host:       "server1",
		Container:  "pod-1",
		Value:      85.5,
		Labels:     map[string]interface{}{"env": "production"},
	}

	if telemetry.MetricName != "utilization" {
		t.Errorf("Telemetry.MetricName = %v, want utilization", telemetry.MetricName)
	}

	if telemetry.Value != 85.5 {
		t.Errorf("Telemetry.Value = %v, want 85.5", telemetry.Value)
	}
}
