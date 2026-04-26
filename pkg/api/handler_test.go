package api

import (
	"testing"
	"time"
)

func TestParseDate(t *testing.T) {
	tests := []struct {
		name    string
		dateStr string
		wantErr bool
	}{
		{
			name:    "RFC3339 format",
			dateStr: "2024-01-15T10:00:00Z",
			wantErr: false,
		},
		{
			name:    "RFC3339Nano format",
			dateStr: "2024-01-15T10:00:00.000000000Z",
			wantErr: false,
		},
		{
			name:    "Simple date format",
			dateStr: "2024-01-15",
			wantErr: false,
		},
		{
			name:    "Datetime format",
			dateStr: "2024-01-15 10:00:00",
			wantErr: false,
		},
		{
			name:    "Invalid format",
			dateStr: "invalid-date",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseDate(tt.dateStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseDate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseDate_ValidOutputs(t *testing.T) {
	tests := []struct {
		name    string
		dateStr string
		want    string
	}{
		{
			name:    "RFC3339 format",
			dateStr: "2024-01-15T10:00:00Z",
			want:    "2024-01-15T10:00:00Z",
		},
		{
			name:    "Simple date format",
			dateStr: "2024-01-15",
			want:    "2024-01-15T00:00:00Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := parseDate(tt.dateStr)
			if err != nil {
				t.Fatalf("parseDate() error = %v", err)
			}

			// Check year, month, day match
			if parsed.Year() != 2024 || parsed.Month() != time.January || parsed.Day() != 15 {
				t.Errorf("parseDate() = %v, want year=2024, month=1, day=15", parsed)
			}
		})
	}
}
