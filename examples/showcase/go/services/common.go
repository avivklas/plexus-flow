package services

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/avivklas/plexus-flow/pkg/workerclient"
)

// SleepDuration determines the activity sleep duration based on caller input or task metadata.
// If sleep_ms is 0, it returns 0.
func SleepDuration(ctx context.Context, input json.RawMessage, defaultDuration time.Duration) time.Duration {
	if len(input) > 0 {
		var s struct {
			SleepMs *int `json:"sleep_ms"`
		}
		if err := json.Unmarshal(input, &s); err == nil && s.SleepMs != nil {
			if *s.SleepMs <= 0 {
				return 0
			}
			return time.Duration(*s.SleepMs) * time.Millisecond
		}
	}
	if task, ok := workerclient.TaskFromContext(ctx); ok && task.Metadata != nil {
		if v, ok := task.Metadata["sleep_ms"]; ok && v != "" {
			if ms, err := strconv.Atoi(v); err == nil {
				if ms <= 0 {
					return 0
				}
				return time.Duration(ms) * time.Millisecond
			}
		}
	}
	return defaultDuration
}

// Sleep pauses for the duration specified by SleepDuration. If duration is <= 0, it returns immediately.
func Sleep(ctx context.Context, input json.RawMessage, defaultDuration time.Duration) {
	d := SleepDuration(ctx, input, defaultDuration)
	if d > 0 {
		time.Sleep(d)
	}
}
