package flow

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ErrScheduleNotFound is returned when a schedule does not exist.
var ErrScheduleNotFound = errors.New("schedule not found")

// Schedule defines a durable recurring workflow trigger (cron expression or fixed interval).
type Schedule struct {
	ScheduleID     string             `json:"schedule_id"`
	CronExpr       string             `json:"cron_expr,omitempty"`
	Interval       time.Duration      `json:"interval,omitempty"`
	Definition     WorkflowDefinition `json:"definition"`
	Input          json.RawMessage    `json:"input,omitempty"`
	Metadata       map[string]string  `json:"metadata,omitempty"`
	Paused         bool               `json:"paused,omitempty"`
	NextFireAt     time.Time          `json:"next_fire_at"`
	LastRunAt      *time.Time         `json:"last_run_at,omitempty"`
	LastWorkflowID string             `json:"last_workflow_id,omitempty"`
	TotalRuns      int                `json:"total_runs"`
	CreatedAt      time.Time          `json:"created_at"`
	UpdatedAt      time.Time          `json:"updated_at"`
}

// Validate checks that the schedule has an ID, valid workflow definition, and valid timing spec.
func (s *Schedule) Validate() error {
	if strings.TrimSpace(s.ScheduleID) == "" {
		return fmt.Errorf("schedule_id cannot be empty")
	}
	if err := s.Definition.Validate(); err != nil {
		return fmt.Errorf("invalid schedule workflow definition: %w", err)
	}
	if s.CronExpr == "" && s.Interval <= 0 {
		return fmt.Errorf("schedule %q must specify either cron_expr or positive interval", s.ScheduleID)
	}
	if _, err := NextScheduleTime(s.CronExpr, s.Interval, time.Now().UTC()); err != nil {
		return err
	}
	return nil
}

// Clone returns a deep copy of the Schedule.
func (s *Schedule) Clone() *Schedule {
	if s == nil {
		return nil
	}
	b, err := json.Marshal(s)
	if err != nil {
		return nil
	}
	var cp Schedule
	_ = json.Unmarshal(b, &cp)
	return &cp
}

// NextScheduleTime computes the next firing timestamp strictly after `after`.
// Supports:
//   - Fixed interval (`interval > 0` when `cronExpr == ""`)
//   - `@every <duration>` (e.g. `@every 5s`, `@every 1m`)
//   - `@hourly`, `@daily` / `@midnight`, `@weekly`, `@monthly`
//   - Standard 5-field cron: `minute hour day-of-month month day-of-week`
func NextScheduleTime(cronExpr string, interval time.Duration, after time.Time) (time.Time, error) {
	after = after.UTC()
	expr := strings.TrimSpace(cronExpr)
	if expr == "" {
		if interval <= 0 {
			return time.Time{}, fmt.Errorf("schedule requires cron_expr or positive interval")
		}
		return after.Add(interval), nil
	}

	if strings.HasPrefix(expr, "@every ") {
		durStr := strings.TrimSpace(strings.TrimPrefix(expr, "@every "))
		d, err := time.ParseDuration(durStr)
		if err != nil || d <= 0 {
			return time.Time{}, fmt.Errorf("invalid @every duration %q", durStr)
		}
		return after.Add(d), nil
	}

	switch expr {
	case "@hourly":
		expr = "0 * * * *"
	case "@daily", "@midnight":
		expr = "0 0 * * *"
	case "@weekly":
		expr = "0 0 * * 0"
	case "@monthly":
		expr = "0 0 1 * *"
	}

	spec, err := parseCron5(expr)
	if err != nil {
		return time.Time{}, err
	}

	// Advance to the next whole minute strictly after `after`.
	t := after.Truncate(time.Minute).Add(time.Minute)
	// Search up to 4 years ahead to cover leap-year schedules.
	limit := t.Add(4 * 366 * 24 * time.Hour)
	for t.Before(limit) {
		if !spec.month[int(t.Month())] {
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, time.UTC)
			continue
		}
		if !spec.dom[t.Day()] || !spec.dow[int(t.Weekday())] {
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, time.UTC)
			continue
		}
		if !spec.hour[t.Hour()] {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, time.UTC)
			continue
		}
		if !spec.minute[t.Minute()] {
			t = t.Add(time.Minute)
			continue
		}
		return t, nil
	}
	return time.Time{}, fmt.Errorf("cron expression %q has no matching firing time within 4 years", cronExpr)
}

type cronSpec struct {
	minute [60]bool
	hour   [24]bool
	dom    [32]bool
	month  [13]bool
	dow    [7]bool
}

func parseCron5(expr string) (*cronSpec, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("invalid cron expression %q: expected 5 fields (min hour dom month dow)", expr)
	}
	var spec cronSpec
	if err := parseCronField(fields[0], 0, 59, spec.minute[:]); err != nil {
		return nil, fmt.Errorf("cron minute field: %w", err)
	}
	if err := parseCronField(fields[1], 0, 23, spec.hour[:]); err != nil {
		return nil, fmt.Errorf("cron hour field: %w", err)
	}
	if err := parseCronField(fields[2], 1, 31, spec.dom[:]); err != nil {
		return nil, fmt.Errorf("cron day-of-month field: %w", err)
	}
	if err := parseCronField(fields[3], 1, 12, spec.month[:]); err != nil {
		return nil, fmt.Errorf("cron month field: %w", err)
	}
	if err := parseCronField(fields[4], 0, 6, spec.dow[:]); err != nil {
		return nil, fmt.Errorf("cron day-of-week field: %w", err)
	}
	return &spec, nil
}

func parseCronField(field string, minVal, maxVal int, out []bool) error {
	for _, part := range strings.Split(field, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return fmt.Errorf("empty cron segment")
		}
		step := 1
		if slashIdx := strings.IndexByte(part, '/'); slashIdx != -1 {
			s, err := strconv.Atoi(part[slashIdx+1:])
			if err != nil || s <= 0 {
				return fmt.Errorf("invalid step in %q", part)
			}
			step = s
			part = part[:slashIdx]
		}

		var start, end int
		switch {
		case part == "*":
			start, end = minVal, maxVal
		case strings.Contains(part, "-"):
			bounds := strings.SplitN(part, "-", 2)
			s, err1 := strconv.Atoi(bounds[0])
			e, err2 := strconv.Atoi(bounds[1])
			if err1 != nil || err2 != nil || s < minVal || e > maxVal || s > e {
				return fmt.Errorf("invalid range %q (bounds %d..%d)", part, minVal, maxVal)
			}
			start, end = s, e
		default:
			v, err := strconv.Atoi(part)
			if err != nil || v < minVal || v > maxVal {
				return fmt.Errorf("invalid value %q (bounds %d..%d)", part, minVal, maxVal)
			}
			start, end = v, v
		}

		for i := start; i <= end; i += step {
			out[i] = true
		}
	}
	return nil
}
