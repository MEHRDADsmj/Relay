package domain

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type stubCronParser struct {
	validateErr error
	next        time.Time
	nextErr     error
	validated   string
	nextAfter   time.Time
	nextExpr    string
	nextLocName string
}

func (s *stubCronParser) Validate(expr string) error {
	s.validated = expr
	return s.validateErr
}

func (s *stubCronParser) Next(expr string, loc *time.Location, after time.Time) (time.Time, error) {
	s.nextExpr = expr
	s.nextAfter = after
	if loc != nil {
		s.nextLocName = loc.String()
	}
	return s.next, s.nextErr
}

func TestValidateScheduleName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{name: "simple", in: "hourly"},
		{name: "dotted", in: "billing.nightly"},
		{name: "underscore and dash", in: "my_sched-1"},
		{name: "starts with digit", in: "1daily"},
		{name: "single char", in: "a"},
		{name: "max length 100", in: "a" + strings.Repeat("b", 99)},
		{name: "empty", in: "", wantErr: true},
		{name: "starts with dash", in: "-bad", wantErr: true},
		{name: "starts with dot", in: ".bad", wantErr: true},
		{name: "uppercase", in: "Bad", wantErr: true},
		{name: "space", in: "bad name", wantErr: true},
		{name: "too long", in: "a" + strings.Repeat("b", 100), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateScheduleName(tt.in)
			if tt.wantErr {
				assertValidationField(t, err, "name")
				return
			}
			if err != nil {
				t.Fatalf("ValidateScheduleName(%q) = %v", tt.in, err)
			}
		})
	}
}

func TestNewSchedule_DefaultsAndFields(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	next := now.Add(time.Hour)
	id := ScheduleID(uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"))
	parser := &stubCronParser{next: next}

	s, err := NewSchedule(NewScheduleParams{
		Name:    "hourly.mail",
		Cron:    "@hourly",
		Queue:   "default",
		JobType: "send_email",
	}, id, now, parser)
	if err != nil {
		t.Fatalf("NewSchedule() err = %v", err)
	}

	if s.ID != id {
		t.Fatalf("ID = %v, want %v", s.ID, id)
	}
	if s.Name != "hourly.mail" || s.Cron != "@hourly" {
		t.Fatalf("name/cron = %q/%q", s.Name, s.Cron)
	}
	if s.Timezone != "UTC" {
		t.Fatalf("Timezone = %q, want UTC", s.Timezone)
	}
	if s.Queue != "default" || s.JobType != "send_email" {
		t.Fatalf("queue/type = %q/%q", s.Queue, s.JobType)
	}
	if !bytes.Equal(s.Payload, []byte("{}")) {
		t.Fatalf("Payload = %q, want {}", s.Payload)
	}
	if s.Priority != DefaultPriority || s.MaxAttempts != DefaultMaxAttempts {
		t.Fatalf("priority/max = %d/%d", s.Priority, s.MaxAttempts)
	}
	if !s.Enabled {
		t.Fatal("Enabled should default to true")
	}
	if !s.NextRunAt.Equal(next) {
		t.Fatalf("NextRunAt = %v, want %v", s.NextRunAt, next)
	}
	if s.LastRunAt != nil || s.LastJobID != nil {
		t.Fatal("last run fields must be nil on create")
	}
	if !s.CreatedAt.Equal(now) || !s.UpdatedAt.Equal(now) {
		t.Fatalf("timestamps: created=%v updated=%v", s.CreatedAt, s.UpdatedAt)
	}
	if parser.validated != "@hourly" {
		t.Fatalf("Validate called with %q", parser.validated)
	}
	if parser.nextExpr != "@hourly" || !parser.nextAfter.Equal(now) || parser.nextLocName != "UTC" {
		t.Fatalf("Next args: expr=%q after=%v loc=%q", parser.nextExpr, parser.nextAfter, parser.nextLocName)
	}
}

func TestNewSchedule_ExplicitValues(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	next := now.Add(5 * time.Minute)
	id := ScheduleID(uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"))
	prio := 10
	max := 5
	enabled := false
	parser := &stubCronParser{next: next}

	s, err := NewSchedule(NewScheduleParams{
		Name:        "eu.jobs",
		Cron:        "*/5 * * * *",
		Timezone:    "Europe/Berlin",
		Queue:       "billing",
		JobType:     "charge.card",
		Payload:     []byte(`{"n":1}`),
		Priority:    &prio,
		MaxAttempts: &max,
		Enabled:     &enabled,
	}, id, now, parser)
	if err != nil {
		t.Fatalf("NewSchedule() err = %v", err)
	}
	if s.Timezone != "Europe/Berlin" {
		t.Fatalf("Timezone = %q", s.Timezone)
	}
	if s.Priority != 10 || s.MaxAttempts != 5 || s.Enabled {
		t.Fatalf("prio/max/enabled = %d/%d/%v", s.Priority, s.MaxAttempts, s.Enabled)
	}
	if !bytes.Equal(s.Payload, []byte(`{"n":1}`)) {
		t.Fatalf("Payload = %q", s.Payload)
	}
	if parser.nextLocName != "Europe/Berlin" {
		t.Fatalf("Next loc = %q", parser.nextLocName)
	}
}

func TestNewSchedule_ValidationErrors(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	id := ScheduleID(uuid.New())
	okNext := now.Add(time.Minute)

	tests := []struct {
		name   string
		p      NewScheduleParams
		parser *stubCronParser
		field  string
	}{
		{
			name:   "bad name",
			p:      NewScheduleParams{Name: "BAD", Cron: "@hourly", Queue: "q", JobType: "t"},
			parser: &stubCronParser{next: okNext},
			field:  "name",
		},
		{
			name:   "bad cron validate",
			p:      NewScheduleParams{Name: "ok", Cron: "not-a-cron", Queue: "q", JobType: "t"},
			parser: &stubCronParser{validateErr: errors.New("bad expr")},
			field:  "cron",
		},
		{
			name:   "bad timezone",
			p:      NewScheduleParams{Name: "ok", Cron: "@hourly", Timezone: "Not/AZone", Queue: "q", JobType: "t"},
			parser: &stubCronParser{next: okNext},
			field:  "timezone",
		},
		{
			name:   "bad queue",
			p:      NewScheduleParams{Name: "ok", Cron: "@hourly", Queue: "BAD", JobType: "t"},
			parser: &stubCronParser{next: okNext},
			field:  "queue",
		},
		{
			name:   "bad type",
			p:      NewScheduleParams{Name: "ok", Cron: "@hourly", Queue: "q", JobType: ""},
			parser: &stubCronParser{next: okNext},
			field:  "type",
		},
		{
			name:   "bad payload",
			p:      NewScheduleParams{Name: "ok", Cron: "@hourly", Queue: "q", JobType: "t", Payload: []byte(`{`)},
			parser: &stubCronParser{next: okNext},
			field:  "payload",
		},
		{
			name:   "bad priority",
			p:      NewScheduleParams{Name: "ok", Cron: "@hourly", Queue: "q", JobType: "t", Priority: intPtr(101)},
			parser: &stubCronParser{next: okNext},
			field:  "priority",
		},
		{
			name:   "bad max_attempts",
			p:      NewScheduleParams{Name: "ok", Cron: "@hourly", Queue: "q", JobType: "t", MaxAttempts: intPtr(0)},
			parser: &stubCronParser{next: okNext},
			field:  "max_attempts",
		},
		{
			name:   "next fails",
			p:      NewScheduleParams{Name: "ok", Cron: "@hourly", Queue: "q", JobType: "t"},
			parser: &stubCronParser{nextErr: errors.New("no next")},
			field:  "cron",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewSchedule(tt.p, id, now, tt.parser)
			assertValidationField(t, err, tt.field)
		})
	}
}

func TestNewSchedule_NilParser(t *testing.T) {
	t.Parallel()

	_, err := NewSchedule(NewScheduleParams{
		Name: "ok", Cron: "@hourly", Queue: "q", JobType: "t",
	}, ScheduleID(uuid.New()), time.Now().UTC(), nil)
	assertValidationField(t, err, "cron")
}

func TestFireIdempotencyKey(t *testing.T) {
	t.Parallel()

	id := ScheduleID(uuid.MustParse("cccccccc-cccc-cccc-cccc-cccccccccccc"))
	fire := time.Unix(1_700_000_000, 123).UTC()
	got := FireIdempotencyKey(id, fire)
	want := "schedule:cccccccc-cccc-cccc-cccc-cccccccccccc:1700000000"
	if got != want {
		t.Fatalf("FireIdempotencyKey = %q, want %q", got, want)
	}
}
