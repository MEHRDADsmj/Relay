package cron

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// Parser wraps robfig/cron for expression validation and next-fire computation.
// The app layer never imports robfig directly.
type Parser struct {
	parser cron.Parser
}

// NewParser returns a CronParser supporting standard 5-field cron and descriptors.
func NewParser() *Parser {
	return &Parser{
		parser: cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
	}
}

// Validate reports whether expr is a supported cron expression.
func (p *Parser) Validate(expr string) error {
	_, err := p.parser.Parse(expr)
	if err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}
	return nil
}

// Next returns the first fire time strictly after after, in loc.
func (p *Parser) Next(expr string, loc *time.Location, after time.Time) (time.Time, error) {
	sched, err := p.parser.Parse(expr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cron expression: %w", err)
	}
	if loc == nil {
		loc = time.UTC
	}
	return sched.Next(after.In(loc)), nil
}
