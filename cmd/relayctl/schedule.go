package main

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

func newScheduleCmd(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Manage cron schedules",
	}
	cmd.AddCommand(
		newScheduleCreateCmd(g),
		newScheduleListCmd(g),
		newScheduleGetCmd(g),
		newScheduleEnableCmd(g),
		newScheduleDisableCmd(g),
		newScheduleTriggerCmd(g),
		newScheduleDeleteCmd(g),
		newScheduleEditCmd(g),
	)
	return cmd
}

func scheduleDTO(s sdk.ScheduleInfo) map[string]any {
	m := map[string]any{
		"id":           s.ID,
		"name":         s.Name,
		"cron":         s.Cron,
		"timezone":     s.Timezone,
		"queue":        s.Queue,
		"type":         s.Type,
		"priority":     s.Priority,
		"max_attempts": s.MaxAttempts,
		"enabled":      s.Enabled,
		"next_run_at":  formatTime(s.NextRunAt),
		"created_at":   formatTime(s.CreatedAt),
		"updated_at":   formatTime(s.UpdatedAt),
	}
	if len(s.Payload) > 0 {
		m["payload"] = jsonRawOrString(s.Payload)
	}
	if s.LastRunAt != nil {
		m["last_run_at"] = formatTimePtr(s.LastRunAt)
	}
	if s.LastJobID != "" {
		m["last_job_id"] = s.LastJobID
	}
	return m
}

func writeSchedule(w io.Writer, mode outputMode, s sdk.ScheduleInfo) error {
	if mode == outputJSON {
		return writeJSON(w, scheduleDTO(s))
	}
	return writeKVTable(w, [][2]string{
		{"ID", s.ID},
		{"Name", s.Name},
		{"Cron", s.Cron},
		{"Timezone", s.Timezone},
		{"Queue", s.Queue},
		{"Type", s.Type},
		{"Priority", fmt.Sprintf("%d", s.Priority)},
		{"MaxAttempts", fmt.Sprintf("%d", s.MaxAttempts)},
		{"Enabled", fmt.Sprintf("%t", s.Enabled)},
		{"NextRunAt", formatTime(s.NextRunAt)},
		{"LastRunAt", formatTimePtr(s.LastRunAt)},
		{"LastJobID", s.LastJobID},
		{"UpdatedAt", formatTime(s.UpdatedAt)},
	})
}

func writeSchedules(w io.Writer, mode outputMode, schedules []sdk.ScheduleInfo) error {
	if mode == outputJSON {
		out := make([]map[string]any, 0, len(schedules))
		for _, s := range schedules {
			out = append(out, scheduleDTO(s))
		}
		return writeJSON(w, out)
	}
	tw := newTable(w)
	if _, err := fmt.Fprintln(tw, "ID\tNAME\tCRON\tTZ\tQUEUE\tTYPE\tENABLED\tNEXT_RUN"); err != nil {
		return err
	}
	for _, s := range schedules {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%t\t%s\n",
			s.ID, s.Name, s.Cron, s.Timezone, s.Queue, s.Type, s.Enabled, formatTime(s.NextRunAt)); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func newScheduleCreateCmd(g *globalFlags) *cobra.Command {
	var (
		name, cron, tz, queue, typ, payload, payloadFile string
		priority, maxAttempts                            int
	)
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a schedule",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if name == "" || cron == "" || queue == "" || typ == "" {
				return usageErrorf("--name, --cron, --queue, and --type are required")
			}
			mode, err := g.mode()
			if err != nil {
				return err
			}
			pl, err := loadPayload(payload, payloadFile)
			if err != nil {
				return err
			}
			c, err := g.client()
			if err != nil {
				return err
			}
			defer c.Close() //nolint:errcheck
			ctx, cancel := g.ctx(cmd)
			defer cancel()
			s, err := c.CreateSchedule(ctx, sdk.CreateScheduleRequest{
				Name:        name,
				Cron:        cron,
				Timezone:    tz,
				Queue:       queue,
				Type:        typ,
				Payload:     pl,
				Priority:    priority,
				MaxAttempts: maxAttempts,
			})
			if err != nil {
				return mapSDKError(err)
			}
			return writeSchedule(cmd.OutOrStdout(), mode, s)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "unique schedule name")
	cmd.Flags().StringVar(&cron, "cron", "", "cron expression")
	cmd.Flags().StringVar(&tz, "timezone", "UTC", "IANA timezone")
	cmd.Flags().StringVar(&queue, "queue", "", "queue name")
	cmd.Flags().StringVar(&typ, "type", "", "job type")
	cmd.Flags().StringVar(&payload, "payload", "", "JSON payload")
	cmd.Flags().StringVar(&payloadFile, "payload-file", "", "path to JSON payload file")
	cmd.Flags().IntVar(&priority, "priority", 0, "priority")
	cmd.Flags().IntVar(&maxAttempts, "max-attempts", 0, "max attempts")
	return cmd
}

func newScheduleListCmd(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List schedules",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			mode, err := g.mode()
			if err != nil {
				return err
			}
			c, err := g.client()
			if err != nil {
				return err
			}
			defer c.Close() //nolint:errcheck
			ctx, cancel := g.ctx(cmd)
			defer cancel()
			schedules, _, err := c.ListSchedules(ctx, sdk.ListSchedulesOptions{})
			if err != nil {
				return mapSDKError(err)
			}
			return writeSchedules(cmd.OutOrStdout(), mode, schedules)
		},
	}
}

func newScheduleGetCmd(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get <id|name>",
		Short: "Get a schedule by ID or name",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			mode, err := g.mode()
			if err != nil {
				return err
			}
			c, err := g.client()
			if err != nil {
				return err
			}
			defer c.Close() //nolint:errcheck
			ctx, cancel := g.ctx(cmd)
			defer cancel()
			s, err := c.GetSchedule(ctx, args[0])
			if err != nil {
				return mapSDKError(err)
			}
			return writeSchedule(cmd.OutOrStdout(), mode, s)
		},
	}
}

func newScheduleEnableCmd(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "enable <id>",
		Short: "Enable a schedule",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			mode, err := g.mode()
			if err != nil {
				return err
			}
			c, err := g.client()
			if err != nil {
				return err
			}
			defer c.Close() //nolint:errcheck
			ctx, cancel := g.ctx(cmd)
			defer cancel()
			s, err := c.EnableSchedule(ctx, args[0])
			if err != nil {
				return mapSDKError(err)
			}
			return writeSchedule(cmd.OutOrStdout(), mode, s)
		},
	}
}

func newScheduleDisableCmd(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "disable <id>",
		Short: "Disable a schedule",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			mode, err := g.mode()
			if err != nil {
				return err
			}
			c, err := g.client()
			if err != nil {
				return err
			}
			defer c.Close() //nolint:errcheck
			ctx, cancel := g.ctx(cmd)
			defer cancel()
			s, err := c.DisableSchedule(ctx, args[0])
			if err != nil {
				return mapSDKError(err)
			}
			return writeSchedule(cmd.OutOrStdout(), mode, s)
		},
	}
}

func newScheduleTriggerCmd(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "trigger <id>",
		Short: "Trigger a schedule immediately",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			mode, err := g.mode()
			if err != nil {
				return err
			}
			c, err := g.client()
			if err != nil {
				return err
			}
			defer c.Close() //nolint:errcheck
			ctx, cancel := g.ctx(cmd)
			defer cancel()
			j, err := c.TriggerSchedule(ctx, args[0])
			if err != nil {
				return mapSDKError(err)
			}
			return writeJob(cmd.OutOrStdout(), mode, j)
		},
	}
}

func newScheduleDeleteCmd(g *globalFlags) *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "delete <id>",
		Short: "Delete a schedule",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := confirmDestructive("delete schedule "+args[0], yes, cmd.InOrStdin(), cmd.ErrOrStderr()); err != nil {
				return err
			}
			c, err := g.client()
			if err != nil {
				return err
			}
			defer c.Close() //nolint:errcheck
			ctx, cancel := g.ctx(cmd)
			defer cancel()
			if err := c.DeleteSchedule(ctx, args[0]); err != nil {
				return mapSDKError(err)
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), "deleted")
			return err
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "skip confirmation")
	return cmd
}

func newScheduleEditCmd(g *globalFlags) *cobra.Command {
	var (
		cron, tz, queue, typ, payload, payloadFile string
		priority, maxAttempts                      int
		setCron, setTZ, setQueue, setType          bool
		setPriority, setMaxAttempts, setPayload    bool
	)
	cmd := &cobra.Command{
		Use:   "edit <id>",
		Short: "Edit a schedule (If-Match via current updated_at)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			mode, err := g.mode()
			if err != nil {
				return err
			}
			c, err := g.client()
			if err != nil {
				return err
			}
			defer c.Close() //nolint:errcheck
			ctx, cancel := g.ctx(cmd)
			defer cancel()

			cur, err := c.GetSchedule(ctx, args[0])
			if err != nil {
				return mapSDKError(err)
			}
			req := sdk.UpdateScheduleRequest{ExpectedUpdatedAt: cur.UpdatedAt}
			if setCron {
				req.Cron = &cron
			}
			if setTZ {
				req.Timezone = &tz
			}
			if setQueue {
				req.Queue = &queue
			}
			if setType {
				req.Type = &typ
			}
			if setPriority {
				req.Priority = &priority
			}
			if setMaxAttempts {
				req.MaxAttempts = &maxAttempts
			}
			if setPayload {
				pl, err := loadPayload(payload, payloadFile)
				if err != nil {
					return err
				}
				req.SetPayload = true
				switch v := pl.(type) {
				case nil:
					req.Payload = []byte("null")
				case json.RawMessage:
					req.Payload = append([]byte(nil), v...)
				case []byte:
					req.Payload = append([]byte(nil), v...)
				default:
					b, err := json.Marshal(v)
					if err != nil {
						return usageErrorf("payload: %v", err)
					}
					req.Payload = b
				}
			}
			s, err := c.UpdateSchedule(ctx, cur.ID, req)
			if err != nil {
				return mapSDKError(err)
			}
			return writeSchedule(cmd.OutOrStdout(), mode, s)
		},
	}
	cmd.Flags().StringVar(&cron, "cron", "", "cron expression")
	cmd.Flags().StringVar(&tz, "timezone", "", "IANA timezone")
	cmd.Flags().StringVar(&queue, "queue", "", "queue name")
	cmd.Flags().StringVar(&typ, "type", "", "job type")
	cmd.Flags().StringVar(&payload, "payload", "", "JSON payload")
	cmd.Flags().StringVar(&payloadFile, "payload-file", "", "path to JSON payload file")
	cmd.Flags().IntVar(&priority, "priority", 0, "priority")
	cmd.Flags().IntVar(&maxAttempts, "max-attempts", 0, "max attempts")
	cmd.PreRun = func(cmd *cobra.Command, _ []string) {
		setCron = cmd.Flags().Changed("cron")
		setTZ = cmd.Flags().Changed("timezone")
		setQueue = cmd.Flags().Changed("queue")
		setType = cmd.Flags().Changed("type")
		setPriority = cmd.Flags().Changed("priority")
		setMaxAttempts = cmd.Flags().Changed("max-attempts")
		setPayload = cmd.Flags().Changed("payload") || cmd.Flags().Changed("payload-file")
	}
	return cmd
}
