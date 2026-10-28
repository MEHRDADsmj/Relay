package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

func newQueueCmd(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queue",
		Short: "Manage queues",
	}
	cmd.AddCommand(
		newQueueListCmd(g),
		newQueueStatsCmd(g),
		newQueuePauseCmd(g),
		newQueueResumeCmd(g),
		newQueueRetryDeadCmd(g),
	)
	return cmd
}

func writeQueues(w io.Writer, mode outputMode, queues []sdk.QueueInfo) error {
	if mode == outputJSON {
		rows := make([]map[string]any, 0, len(queues))
		for _, q := range queues {
			rows = append(rows, map[string]any{
				"name":       q.Name,
				"paused":     q.Paused,
				"created_at": formatTime(q.CreatedAt),
				"updated_at": formatTime(q.UpdatedAt),
			})
		}
		return writeJSON(w, rows)
	}
	tw := newTable(w)
	if _, err := fmt.Fprintln(tw, "NAME\tPAUSED\tCREATED\tUPDATED"); err != nil {
		return err
	}
	for _, q := range queues {
		if _, err := fmt.Fprintf(tw, "%s\t%t\t%s\t%s\n", q.Name, q.Paused, formatTime(q.CreatedAt), formatTime(q.UpdatedAt)); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeQueue(w io.Writer, mode outputMode, q sdk.QueueInfo) error {
	if mode == outputJSON {
		return writeJSON(w, map[string]any{
			"name":       q.Name,
			"paused":     q.Paused,
			"created_at": formatTime(q.CreatedAt),
			"updated_at": formatTime(q.UpdatedAt),
		})
	}
	return writeKVTable(w, [][2]string{
		{"Name", q.Name},
		{"Paused", fmt.Sprintf("%t", q.Paused)},
		{"CreatedAt", formatTime(q.CreatedAt)},
		{"UpdatedAt", formatTime(q.UpdatedAt)},
	})
}

func writeStats(w io.Writer, mode outputMode, s sdk.QueueStatsInfo) error {
	if mode == outputJSON {
		return writeJSON(w, map[string]any{
			"queue":              s.Queue,
			"paused":             s.Paused,
			"pending":            s.Pending,
			"scheduled":          s.Scheduled,
			"running":            s.Running,
			"succeeded":          s.Succeeded,
			"dead":               s.Dead,
			"cancelled":          s.Cancelled,
			"oldest_pending_age": formatDuration(s.OldestPendingAge),
		})
	}
	return writeKVTable(w, [][2]string{
		{"Queue", s.Queue},
		{"Paused", fmt.Sprintf("%t", s.Paused)},
		{"Pending", fmt.Sprintf("%d", s.Pending)},
		{"Scheduled", fmt.Sprintf("%d", s.Scheduled)},
		{"Running", fmt.Sprintf("%d", s.Running)},
		{"Succeeded", fmt.Sprintf("%d", s.Succeeded)},
		{"Dead", fmt.Sprintf("%d", s.Dead)},
		{"Cancelled", fmt.Sprintf("%d", s.Cancelled)},
		{"OldestPendingAge", formatDuration(s.OldestPendingAge)},
	})
}

func newQueueListCmd(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List queues",
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
			queues, err := c.ListQueues(ctx)
			if err != nil {
				return mapSDKError(err)
			}
			return writeQueues(cmd.OutOrStdout(), mode, queues)
		},
	}
}

func newQueueStatsCmd(g *globalFlags) *cobra.Command {
	var watch time.Duration
	cmd := &cobra.Command{
		Use:   "stats <queue>",
		Short: "Show queue stats",
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

			printOnce := func() error {
				ctx, cancel := g.ctx(cmd)
				defer cancel()
				s, err := c.QueueStats(ctx, args[0])
				if err != nil {
					return mapSDKError(err)
				}
				return writeStats(cmd.OutOrStdout(), mode, s)
			}

			if watch <= 0 {
				return printOnce()
			}

			base := cmd.Context()
			if base == nil {
				base = cmd.Root().Context()
			}
			ctx, stop := signal.NotifyContext(base, os.Interrupt, syscall.SIGTERM)
			defer stop()
			ticker := time.NewTicker(watch)
			defer ticker.Stop()
			for {
				if err := printOnce(); err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return nil
				case <-ticker.C:
				}
			}
		},
	}
	cmd.Flags().DurationVar(&watch, "watch", 0, "redraw interval (e.g. 2s); 0 = once")
	return cmd
}

func newQueuePauseCmd(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "pause <queue>",
		Short: "Pause a queue",
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
			q, err := c.PauseQueue(ctx, args[0])
			if err != nil {
				return mapSDKError(err)
			}
			return writeQueue(cmd.OutOrStdout(), mode, q)
		},
	}
}

func newQueueResumeCmd(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "resume <queue>",
		Short: "Resume a queue",
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
			q, err := c.ResumeQueue(ctx, args[0])
			if err != nil {
				return mapSDKError(err)
			}
			return writeQueue(cmd.OutOrStdout(), mode, q)
		},
	}
}

func newQueueRetryDeadCmd(g *globalFlags) *cobra.Command {
	var limit int
	cmd := &cobra.Command{
		Use:   "retry-dead <queue>",
		Short: "Retry dead jobs in a queue",
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
			n, err := c.RetryDead(ctx, args[0], limit)
			if err != nil {
				return mapSDKError(err)
			}
			if mode == outputJSON {
				return writeJSON(cmd.OutOrStdout(), map[string]any{"retried": n})
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Retried\t%d\n", n)
			return err
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 0, "max jobs to retry (0 = server default)")
	return cmd
}
