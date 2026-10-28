package main

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

func newJobCmd(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "job",
		Short: "Manage jobs",
	}
	cmd.AddCommand(
		newJobEnqueueCmd(g),
		newJobGetCmd(g),
		newJobAttemptsCmd(g),
		newJobCancelCmd(g),
		newJobRetryCmd(g),
		newJobListCmd(g),
	)
	return cmd
}

func newJobEnqueueCmd(g *globalFlags) *cobra.Command {
	var (
		queue, typ, payload, payloadFile, runAt, idem string
		priority, maxAttempts                         int
	)
	cmd := &cobra.Command{
		Use:   "enqueue",
		Short: "Enqueue a job",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if queue == "" || typ == "" {
				return usageErrorf("--queue and --type are required")
			}
			mode, err := g.mode()
			if err != nil {
				return err
			}
			pl, err := loadPayload(payload, payloadFile)
			if err != nil {
				return err
			}
			runAtTime, err := parseRunAt(runAt, time.Now().UTC())
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
			j, err := c.Enqueue(ctx, sdk.EnqueueRequest{
				Queue:          queue,
				Type:           typ,
				Payload:        pl,
				Priority:       priority,
				RunAt:          runAtTime,
				MaxAttempts:    maxAttempts,
				IdempotencyKey: idem,
			})
			if err != nil {
				return mapSDKError(err)
			}
			return writeJob(cmd.OutOrStdout(), mode, j)
		},
	}
	cmd.Flags().StringVar(&queue, "queue", "", "queue name")
	cmd.Flags().StringVar(&typ, "type", "", "job type")
	cmd.Flags().StringVar(&payload, "payload", "", "JSON payload")
	cmd.Flags().StringVar(&payloadFile, "payload-file", "", "path to JSON payload file")
	cmd.Flags().IntVar(&priority, "priority", 0, "priority")
	cmd.Flags().StringVar(&runAt, "run-at", "", "RFC3339 or relative (+5m)")
	cmd.Flags().IntVar(&maxAttempts, "max-attempts", 0, "max attempts (0 = server default)")
	cmd.Flags().StringVar(&idem, "idempotency-key", "", "idempotency key")
	return cmd
}

func newJobGetCmd(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get <id>",
		Short: "Get a job by ID",
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
			j, err := c.GetJob(ctx, args[0])
			if err != nil {
				return mapSDKError(err)
			}
			return writeJob(cmd.OutOrStdout(), mode, j)
		},
	}
}

func newJobAttemptsCmd(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "attempts <id>",
		Short: "List attempts for a job",
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
			attempts, err := c.ListAttempts(ctx, args[0])
			if err != nil {
				return mapSDKError(err)
			}
			return writeAttempts(cmd.OutOrStdout(), mode, attempts)
		},
	}
}

func newJobCancelCmd(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "cancel <id>",
		Short: "Cancel a job",
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
			j, err := c.CancelJob(ctx, args[0])
			if err != nil {
				return mapSDKError(err)
			}
			return writeJob(cmd.OutOrStdout(), mode, j)
		},
	}
}

func newJobRetryCmd(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "retry <id>",
		Short: "Retry a dead or cancelled job",
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
			j, err := c.RetryJob(ctx, args[0])
			if err != nil {
				return mapSDKError(err)
			}
			return writeJob(cmd.OutOrStdout(), mode, j)
		},
	}
}

func newJobListCmd(g *globalFlags) *cobra.Command {
	var queue, status string
	var limit int
	var all bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List jobs by queue and status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if queue == "" || status == "" {
				return usageErrorf("--queue and --status are required")
			}
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

			var jobs []sdk.JobInfo
			if all {
				for j, err := range c.ListJobs(ctx, queue, status, sdk.ListJobsOptions{Limit: limit}) {
					if err != nil {
						return mapSDKError(err)
					}
					jobs = append(jobs, j)
				}
			} else {
				pageLimit := limit
				if pageLimit <= 0 {
					pageLimit = 50
				}
				n := 0
				for j, err := range c.ListJobs(ctx, queue, status, sdk.ListJobsOptions{Limit: pageLimit}) {
					if err != nil {
						return mapSDKError(err)
					}
					jobs = append(jobs, j)
					n++
					if n >= pageLimit {
						break
					}
				}
			}
			return writeJobs(cmd.OutOrStdout(), mode, jobs)
		},
	}
	cmd.Flags().StringVar(&queue, "queue", "", "queue name")
	cmd.Flags().StringVar(&status, "status", "", "job status")
	cmd.Flags().IntVar(&limit, "limit", 0, "page size")
	cmd.Flags().BoolVar(&all, "all", false, "fetch all pages")
	return cmd
}
