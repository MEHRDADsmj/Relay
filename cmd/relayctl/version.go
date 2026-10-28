package main

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

// Set via -ldflags at build time.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print relayctl version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "relayctl %s (commit=%s date=%s go=%s)\n",
				version, commit, date, runtime.Version())
			return err
		},
	}
}
