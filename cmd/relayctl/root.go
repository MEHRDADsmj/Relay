package main

import (
	"os"
	"time"

	"github.com/spf13/cobra"
)

func newRoot() *cobra.Command {
	g := &globalFlags{}

	root := &cobra.Command{
		Use:           "relayctl",
		Short:         "Operator CLI for the Relay job queue",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	fs := root.PersistentFlags()
	fs.StringVar(&g.server, "server", envOr("RELAY_URL", "http://localhost:7420"), "Relay server URL (RELAY_URL)")
	fs.StringVar(&g.apiKey, "api-key", os.Getenv("RELAY_API_KEY"), "API key (RELAY_API_KEY)")
	fs.StringVarP(&g.output, "output", "o", "table", "output format: table|json")
	fs.DurationVar(&g.timeout, "timeout", 10*time.Second, "per-command timeout")

	root.AddCommand(
		newVersionCmd(),
		newCompletionCmd(),
		newJobCmd(g),
		newQueueCmd(g),
		newScheduleCmd(g),
		newAPIKeyCmd(g),
	)
	return root
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func newCompletionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:       "completion [bash|zsh|fish|powershell]",
		Short:     "Generate shell completion scripts",
		Args:      cobra.ExactArgs(1),
		ValidArgs: []string{"bash", "zsh", "fish", "powershell"},
		RunE: func(cmd *cobra.Command, args []string) error {
			switch args[0] {
			case "bash":
				return cmd.Root().GenBashCompletion(cmd.OutOrStdout())
			case "zsh":
				return cmd.Root().GenZshCompletion(cmd.OutOrStdout())
			case "fish":
				return cmd.Root().GenFishCompletion(cmd.OutOrStdout(), true)
			case "powershell":
				return cmd.Root().GenPowerShellCompletionWithDesc(cmd.OutOrStdout())
			default:
				return usageErrorf("unsupported shell %q", args[0])
			}
		},
	}
	return cmd
}
