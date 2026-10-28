package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

func newAPIKeyCmd(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "apikey",
		Aliases: []string{"api-key"},
		Short:   "Manage API keys",
	}
	cmd.AddCommand(
		newAPIKeyCreateCmd(g),
		newAPIKeyListCmd(g),
		newAPIKeyRevokeCmd(g),
	)
	return cmd
}

func apiKeyDTO(k sdk.APIKeyInfo) map[string]any {
	m := map[string]any{
		"id":         k.ID,
		"name":       k.Name,
		"prefix":     k.Prefix,
		"scopes":     k.Scopes,
		"created_at": formatTime(k.CreatedAt),
	}
	if k.LastUsedAt != nil {
		m["last_used_at"] = formatTimePtr(k.LastUsedAt)
	}
	if k.ExpiresAt != nil {
		m["expires_at"] = formatTimePtr(k.ExpiresAt)
	}
	if k.RevokedAt != nil {
		m["revoked_at"] = formatTimePtr(k.RevokedAt)
	}
	return m
}

func writeAPIKey(w io.Writer, mode outputMode, k sdk.APIKeyInfo, secret string) error {
	if mode == outputJSON {
		m := apiKeyDTO(k)
		if secret != "" {
			m["secret"] = secret
		}
		return writeJSON(w, m)
	}
	rows := [][2]string{
		{"ID", k.ID},
		{"Name", k.Name},
		{"Prefix", k.Prefix},
		{"Scopes", strings.Join(k.Scopes, ",")},
		{"CreatedAt", formatTime(k.CreatedAt)},
	}
	if secret != "" {
		rows = append(rows, [2]string{"Secret", secret})
	}
	if k.RevokedAt != nil {
		rows = append(rows, [2]string{"RevokedAt", formatTimePtr(k.RevokedAt)})
	}
	return writeKVTable(w, rows)
}

func writeAPIKeys(w io.Writer, mode outputMode, keys []sdk.APIKeyInfo) error {
	if mode == outputJSON {
		out := make([]map[string]any, 0, len(keys))
		for _, k := range keys {
			out = append(out, apiKeyDTO(k))
		}
		return writeJSON(w, out)
	}
	tw := newTable(w)
	if _, err := fmt.Fprintln(tw, "ID\tNAME\tPREFIX\tSCOPES\tCREATED"); err != nil {
		return err
	}
	for _, k := range keys {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
			k.ID, k.Name, k.Prefix, strings.Join(k.Scopes, ","), formatTime(k.CreatedAt)); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func newAPIKeyCreateCmd(g *globalFlags) *cobra.Command {
	var name, scopes string
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create an API key (secret shown once)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if name == "" || scopes == "" {
				return usageErrorf("--name and --scopes are required")
			}
			mode, err := g.mode()
			if err != nil {
				return err
			}
			parts := strings.Split(scopes, ",")
			cleaned := make([]string, 0, len(parts))
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p != "" {
					cleaned = append(cleaned, p)
				}
			}
			if len(cleaned) == 0 {
				return usageErrorf("--scopes must list at least one scope")
			}
			c, err := g.client()
			if err != nil {
				return err
			}
			defer c.Close() //nolint:errcheck
			ctx, cancel := g.ctx(cmd)
			defer cancel()
			created, err := c.CreateAPIKey(ctx, sdk.CreateAPIKeyRequest{Name: name, Scopes: cleaned})
			if err != nil {
				return mapSDKError(err)
			}
			return writeAPIKey(cmd.OutOrStdout(), mode, created.Key, created.Secret)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "key name")
	cmd.Flags().StringVar(&scopes, "scopes", "", "comma-separated scopes (producer,worker,admin)")
	return cmd
}

func newAPIKeyListCmd(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List API keys",
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
			keys, err := c.ListAPIKeys(ctx)
			if err != nil {
				return mapSDKError(err)
			}
			return writeAPIKeys(cmd.OutOrStdout(), mode, keys)
		},
	}
}

func newAPIKeyRevokeCmd(g *globalFlags) *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "revoke <id>",
		Short: "Revoke an API key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := confirmDestructive("revoke API key "+args[0], yes, cmd.InOrStdin(), cmd.ErrOrStderr()); err != nil {
				return err
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
			k, err := c.RevokeAPIKey(ctx, args[0])
			if err != nil {
				return mapSDKError(err)
			}
			return writeAPIKey(cmd.OutOrStdout(), mode, k, "")
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "skip confirmation")
	return cmd
}
