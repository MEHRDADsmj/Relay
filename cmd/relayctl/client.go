package main

import (
	"context"
	"errors"
	"time"

	"github.com/spf13/cobra"

	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

type globalFlags struct {
	server  string
	apiKey  string
	output  string
	timeout time.Duration
}

func (g globalFlags) mode() (outputMode, error) {
	switch outputMode(g.output) {
	case outputTable, outputJSON:
		return outputMode(g.output), nil
	default:
		return "", usageErrorf("invalid --output %q (want table|json)", g.output)
	}
}

func (g globalFlags) client() (*sdk.Client, error) {
	opts := []sdk.ClientOption{}
	if g.apiKey != "" {
		opts = append(opts, sdk.WithAPIKey(g.apiKey))
	}
	c := sdk.NewClient(g.server, opts...)
	return c, nil
}

func (g globalFlags) ctx(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	base := cmd.Context()
	if base == nil {
		base = context.Background()
	}
	return context.WithTimeout(base, g.timeout)
}

func mapSDKError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sdk.ErrNotFound) {
		return &cliError{code: exitNotFound, err: err}
	}
	var ae *sdk.APIError
	if errors.As(err, &ae) {
		if ae.StatusCode == 404 {
			return &cliError{code: exitNotFound, err: err}
		}
		return &cliError{code: exitAPI, err: err}
	}
	return &cliError{code: exitAPI, err: err}
}
