package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
	"github.com/MEHRDADsmj/relay/internal/platform/config"
	"github.com/MEHRDADsmj/relay/internal/platform/logging"
)

func runAPIKey(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: relay-server apikey create --name <n> --scopes <s1,s2,...>")
	}
	switch args[0] {
	case "create":
		return runAPIKeyCreate(args[1:])
	default:
		return fmt.Errorf("unknown apikey command %q (want create)", args[0])
	}
}

func runAPIKeyCreate(args []string) error {
	var name, scopesCSV string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--name":
			if i+1 >= len(args) {
				return fmt.Errorf("--name requires a value")
			}
			i++
			name = args[i]
		case "--scopes":
			if i+1 >= len(args) {
				return fmt.Errorf("--scopes requires a value")
			}
			i++
			scopesCSV = args[i]
		default:
			return fmt.Errorf("unknown flag %q", args[i])
		}
	}
	if name == "" {
		return fmt.Errorf("--name is required")
	}
	if scopesCSV == "" {
		return fmt.Errorf("--scopes is required")
	}
	scopes, err := parseScopesCSV(scopesCSV)
	if err != nil {
		return err
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log, err := logging.New(cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	repo, cleanup, err := openRepository(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer cleanup()

	apiKeys := app.NewAPIKeyService(mustAPIKeyRepo(repo), app.SystemClock{})
	adminCtx := app.WithPrincipal(ctx, app.BootstrapAdmin)
	created, err := apiKeys.Create(adminCtx, app.CreateAPIKeyCommand{
		Name:   name,
		Scopes: scopes,
	})
	if err != nil {
		return err
	}
	fmt.Println(created.Secret)
	return nil
}

func parseScopesCSV(csv string) ([]domain.Scope, error) {
	parts := strings.Split(csv, ",")
	out := make([]domain.Scope, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		sc := domain.Scope(p)
		switch sc {
		case domain.ScopeProducer, domain.ScopeWorker, domain.ScopeAdmin, domain.ScopeDashboardRead:
			out = append(out, sc)
		default:
			return nil, fmt.Errorf("unknown scope %q (want producer|worker|admin|dashboard:read)", p)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("--scopes must list at least one scope")
	}
	return out, nil
}
