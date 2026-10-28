package app

import (
	"context"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

type principalKey struct{}

// BootstrapAdmin is the principal for the RELAY_API_KEY bootstrap credential.
var BootstrapAdmin = domain.Principal{
	KeyID:  "bootstrap",
	Name:   "bootstrap",
	Scopes: []domain.Scope{domain.ScopeAdmin},
}

// WithPrincipal attaches p to ctx for RequireScope / PrincipalFrom.
func WithPrincipal(ctx context.Context, p domain.Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// PrincipalFrom returns the authenticated principal, if any.
func PrincipalFrom(ctx context.Context) (domain.Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(domain.Principal)
	return p, ok
}

// RequireScope ensures the context principal holds the required scope.
func RequireScope(ctx context.Context, scope domain.Scope) error {
	p, ok := PrincipalFrom(ctx)
	if !ok {
		return domain.ErrUnauthenticated
	}
	if !p.Can(scope) {
		return domain.ErrForbidden
	}
	return nil
}
