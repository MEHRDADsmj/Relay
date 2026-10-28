# 0021. Dashboard session security

## Status

Accepted

## Context

The dashboard performs privileged actions (pause queues, retry dead letters, toggle
schedules). Browser sessions need a different auth model than raw `Authorization: Bearer`
on every XHR, while revocation of API keys must still apply quickly. XSS/CSRF and cookie
theft are first-class risks for a cookie-backed UI.

## Decision

- **Login:** form posts an API key. Accept keys with `admin` (actions + read) or
  `dashboard:read` (read-only). Session stores **key ID + expiry**, not the secret.
- **Session cookie:** AES-GCM sealed payload; key from `RELAY_UI_SESSION_KEY` (32 bytes
  base64). If unset in development, generate an ephemeral key and warn; required when
  `RELAY_ENVIRONMENT=production`. Flags: `HttpOnly`, `SameSite=Lax`, `Path=/ui`, 12h
  expiry; `Secure` when `RELAY_UI_SECURE_COOKIES` is true (default true in production).
- **Re-validation:** on each request, resolve the key ID via `AuthService` (cached) so
  revocation applies within the auth cache TTL.
- **CSRF:** synchronizer token bound to the session (hidden field / HTMX headers); verified
  on all non-GET requests. Double-submit cookies are not used.
- **Security headers** on `/ui`: strict CSP
  (`default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:;
  connect-src 'self'; frame-ancestors 'none'`), `X-Content-Type-Options: nosniff`,
  `Referrer-Policy: same-origin`.
- **Login rate limit:** 5 attempts/min per IP (in-memory token bucket).
- Templ auto-escapes; job payloads are never rendered as raw HTML.
- Kill switch: `RELAY_UI_ENABLED` (default `true`).

## Consequences

- Operators sign in once with a scoped key; workers/producers keep using Bearer tokens on
  the API.
- Production deploys must set a stable `RELAY_UI_SESSION_KEY` or sessions reset on restart.
- Dashboard actions still go through app scope checks (`admin` for mutations).
