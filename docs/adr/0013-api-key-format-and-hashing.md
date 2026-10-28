# 0013. API key format and hashing

## Status

Accepted

## Context

Phase 1’s single static `RELAY_API_KEY` cannot express producer vs worker vs admin roles
or rotate credentials safely. Stored secrets must not be recoverable from the database,
yet verification must stay cheap on every request.

## Decision

### Format

Secrets are `rly_<8-char prefix>_<base62(32 random bytes)>`, generated with
`crypto/rand`. The full secret is shown **once** at creation (REST, gRPC, `relayctl`, or
`relay-server apikey create`). Listings expose only the prefix.

### Storage and verify

- Persist **SHA-256(secret)** (binary). High-entropy random secrets do not need
  bcrypt/argon2; a fast hash plus constant-time compare is appropriate.
- Lookup by prefix, then `subtle.ConstantTimeCompare` on the hash.
- `last_used_at` updates at most once per minute per key (in-memory throttle).

### Cache and bootstrap

- Verified-key **LRU cache** with TTL `RELAY_AUTH_CACHE_TTL` (default 30s): revocation
  takes effect within one TTL.
- `RELAY_API_KEY` remains a **bootstrap admin** principal. Warn if set alongside stored
  keys; warn loudly if auth is fully disabled (no stored keys and empty bootstrap).

### Scopes

`producer`, `worker`, and `admin` (`admin` implies all). Authorization runs in the app
layer (`RequireScope`) so REST and gRPC share one matrix.

## Consequences

- Compromised DB hashes cannot be turned back into working Bearer tokens without
  guessing high-entropy secrets.
- Operators must save the secret at create time; there is no reveal endpoint.
- Revocation is eventually consistent up to the auth cache TTL (acceptable for this
  control plane).
