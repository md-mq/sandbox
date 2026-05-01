# sandbox / plx-exec

`plx-exec` is the Polyaxon sandbox daemon. It runs inside the user container,
listens on `:9090`, and serves exec / PTY / filesystem requests from the
Polyaxon streams proxy. Design lives in the top-level memos:

- `memos/sandbox/architecture.md` — top-level sandbox abstraction
- `memos/sandbox/plugins-sandbox.md` — plugin design
- `memos/sandbox/plx-exec-api.md` — HTTP/WS contract
- `memos/sandbox/authentication-decisions.md` — why we auth this way
- `memos/sandbox/roadmap.md` — phased rollout, current status

## Status

Exec endpoints live. `/ping`, `/exec`, `/exec/stream`, `/exec/bg`
(+ status, logs, signal, delete) all implemented with file-backed output and
crash recovery. PTY and filesystem endpoints land in later phases.

## Build

```
make build              # local dev binary → bin/plx-exec
make build-static       # static linux build → bin/plx-exec-linux-$ARCH
```

## Test

```
make test
make lint
```

## Run locally

```
make run                # starts in PING_ONLY mode, no token required
curl localhost:9090/ping
```

Override config via env vars (all prefixed `POLYAXON_SANDBOX_`):

| Var | Default | Purpose |
|---|---|---|
| `POLYAXON_SANDBOX_LISTEN_ADDR` | `:9090` | HTTP listen address |
| `POLYAXON_SANDBOX_TOKEN_FILE` | `/opt/polyaxon/sandbox-token` | Auth token path |
| `POLYAXON_SANDBOX_STATE_DIR` | `/tmp/plx-exec` | On-disk exec state |
| `POLYAXON_SANDBOX_LOG_FORMAT` | `json` | `json` or `text` |
| `POLYAXON_SANDBOX_SHUTDOWN_TIMEOUT` | `10s` | Graceful shutdown deadline |
| `POLYAXON_SANDBOX_PING_ONLY` | unset | Skip token requirement (dev only) |
| `POLYAXON_SANDBOX_MAX_EXECS` | `64` | Concurrent running exec cap; 65th returns 429 |

## Layout

```
cmd/plx-exec/       # main entry point
internal/
  config/           # env-driven config loader
  auth/             # constant-time token check
  server/           # HTTP server, middleware, handlers
```

`internal/` prevents external imports — this binary is a leaf product.

## Threat model

plx-exec is **not a tenancy boundary**. It is a daemon that runs inside the user's container and serves the same principal who already owns the pod. What it does and does not protect:

**What it protects:**
- Co-tenant pods on the same cluster (they don't have this pod's token)
- Tokens extracted from one pod being reused against another (each token is HMAC-derived from a single `run_uuid`)

**What it does NOT protect, by design:**
- User code inside this container calling `localhost:9090`. The token is mounted into the same filesystem the user's own code reads; the user CAN read it and authenticate. That's fine — user code can already do anything it wants inside its own container via normal process-level means. plx-exec is a convenience daemon, not a sandbox-within-a-sandbox.
- `POLYAXON_*` env-key rejection on `/exec*` is audit hygiene (prevents accidental clobbering of platform-injected env), not a security boundary.
- No workdir jailing, no PATH / `LD_PRELOAD` filtering, no syscall sandbox.

Authn/authz for end users happens upstream at the Polyaxon streams proxy. plx-exec trusts that layer to have checked RBAC before forwarding.
