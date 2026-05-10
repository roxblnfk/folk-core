# folk-core

Rust-based application server for PHP. Long-lived workers, fork-based runtime with warm OPcache, plugin ecosystem.

> **Status:** in active development. See [folk-spec](https://github.com/Folk-Project/folk-spec) for the roadmap.

## Requirements

- Rust 1.85+
- PHP 8.2+ with `ext-msgpack`
- Unix-like OS (Linux or macOS)

## Installation

```toml
# Cargo.toml
folk-core = "0.1"
```

For most users, build a custom binary with [folk-builder](https://github.com/Folk-Project/folk-builder) instead of depending on this crate directly.

## Quick start

1. Create a minimal `folk.toml`:

```toml
[server]
runtime = "pipe"
rpc_socket = "/tmp/folk.sock"
shutdown_timeout = "30s"

[workers]
script = "vendor/bin/folk-worker"
php = "php"
count = 4

[log]
filter = "info"
format = "text"
```

2. Start the server:

```bash
folk serve --config folk.toml
```

3. Expected startup output:

```
INFO folk server starting version=0.1.0 workers=4 runtime=pipe
INFO worker pool started
INFO all plugins booted plugins=["http", "metrics"]
INFO admin RPC server listening socket=/tmp/folk.sock
```

4. Stop with `Ctrl-C` or `SIGTERM`. The server drains gracefully:

```
INFO received SIGTERM
INFO shutdown signal received; draining
INFO http shutting down
INFO metrics shutting down
INFO folk server stopped
```

## Configuration

All fields can be overridden with environment variables using the `FOLK_` prefix (e.g., `FOLK_WORKERS_COUNT=8`). Config is loaded as: defaults → `folk.toml` → environment variables.

### `[server]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `runtime` | `"pipe"` \| `"fork"` | `"pipe"` | Worker runtime. `pipe` spawns via execve; `fork` uses a prefork master with warm OPcache. |
| `rpc_socket` | `String` | `"/tmp/folk.sock"` | Unix socket path for the admin RPC server. |
| `shutdown_timeout` | `Duration` | `"30s"` | Maximum time to wait for graceful shutdown before forcing exit. |

### `[workers]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `script` | `String` | `"vendor/bin/folk-worker"` | Path to the PHP worker entry script. |
| `php` | `String` | `"php"` | PHP binary path. |
| `count` | `usize` | `4` | Number of worker processes. |
| `max_jobs` | `u64` | `1000` | Requests a worker handles before being recycled. |
| `ttl` | `Duration` | `"3600s"` | Maximum worker lifetime before recycling. |
| `max_memory_mb` | `u64` | `256` | RSS memory limit (MB) before recycling. |
| `exec_timeout` | `Duration` | `"30s"` | Per-request execution timeout. |
| `boot_timeout` | `Duration` | `"30s"` | Timeout for worker to send `control.ready`. |

### `[log]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `filter` | `String` | `"info"` | Log level filter. Supports per-module: `folk_core=trace`. Overridden by `RUST_LOG`. |
| `format` | `"text"` \| `"json"` \| `"pretty"` | `"text"` | Log output format. Use `json` for log aggregators. |

## How it works

Folk supervises long-lived PHP processes and dispatches HTTP requests, jobs, and gRPC calls to them over a length-prefixed MessagePack-RPC protocol on Unix sockets.

### Runtimes

**Pipe runtime** (default) — spawns each PHP worker via `execve` with a Unix socketpair. Each worker is independent and stateless. Workers communicate over two file descriptors: FD 3 (task channel) and FD 4 (control channel).

**Fork runtime** — a prefork master boots the PHP framework once, warming OPcache and autoload caches. Workers are then `fork()`-ed from the master, inheriting the warm state. This eliminates cold-start overhead per worker. Requires `ext-pcntl`.

### Worker lifecycle

Workers cycle through states: **Spawning** → **Idle** → **Busy** → back to Idle, until recycled. A worker is recycled when it exceeds `max_jobs`, `ttl`, or `max_memory_mb`. Recycled workers are replaced immediately.

### Shutdown behavior

On `SIGTERM` or `SIGINT`:

1. The shutdown signal broadcasts to all components.
2. Plugins shut down in **reverse registration order**.
3. Workers finish in-flight requests (graceful drain).
4. If drain exceeds `shutdown_timeout`, remaining tasks are aborted.
5. The admin RPC socket is removed and the process exits.

### Plugin system

Plugins are compiled into the binary via [folk-builder](https://github.com/Folk-Project/folk-builder). Each plugin depends only on [folk-api](https://github.com/Folk-Project/folk-api), not on folk-core. Plugins boot in registration order and shut down in reverse. See [folk-api](https://github.com/Folk-Project/folk-api) for the plugin authoring guide.

### Repository layout

```
crates/
├── folk-protocol/     wire format (RpcMessage, FrameCodec)
├── folk-core/         server core (worker pool, plugin registry)
├── folk-runtime-pipe/ runtime: spawn via execve + Unix socketpair
└── folk-runtime-fork/ runtime: warm OPcache via prefork
folk/                  reference binary (folk serve)
```

## License

MIT
