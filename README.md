# folk-core

Server core for Folk -- worker pool, plugin registry, and ZTS runtime. Rust-based application server for PHP with long-lived workers and a plugin ecosystem.

**Version:** 0.2.0

## Workspace crates

```
crates/
  folk-core/       -- pool, config, runtime traits, plugin registry
  folk-ext/        -- PHP extension (folk.so), ZTS workers, bridge, zval conversion
  folk-protocol/   -- legacy wire format (FrameCodec, RpcMessage)
```

## Architecture

Folk supervises long-lived PHP workers and dispatches requests to them. In 0.2.0, the primary multi-worker path is **ZTS threads** via `folk-ext`:

- `folk-ext` builds a PHP extension (`folk.so`) as a cdylib
- Workers run as ZTS threads inside the Rust process
- Dispatch uses `execute_value` -- Rust passes `serde_json::Value` directly to PHP via zval conversion, avoiding serialization overhead
- No fork, no pipes, no msgpack

## Configuration

```toml
[server]
shutdown_timeout = "30s"

[workers]
count = 4
max_jobs = 1000
ttl = "1h"
exec_timeout = "30s"
```

All fields support `FOLK_` env prefix overrides (e.g. `FOLK_WORKERS_COUNT=8`).

## Requirements

- Rust 1.88+ (MSRV)
- PHP 8.2+ with ZTS build (for folk-ext)
- Linux or macOS

## Installation

Most users build a custom binary via [folk-builder](https://github.com/Folk-Project/folk-builder) rather than depending on this crate directly.

```toml
folk-core = "0.2"
```

## Shutdown

On `SIGTERM`/`SIGINT`:

1. Shutdown signal broadcasts to all components.
2. Plugins shut down in reverse registration order.
3. Workers finish in-flight requests (graceful drain).
4. If drain exceeds `shutdown_timeout`, remaining tasks are aborted.

## License

MIT
