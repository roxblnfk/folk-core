# Folk

Rust-based application server for PHP. Long-lived workers, fork-based runtime with warm OPcache, plugin ecosystem.

> **Status:** in active development. Phase 0 (workspace setup) complete; subsequent phases are tracked in [`folk-spec`](https://github.com/Folk-Project/folk-spec).

## What is this?

Folk supervises long-lived PHP processes and dispatches HTTP requests, jobs, and gRPC calls to them over a length-prefixed MessagePack-RPC protocol on Unix sockets. It is conceptually similar to RoadRunner.

The differentiator: a fork-based runtime that gives each PHP worker a warm OPcache without paying the cold-start cost RoadRunner pays per worker.

## Repository layout

```
crates/
├── folk-protocol/     wire format (RpcMessage, FrameCodec)
├── folk-core/         server core (worker pool, plugin registry)
├── folk-runtime-pipe/ runtime: spawn via execve + Unix socketpair
└── folk-runtime-fork/ runtime: warm OPcache via prefork (phase 10)
folk/                  reference binary (`folk serve`)
```

`folk-api` (plugin contract) lives in a **separate repository** (`Folk-Project/folk-api`).
Plugins depend on it directly — they do not pull in `folk-core`.

Plugins (HTTP, jobs, metrics, process supervisor, gRPC) each live in separate repositories and are composed at build time via `folk-builder`.

## Building

```bash
cargo build --release
```

This produces `./target/release/folk`. Once phase 5 is complete, you'll be able to run it against a PHP application.

## Documentation

- Architecture: see `folk-spec` (separate repo).
- Architecture decisions: see `folk-spec/adr/`.
- Phase-by-phase build plan: see `folk-spec/phases/`.

## License

MIT.
