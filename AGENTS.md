# AGENTS.md

Project map for AI coding agents working in this code repository.

## What this repository is

The Folk Rust codebase: protocol, plugin API, server core, two runtimes, reference binary.

## Where the design lives

The complete specification — architecture, ADRs, phase-by-phase tasks — is in the [`folk-spec`](https://github.com/Folk-Project/folk-spec) repository. **You must read it before writing code in this repo.**

In particular:

- `folk-spec/AGENTS.md` — global rules and tech stack
- `folk-spec/spec/` — high-level documents
- `folk-spec/adr/` — every architectural decision (read all of them)
- `folk-spec/phases/phase-N-<name>/` — the active phase's instructions

## Crates

| Crate                  | Repo        | Phase | Purpose                                        |
|------------------------|-------------|------:|------------------------------------------------|
| `folk-api`             | `folk-api`  |     2 | Plugin trait, PluginContext, registries (separate repo) |
| `folk-protocol`        | `folk-core` |     1 | RpcMessage, FrameCodec                         |
| `folk-core`            | `folk-core` |     3 | Config, logging, worker pool, plugin registry  |
| `folk-runtime-pipe`    | `folk-core` |     4 | Spawn PHP via execve                           |
| `folk-runtime-fork`    | `folk-core` |    10 | Warm OPcache via fork                          |
| `folk` (binary)        | `folk-core` |     5 | Reference CLI                                  |

## House rules

See `folk-spec/AGENTS.md`. The headlines:

- No contract version strings (`q4.X`, `i007a`, `b003a`). Single `FOLK_API_VERSION` only.
- Use `serde` derive + `rmp_serde`, not hand-rolled `rmpv::Value`.
- Use `tokio_util::codec::LengthDelimitedCodec`, not hand-rolled framing.
- No workload declarations, no capability negotiation, no admission failure taxonomy.
- YAGNI: if a feature isn't required by the current phase, it doesn't exist.

## Verification before committing

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo check --workspace --all-targets
cargo test --workspace --all-targets
```

All four must pass.
