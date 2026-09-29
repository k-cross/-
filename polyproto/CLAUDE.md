# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Version control: Jujutsu, not Git

polyproto lives in a monorepo rooted at `~/src/delta`, a **colocated `jj` + Git repo** (`.jj/`
alongside `.git/`) that holds other projects too. Use `jj`, not `git`, for history and commits:

- `jj status`, `jj log`, `jj diff` — inspect state
- `jj describe -m "..."` — set the working-copy commit message
- `jj new` — start a new change

Commit messages are prefixed with the project, e.g. `[polyproto] Phase 4: implementation`.

## Build and test

Stock Cargo; there is no Makefile, justfile, or CI:

- `cargo build`, `cargo run`, `cargo test`
- `cargo fmt --check` and `cargo clippy --all-targets` are the quality gates. Lints are configured
  in Cargo.toml's `[lints]` tables — `clippy::pedantic` is on at `warn`, and
  `unsafe_op_in_unsafe_fn` / `unused_must_use` are **deny**. Keep both clean.

Edition is **2024** (MSRV floor 1.85). Target **stable** Rust — do not use nightly-only features.
`rust-version` is not pinned and there is no `rust-toolchain.toml`.

`rustfmt.toml` sets `edition = "2024"` so that standalone `rustfmt <file>` matches `cargo fmt`;
without it the bare binary defaults to edition 2015 and rewrites 2024 code incorrectly.

## Project state

polyproto is a Rust simulator (the `polyphonic` binary) for testing the design in `docs/`: a
residency ledger over HBM, host DDR and NVMe, a cluster topology, a batched serving-engine model,
scored placement, and a measured boundary-cost ladder. The `grpc` and `wasm` features enable the
gRPC/`ext_proc` and WASM rungs of that ladder; `census` marks the entry points that assume
allocation authority over engine state.

Start with `docs/prototype.md`, which maps the documents: @README.md is the direction (a manifesto,
not a spec), `docs/owned-and-observed.md` the design and phase plan, `docs/residency-ledger.md` the
model and its current results, and `docs/phase-N.md` one record per phase. The phase docs are
historical records -- predictions stated before a run, then what was measured -- so they keep their
results as measured; every other doc states only current decisions and results. Treat the README's
goals (WASM/ABI zero-cost extensions, VM-as-native-abstraction, unified scheduling across
FaaS/AI/edge) as direction, not as implemented behavior.

The build system beyond Cargo is undecided. Do not introduce Buck2, Bazel, or a Cargo workspace
without asking.
