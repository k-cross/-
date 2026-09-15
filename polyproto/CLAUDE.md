# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Version control: Jujutsu, not Git

This repo is a **colocated `jj` + Git repo** (`.jj/` alongside `.git/`). Use `jj`, not `git`, for
history and commits:

- `jj status`, `jj log`, `jj diff` — inspect state
- `jj describe -m "..."` — set the working-copy commit message
- `jj new` — start a new change

The Git side currently has **zero commits**, so `git log`, `git show`, and any tooling built on them
fail with `your current branch 'main' does not have any commits yet`. Reach for `jj log` instead.
There is no Git remote configured yet.

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

`src/main.rs` is still the unmodified `cargo new` hello-world and `[dependencies]` is empty. There is
no architecture in code yet — @README.md is the only record of design intent, and it is a manifesto
rather than a spec. Read it before proposing structure, and treat its goals (WASM/ABI zero-cost
extensions, VM-as-native-abstraction, unified scheduling across FaaS/AI/edge) as direction, not as
implemented behavior.

The build system beyond Cargo is undecided. Do not introduce Buck2, Bazel, or a Cargo workspace
without asking.
