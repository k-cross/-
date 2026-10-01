# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

limitless-deck is a Bevy 0.19 slide deck (edition 2024) inside the `~/src/delta` monorepo, which holds other projects too. Commit messages are prefixed `[limitless-deck]`. Do not commit unless asked.

## Verifying changes

There are no tests, and `cargo run` opens a 1280x720 window you cannot see. Verify with:

- `cargo check`
- `cargo clippy --all-targets`
- `cargo fmt --check`

All three must be clean. The baseline currently fails: clippy has a deny-level `approx_constant` in `src/slideshow/animation.rs` and `cargo fmt --check` has a diff in `src/slideshow/mod.rs`. Those predate your edits.

Use `cargo fmt`, not bare `rustfmt <file>` (it defaults to edition 2015 and there is no `rustfmt.toml`); the hook in the repo-root `.claude/settings.json` passes `--edition 2024` for edited `.rs` files.

## Adding or reordering slides

A slide is wired up in four places, and nothing fails to compile if one is missed:

- a `SlideState` variant in `src/slideshow/mod.rs`
- the matching arm in `SlideState::from_index`
- `SlideState::TOTAL_COUNT`
- an `OnEnter` registration in `SlidesPlugin::build` in `src/slides/mod.rs`

Every entity a slide spawns needs `DespawnOnExit(SlideState::X)`, otherwise it persists onto the next slide. `src/slides/auto_slides.rs` takes the state as a parameter in its shared helper; `intro.rs` inserts it per entity.

`README.md` has the slide list, palette and font roles (`src/theme.rs`, `src/slideshow/fonts.rs`). Its file tree and slide count lag the code.

## Style

Existing files here carry many `//` and `///` comments. The global no-comments rule still applies to anything you write; do not match the surrounding density.
