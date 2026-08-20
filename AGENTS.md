# TauriTavern Fork - Agent Guidelines

## Project scope

This repository is a fork of Darkatse/TauriTavern and is based on the upstream `dev` branch.

Primary supported platforms for this fork:

- Windows
- Android

Do not spend time adding or maintaining macOS, Linux, or iOS-specific behavior unless shared cross-platform code requires it.

The developer works on Windows and uses PowerShell. Prefer PowerShell-compatible commands in instructions and examples.

Do not create, enable, or modify scheduled GitHub Actions workflows unless explicitly requested.

Do not publish releases, create tags, push commits, merge branches, or push to GitHub unless explicitly requested.

## Before coding

Before implementing a feature:

1. Understand the requested behavior.
2. Search the repository for similar existing functionality.
3. Read the relevant documentation under `docs/`.
4. For backend architecture changes, read `docs/BackendStructure.md`.
5. For frontend changes, read `docs/FrontendGuide.md`.
6. For frontend/backend contracts, read `docs/FrontendHostContract.md`.
7. Explain a short implementation plan before making large changes.

Prefer modifying an existing mechanism over creating a parallel implementation.

Keep changes focused on the requested feature.

## Engineering principles

Prefer simple general solutions over special cases.

Use first-principles reasoning:
- determine the intended behavior,
- inspect existing behavior,
- find evidence for the cause,
- then decide where to change the code.

Fail fast when an operation cannot be completed or verified.
Do not silently ignore errors or invent fallback results for unknown states.

Maintainability and readability are more important than clever abstractions.

Follow KISS. Do not create abstractions, factories, traits, or helper layers unless they solve a real current problem.

Reuse existing helpers and conventions whenever possible.

## Rust / backend architecture

Follow the workspace crate boundaries defined in `docs/BackendStructure.md`.

The `tauritavern` host should contain only Tauri shell/composition concerns such as:
- commands,
- AppHandle/WebView integration,
- resources,
- platform glue,
- composition.

Tauri-independent concrete I/O belongs in the appropriate `tt-adapter-*` crate.

Repository traits and outbound ports belong in `tt-ports`.

Use idiomatic Rust.

Prefer `Result` with `thiserror` / `anyhow` where appropriate.

Use clear error propagation.

At the Tauri command boundary use the project's `CommandError` conventions.

Domain/repository code should use the project's domain error conventions.

Use async/await and Tokio for I/O or concurrent work when appropriate.

`#[tauri::command]` belongs in the presentation layer and should call application-layer services instead of implementing complex business logic directly.

Use DTOs across application/presentation boundaries and keep frontend/backend contracts consistent.

## Android

Android is a first-class platform for this fork.

Before changing Android-specific behavior, read `docs/AndroidDevelopment.md`.

Reuse existing Android platform abstractions instead of adding duplicate platform-specific paths.

Do not modify iOS behavior unless a shared implementation genuinely requires it.

## Validation

Start with focused tests for the code being changed.

Before considering a task complete, run:

    pnpm run check

For Windows desktop development:

    pnpm run tauri:dev

For a Windows build:

    pnpm run tauri:build

For Android development:

    pnpm run android:dev

For an Android debug APK:

    pnpm run android:build -- --debug --apk --target aarch64

Report which checks were actually run and whether they passed.

Do not claim a build or test passed unless it was actually executed successfully.

### Known Windows test flake

On Windows, the following agent runtime contract test can occasionally hit its
5-second timeout when the Rust test suite runs in parallel:

`app::contract_tests::agent_runtime::execution::agent_runtime_foreground_auto_commits_once_per_round_until_explicit_commit`

If `pnpm run check` fails only because this exact test reports:

`agent loop and host resolver timed out`

do not modify application code immediately.

First rerun the test by itself:

    cargo test --manifest-path src-tauri/Cargo.toml -p tauritavern --lib "app::contract_tests::agent_runtime::execution::agent_runtime_foreground_auto_commits_once_per_round_until_explicit_commit" -- --exact --nocapture

If it passes, verify the tauritavern Rust tests serially:

    cargo test --manifest-path src-tauri/Cargo.toml -p tauritavern --lib -- --test-threads=1

If both pass, report the original failure as the known parallel Windows timeout
flake. Do not claim that `pnpm run check` passed; report exactly which validation
commands passed.

If the isolated test also fails, investigate it as a real failure.