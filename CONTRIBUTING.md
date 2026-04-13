# Contributing to cm

Thanks for your interest in contributing! Here's how to get started.

## Building

```bash
cargo build
```

Minimum supported Rust version: **1.85** (edition 2024).

## Running Tests

```bash
cargo test
```

## Code Style

This project uses standard Rust formatting and linting:

```bash
cargo fmt --check
cargo clippy -- -D warnings
```

Please run both before submitting a pull request.

## Pull Requests

1. Fork the repository and create a feature branch.
2. Make your changes with clear, focused commits.
3. Ensure `cargo test`, `cargo fmt --check`, and `cargo clippy -- -D warnings` all pass.
4. Open a pull request against `main`.

## Reporting Issues

Open an issue on [GitHub](https://github.com/mindrunner/cm-rs/issues) with
a clear description of the problem, steps to reproduce, and your environment
(OS, Rust version, cm version).

## License

By contributing you agree that your contributions will be licensed under the
[Apache License 2.0](LICENSE).
