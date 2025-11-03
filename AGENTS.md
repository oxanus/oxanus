# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Building and Testing

- `cargo build` - Build the project
- `cargo test` - Run all tests (unit and integration tests)
- `cargo test <test_name>` - Run specific tests by name
- `cargo bench` - Run benchmarks (uses divan benchmarking framework)
- `cargo check` - Quick syntax and type checking without building
- `cargo clippy` - Run Rust linter with extensive warnings enabled
- `cargo doc` - Generate documentation

### Package Structure

This is a Rust workspace with two main crates:

- `oxanus/` - Main job processing library
- `oxanus-api/` - API placeholder crate (minimal implementation)

## Architecture Overview

Oxanus is a job processing library built around several core components:

### Core Components

- **Storage**: Main interface for job management, handles enqueueing, scheduling, and monitoring
- **Config**: Configuration builder that registers queues and workers, manages graceful shutdown
- **Context**: Provides shared state and utilities to workers
- **Worker**: Trait defining job processing logic
- **Queue**: Channels through which jobs flow (static or dynamic)
- **JobEnvelope**: Wrapper containing job data and metadata
- **Coordinator**: Orchestrates job processing across queues
- **Dispatcher**: Routes jobs to appropriate workers
- **Executor**: Handles actual job execution

### Key Design Patterns

- Uses Redis as the backing store (via deadpool-redis)
- Graceful shutdown handling with signal management (SIGTERM/SIGINT on Unix, Ctrl+C on Windows)
- Comprehensive error handling with custom `OxanusError` type
- Extensive Clippy linting rules enforced for code quality
- Supports both static queues (compile-time) and dynamic queues (runtime)
- Job uniqueness, throttling, retries, and scheduling capabilities

### Multi-crate Structure

The workspace is organized as:

- Root `Cargo.toml` defines workspace members and shared package metadata
- `oxanus/` contains the main library implementation
- `oxanus-api/` is a placeholder for future API functionality

### Testing

- Unit tests are co-located with source code
- Integration tests are in `oxanus/tests/integration/`
- Test utilities are in `test_helper.rs`
- Uses `testresult` crate for test error handling
- Benchmarks use the divan framework in `oxanus/benches/`

### Examples

Comprehensive examples are provided in `oxanus/examples/` covering:

- Basic usage (`minimal.rs`)
- Cron scheduling (`cron.rs`)
- Dynamic queues (`dynamic.rs`)
- Throttling (`throttled.rs`)
- Unique jobs (`unique.rs`)
- Resumable jobs (`resumable.rs`)
