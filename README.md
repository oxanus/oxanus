# Oxanus
[![Build Status](https://img.shields.io/github/actions/workflow/status/oxanus/oxanus/test.yml?branch=main)](https://github.com/oxanus/oxanus/actions)
[![Latest Version](https://img.shields.io/crates/v/oxanus.svg)](https://crates.io/crates/oxanus)
[![docs.rs](https://img.shields.io/static/v1?label=docs.rs&message=oxanus&color=blue&logo=data:image/svg+xml;base64,PHN2ZyByb2xlPSJpbWciIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgdmlld0JveD0iMCAwIDUxMiA1MTIiPjxwYXRoIGZpbGw9IiNmNWY1ZjUiIGQ9Ik00ODguNiAyNTAuMkwzOTIgMjE0VjEwNS41YzAtMTUtOS4zLTI4LjQtMjMuNC0zMy43bC0xMDAtMzcuNWMtOC4xLTMuMS0xNy4xLTMuMS0yNS4zIDBsLTEwMCAzNy41Yy0xNC4xIDUuMy0yMy40IDE4LjctMjMuNCAzMy43VjIxNGwtOTYuNiAzNi4yQzkuMyAyNTUuNSAwIDI2OC45IDAgMjgzLjlWMzk0YzAgMTMuNiA3LjcgMjYuMSAxOS45IDMyLjJsMTAwIDUwYzEwLjEgNS4xIDIyLjEgNS4xIDMyLjIgMGwxMDMuOS01MiAxMDMuOSA1MmMxMC4xIDUuMSAyMi4xIDUuMSAzMi4yIDBsMTAwLTUwYzEyLjItNi4xIDE5LjktMTguNiAxOS45LTMyLjJWMjgzLjljMC0xNS05LjMtMjguNC0yMy40LTMzLjd6TTM1OCAyMTQuOGwtODUgMzEuOXYtNjguMmw4NS0zN3Y3My4zek0xNTQgMTA0LjFsMTAyLTM4LjIgMTAyIDM4LjJ2LjZsLTEwMiA0MS40LTEwMi00MS40di0uNnptODQgMjkxLjFsLTg1IDQyLjV2LTc5LjFsODUtMzguOHY3NS40em0wLTExMmwtMTAyIDQxLjQtMTAyLTQxLjR2LS42bDEwMi0zOC4yIDEwMiAzOC4ydi42em0yNDAgMTEybC04NSA0Mi41di03OS4xbDg1LTM4Ljh2NzUuNHptMC0xMTJsLTEwMiA0MS40LTEwMi00MS40di0uNmwxMDItMzguMiAxMDIgMzguMnYuNnoiPjwvcGF0aD48L3N2Zz4K)](https://docs.rs/oxanus/latest)


<p align="center">
  <picture>
    <img alt="Oxanus logo" src="https://raw.githubusercontent.com/oxanus/oxanus/refs/heads/main/logo.jpg" width="320">
  </picture>
</p>

Oxanus is job processing library written in Rust doesn't suck (or at least sucks in a completely different way than other options).

Oxanus goes for simplity and depth over breadth. It only aims support single backend with simple flow.

## Key Features

- **Isolated Queues**: Separate job processing queues with independent configurations
- **Retrying**: Automatic retry of failed jobs with configurable backoff
- **Scheduled Jobs**: Schedule jobs to run at specific times or after delays
- **Dynamic Queues**: Create and manage queues at runtime
- **Throttling**: Control job processing rates with queue-based throttling
- **Unique Jobs**: Ensure only one instance of a job runs at a time
- **Resilient Jobs**: Jobs that can survive worker crashes and restarts
- **Graceful Shutdown**: Clean shutdown of workers with in-progress job handling
- **Periodic Jobs**: Run jobs on a schedule using cron-like expressions

## Quick Start

```rust
use oxanus::{Worker, Queue, Context, Config, Storage};
use serde::{Serialize, Deserialize};

// Define your worker
#[derive(Debug, Serialize, Deserialize)]
struct MyWorker {
    data: String,
}

#[async_trait::async_trait]
impl Worker for MyWorker {
    type Context = MyContext;
    type Error = MyError;

    async fn process(&self, ctx: &Context<MyContext>) -> Result<(), MyError> {
        // Process your job here
        Ok(())
    }
}

// Define your queue
#[derive(Serialize)]
struct MyQueue;

impl Queue for MyQueue {
    fn to_config() -> QueueConfig {
        QueueConfig::as_static("my_queue")
    }
}

// Define your context
struct MyContext {}

// Run your worker
async fn run_worker() -> Result<(), OxanusError> {
    let ctx = Context::value(MyContext {});
    let storage = Storage::builder().from_env()?.build()?;
    let config = Config::new(&storage)
        .register_queue::<MyQueue>()
        .register_worker::<MyWorker>();

    // Enqueue some jobs
    storage.enqueue(MyQueue, MyWorker { data: "hello".into() }).await?;

    // Run the worker
    oxanus::run(config, ctx).await?;
    Ok(())
}
```

For more detailed usage examples, check out the [examples directory](https://github.com/oxanus/oxanus/tree/main/examples).


## Core Concepts

### Workers

Workers are the units of work in Oxanus. They implement the [`Worker`] trait and define the processing logic.

### Queues

Queues are the channels through which jobs flow. They can be:

- Static: Defined at compile time
- Dynamic: Created at runtime with each instance being a separate queue

Each queue can have its own:

- Concurrency limits
- Throttling rules
- Retry policies

### Storage

The [`Storage`] trait provides the interface for job persistence. It handles:

- Job enqueueing
- Job scheduling
- Job state management
- Queue monitoring

### Context

The context provides shared state and utilities to workers. It can include:

- Database connections
- Configuration
- Shared resources

### Configuration

Configuration is done through the [`Config`] builder, which allows you to:

- Register queues and workers
- Set up graceful shutdown

### Error Handling

Oxanus uses a custom error type [`OxanusError`] that covers all possible error cases in the library.
Workers can define their own error type that implements `std::error::Error`.

