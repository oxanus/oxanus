# Oxanus [![Latest Version](https://img.shields.io/crates/v/oxanus.svg)](https://crates.io/crates/oxanus) [![Build Status](https://img.shields.io/github/actions/workflow/status/oxanus/oxanus/test.yml?branch=main)](https://github.com/oxanus/oxanus/actions)

<p align="center">
  <picture>
    <img alt="Oxanus logo" src="logo.jpg" width="320">
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

## Core Concepts

### Workers

Workers are the units of work in Oxanus. They implement the `Worker` trait and define the processing logic.

### Queues

Queues are the channels through which jobs flow. They can be:

- Static: Defined at compile time
- Dynamic: Created at runtime with each instance being a separate queue

Each queue can have its own:

- Concurrency limits
- Throttling rules
- Retry policies

### Storage

The `Storage` trait provides the interface for job persistence. It handles:

- Job enqueueing
- Job scheduling
- Job state management
- Queue monitoring

### Context

The `Context` provides shared state and utilities to workers. It can include:

- Database connections
- Configuration
- Shared resources

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
        QueueConfig {
            kind: QueueKind::Static {
                key: "my_queue".to_string(),
            },
            concurrency: 2,
            throttle: None,
        }
    }
}

// Run your worker
async fn run_worker() -> Result<(), OxanusError> {
    let ctx = Context::value(MyContext {});
    let storage = Storage::builder().from_env()?.build()?;
    let config = Config::new(&storage)
        .register_queue::<MyQueue>()
        .register_worker::<MyWorker>()
        .with_graceful_shutdown(tokio::signal::ctrl_c());

    // Enqueue some jobs
    storage.enqueue(MyQueue, MyWorker { data: "hello".into() }).await?;

    // Run the worker
    oxanus::run(config, ctx).await?;
    Ok(())
}
```

For more detailed usage examples, check out the [examples directory](https://github.com/oxanus/oxanus/tree/main/examples).
