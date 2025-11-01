use serde::{Deserialize, Serialize};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Debug, thiserror::Error)]
enum WorkerError {}

#[derive(Debug, Clone)]
struct WorkerState {}

#[derive(Debug, Serialize, Deserialize)]
struct WorkerInstant {}

#[async_trait::async_trait]
impl oxanus::Worker for WorkerInstant {
    type Context = WorkerState;
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::Context { .. }: &oxanus::Context<WorkerState>,
    ) -> Result<(), WorkerError> {
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct WorkerInstant2 {}

#[async_trait::async_trait]
impl oxanus::Worker for WorkerInstant2 {
    type Context = WorkerState;
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::Context { .. }: &oxanus::Context<WorkerState>,
    ) -> Result<(), WorkerError> {
        Ok(())
    }
}

#[derive(Serialize)]
struct QueueThrottled;

impl oxanus::Queue for QueueThrottled {
    fn to_config() -> oxanus::QueueConfig {
        oxanus::QueueConfig {
            kind: oxanus::QueueKind::Static {
                key: "throttled".to_string(),
            },
            concurrency: 1,
            throttle: Some(oxanus::QueueThrottle {
                limit: 2,
                window_ms: 2000,
            }),
        }
    }
}
#[tokio::main]
pub async fn main() -> Result<(), oxanus::OxanusError> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let ctx = oxanus::Context::value(WorkerState {});
    let storage = oxanus::Storage::builder().from_env()?.build()?;
    let config = oxanus::Config::new(&storage.clone())
        .register_queue::<QueueThrottled>()
        .register_worker::<WorkerInstant>()
        .register_worker::<WorkerInstant2>()
        .exit_when_processed(8);

    storage.enqueue(QueueThrottled, WorkerInstant {}).await?;
    storage.enqueue(QueueThrottled, WorkerInstant2 {}).await?;
    storage.enqueue(QueueThrottled, WorkerInstant {}).await?;
    storage.enqueue(QueueThrottled, WorkerInstant {}).await?;
    storage.enqueue(QueueThrottled, WorkerInstant2 {}).await?;
    storage.enqueue(QueueThrottled, WorkerInstant {}).await?;
    storage.enqueue(QueueThrottled, WorkerInstant {}).await?;
    storage.enqueue(QueueThrottled, WorkerInstant {}).await?;

    oxanus::run(config, ctx).await?;

    Ok(())
}
