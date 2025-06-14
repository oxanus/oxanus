use serde::{Deserialize, Serialize};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("Generic error: {0}")]
    GenericError(String),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkerState {}

#[derive(Debug, Serialize, Deserialize)]
pub struct Worker2Sec {
    id: usize,
}

#[async_trait::async_trait]
impl oxanus::Worker for Worker2Sec {
    type Context = WorkerState;
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::Context { .. }: &oxanus::Context<WorkerState>,
    ) -> Result<(), WorkerError> {
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
        Ok(())
    }

    fn unique_id(&self) -> Option<String> {
        Some(format!("worker2sec:{}", self.id))
    }
}

#[derive(Serialize)]
pub struct QueueOne;

impl oxanus::Queue for QueueOne {
    fn to_config() -> oxanus::QueueConfig {
        oxanus::QueueConfig {
            kind: oxanus::QueueKind::Static {
                key: "one".to_string(),
            },
            concurrency: 1,
            throttle: None,
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
    let config = oxanus::Config::new(&storage)
        .register_queue::<QueueOne>()
        .register_worker::<Worker2Sec>();

    storage.enqueue(QueueOne, Worker2Sec { id: 1 }).await?;
    storage.enqueue(QueueOne, Worker2Sec { id: 1 }).await?;
    storage.enqueue(QueueOne, Worker2Sec { id: 2 }).await?;

    oxanus::run(config, ctx).await?;

    Ok(())
}
