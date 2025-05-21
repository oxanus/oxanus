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
pub struct Worker {}

#[async_trait::async_trait]
impl oxanus::Worker for Worker {
    type Data = WorkerState;
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::WorkerState(_conns): &oxanus::WorkerState<WorkerState>,
    ) -> Result<(), WorkerError> {
        tokio::time::sleep(std::time::Duration::from_millis(20_000)).await;
        Ok(())
    }
}

pub struct QueueOne;

impl oxanus::Queue for QueueOne {
    fn key(&self) -> String {
        "one".to_string()
    }

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

    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL is not set");
    let redis_client = redis::Client::open(redis_url.clone()).expect("Failed to open Redis client");
    // let redis_manager = redis::aio::ConnectionManager::new(redis_client).await?;
    let data = oxanus::WorkerState::new(WorkerState {});

    let config = oxanus::Config::new()
        .register_queue::<QueueOne>()
        .register_worker::<Worker>();

    oxanus::run(&redis_client, config, data).await?;

    Ok(())
}
