use rand::Rng;
use serde::{Deserialize, Serialize};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Debug, thiserror::Error)]
enum WorkerError {
    #[error("Generic error: {0}")]
    GenericError(String),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct WorkerState {}

#[derive(Debug, Serialize, Deserialize, Default)]
struct TestWorker {}

#[async_trait::async_trait]
impl oxanus::Worker for TestWorker {
    type Context = WorkerState;
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::Context { .. }: &oxanus::Context<WorkerState>,
    ) -> Result<(), WorkerError> {
        if rand::rng().random_bool(0.5) {
            Err(WorkerError::GenericError("foo".to_string()))
        } else {
            Ok(())
        }
    }

    fn max_retries(&self) -> u32 {
        3
    }

    fn retry_delay(&self, _retries: u32) -> u64 {
        0
    }
}

#[derive(Serialize)]
struct QueueOne;

impl oxanus::Queue for QueueOne {
    fn to_config() -> oxanus::QueueConfig {
        oxanus::QueueConfig::as_static("one")
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
        .register_cron_worker::<TestWorker>("*/10 * * * * *", QueueOne)
        .with_graceful_shutdown(tokio::signal::ctrl_c());

    oxanus::run(config, ctx).await?;

    Ok(())
}
