use serde::{Deserialize, Serialize};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("Generic error: {0}")]
    GenericError(String),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkerState {}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TestWorker {}

#[async_trait::async_trait]
impl oxanus::Worker for TestWorker {
    type Context = WorkerState;
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::WorkerContext { .. }: &oxanus::WorkerContext<WorkerState>,
    ) -> Result<(), WorkerError> {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Ok(())
    }
}

#[derive(Serialize)]
pub struct QueueOne;

impl oxanus::Queue for QueueOne {
    fn to_config() -> oxanus::QueueConfig {
        oxanus::QueueConfig::key("one")
    }
}

#[tokio::main]
pub async fn main() -> Result<(), oxanus::OxanusError> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let ctx = oxanus::WorkerContextValue::new(WorkerState {});
    let storage = oxanus::Storage::from_env()?;
    let config = oxanus::Config::new(storage.clone())
        .register_queue::<QueueOne>()
        .register_cron_worker::<TestWorker>("*/5 * * * * *", QueueOne)
        .with_graceful_shutdown(tokio::signal::ctrl_c());

    oxanus::run(config, ctx).await?;

    Ok(())
}
