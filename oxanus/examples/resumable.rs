use serde::{Deserialize, Serialize};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Debug, thiserror::Error)]
enum WorkerError {
    #[error("Generic error: {0}")]
    GenericError(String),
    #[error("Job state json error: {0}")]
    JobError(#[from] oxanus::OxanusError),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct WorkerState {}

#[derive(Debug, Serialize, Deserialize)]
struct ResumableTestWorker {}

#[async_trait::async_trait]
impl oxanus::Worker for ResumableTestWorker {
    type Context = WorkerState;
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::Context { state, .. }: &oxanus::Context<WorkerState>,
    ) -> Result<(), WorkerError> {
        let progress = state.get::<i32>().await?;

        dbg!(&progress);

        state.update(progress.unwrap_or(0) + 1).await?;

        if progress.unwrap_or(0) == 10 {
            Ok(())
        } else {
            Err(WorkerError::GenericError("test".to_string()))
        }
    }

    fn max_retries(&self) -> u32 {
        10
    }

    fn retry_delay(&self, _retries: u32) -> u64 {
        3
    }
}

#[derive(Serialize)]
struct QueueOne;

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
        .register_worker::<ResumableTestWorker>()
        .with_graceful_shutdown(tokio::signal::ctrl_c())
        .exit_when_processed(11);

    storage.enqueue(QueueOne, ResumableTestWorker {}).await?;

    oxanus::run(config, ctx).await?;

    Ok(())
}
