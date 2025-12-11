use serde::{Deserialize, Serialize};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Debug, thiserror::Error)]
enum WorkerError {}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct WorkerState {}

#[derive(Debug, Serialize, Deserialize)]
struct TestWorker {
    sleep_s: u64,
}

#[async_trait::async_trait]
impl oxanus::Worker for TestWorker {
    type Context = WorkerState;
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::Context { .. }: &oxanus::Context<WorkerState>,
    ) -> Result<(), WorkerError> {
        tokio::time::sleep(std::time::Duration::from_secs(self.sleep_s)).await;
        Ok(())
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
            concurrency: 2,
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
    let storage = oxanus::Storage::builder().build_from_env()?;
    let config = oxanus::Config::new(&storage)
        .register_queue::<QueueOne>()
        .register_worker::<TestWorker>()
        .with_graceful_shutdown(tokio::signal::ctrl_c())
        .exit_when_processed(1);

    storage
        .enqueue(QueueOne, TestWorker { sleep_s: 10 })
        .await?;
    storage.enqueue(QueueOne, TestWorker { sleep_s: 5 }).await?;

    oxanus::run(config, ctx).await?;

    Ok(())
}
