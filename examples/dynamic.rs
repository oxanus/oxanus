use serde::{Deserialize, Serialize};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Debug, thiserror::Error)]
enum WorkerError {}

#[derive(Debug, Clone)]
struct WorkerState {}

#[derive(Debug, Serialize, Deserialize)]
struct Worker2Sec {}

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
}

#[derive(Serialize)]
struct QueueDynamic(Animal, i32);

#[derive(Debug, Serialize)]
enum Animal {
    Dog,
    Cat,
    Bird,
}

impl oxanus::Queue for QueueDynamic {
    fn to_config() -> oxanus::QueueConfig {
        oxanus::QueueConfig {
            kind: oxanus::QueueKind::Dynamic {
                prefix: "two".to_string(),
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
    let config = oxanus::Config::new(&storage.clone())
        .register_queue::<QueueDynamic>()
        .register_worker::<Worker2Sec>()
        .exit_when_processed(5);

    storage
        .enqueue(QueueDynamic(Animal::Cat, 2), Worker2Sec {})
        .await?;
    storage
        .enqueue(QueueDynamic(Animal::Dog, 1), Worker2Sec {})
        .await?;
    storage
        .enqueue(QueueDynamic(Animal::Cat, 2), Worker2Sec {})
        .await?;
    storage
        .enqueue(QueueDynamic(Animal::Bird, 1), Worker2Sec {})
        .await?;
    storage
        .enqueue(QueueDynamic(Animal::Dog, 1), Worker2Sec {})
        .await?;

    oxanus::run(config, ctx).await?;

    Ok(())
}
