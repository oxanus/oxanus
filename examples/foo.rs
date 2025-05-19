use serde::{Deserialize, Serialize};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Debug, Serialize, Deserialize)]
pub struct Worker1 {
    id: usize,
    payload: String,
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("Generic error: {0}")]
    GenericError(String),
}

#[derive(Debug, Clone)]
pub struct WorkerState {}

#[async_trait::async_trait]
impl oxanus::Worker for Worker1 {
    type Data = WorkerState;
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::WorkerState(_conns): &oxanus::WorkerState<WorkerState>,
    ) -> Result<(), WorkerError> {
        tracing::info!("Job 1 {} started", self.id);
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
        tracing::info!("Job 1 {} done: {}", self.id, self.payload);
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Worker2 {
    id: usize,
    foo: i32,
}

#[async_trait::async_trait]
impl oxanus::Worker for Worker2 {
    type Data = WorkerState;
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::WorkerState(_conns): &oxanus::WorkerState<WorkerState>,
    ) -> Result<(), WorkerError> {
        tracing::info!("Job 2 {} started", self.id);
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
        tracing::info!("Job 2 {} done: {}", self.id, self.foo);
        Ok(())
    }
}

pub struct QueueOne;

pub struct QueueTwo(Animal, i32);

#[derive(Debug)]
pub enum Animal {
    Dog,
    Cat,
    Bird,
}

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
            retry: oxanus::QueueRetry {
                max_retries: 2,
                delay: 3,
                backoff: oxanus::QueueRetryBackoff::None,
            },
        }
    }
}

impl oxanus::Queue for QueueTwo {
    fn key(&self) -> String {
        // probably use Display trait here or specific one
        format!("two:{:?}:{:?}", self.0, self.1)
    }

    fn to_config() -> oxanus::QueueConfig {
        oxanus::QueueConfig {
            kind: oxanus::QueueKind::Dynamic {
                prefix: "two".to_string(),
            },
            concurrency: 1,
            retry: oxanus::QueueRetry {
                max_retries: 2,
                delay: 3,
                backoff: oxanus::QueueRetryBackoff::Exponential { factor: 2.0 },
            },
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
    let redis_manager = redis::aio::ConnectionManager::new(redis_client).await?;
    let data = oxanus::WorkerState::new(WorkerState {});

    let config = oxanus::Config::new()
        .register_queue::<QueueOne>()
        .register_queue::<QueueTwo>()
        .register_worker::<Worker1>()
        .register_worker::<Worker2>();

    oxanus::enqueue(
        &redis_manager,
        QueueOne,
        Worker1 {
            id: 1,
            payload: "test".to_string(),
        },
    )
    .await?;
    oxanus::enqueue(
        &redis_manager,
        QueueTwo(Animal::Dog, 1),
        Worker2 { id: 2, foo: 42 },
    )
    .await?;
    oxanus::enqueue(
        &redis_manager,
        QueueOne,
        Worker1 {
            id: 3,
            payload: "test".to_string(),
        },
    )
    .await?;
    oxanus::enqueue(
        &redis_manager,
        QueueTwo(Animal::Cat, 2),
        Worker2 { id: 4, foo: 44 },
    )
    .await?;
    oxanus::enqueue_in(
        &redis_manager,
        QueueOne,
        Worker1 {
            id: 4,
            payload: "test".to_string(),
        },
        3,
    )
    .await?;
    oxanus::enqueue_in(
        &redis_manager,
        QueueTwo(Animal::Bird, 7),
        Worker2 { id: 5, foo: 44 },
        6,
    )
    .await?;

    let client = redis::Client::open(redis_url).expect("Failed to open Redis client");
    let stats = oxanus::run(&client, config, data).await?;

    println!("Stats: {:?}", stats);

    Ok(())
}
