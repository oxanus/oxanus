use serde::{Deserialize, Serialize};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Debug, Serialize, Deserialize)]
pub struct Worker1Sec {
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
impl oxanus::Worker for Worker1Sec {
    type Data = WorkerState;
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::WorkerState(_conns): &oxanus::WorkerState<WorkerState>,
    ) -> Result<(), WorkerError> {
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Worker2Sec {
    id: usize,
    foo: i32,
}

#[async_trait::async_trait]
impl oxanus::Worker for Worker2Sec {
    type Data = WorkerState;
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::WorkerState(_conns): &oxanus::WorkerState<WorkerState>,
    ) -> Result<(), WorkerError> {
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerInstant {}

#[async_trait::async_trait]
impl oxanus::Worker for WorkerInstant {
    type Data = WorkerState;
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::WorkerState(_conns): &oxanus::WorkerState<WorkerState>,
    ) -> Result<(), WorkerError> {
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerInstant2 {}

#[async_trait::async_trait]
impl oxanus::Worker for WorkerInstant2 {
    type Data = WorkerState;
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::WorkerState(_conns): &oxanus::WorkerState<WorkerState>,
    ) -> Result<(), WorkerError> {
        Ok(())
    }
}

#[derive(Serialize)]
pub struct QueueOne;

#[derive(Serialize)]
pub struct QueueTwo(Animal, i32);

#[derive(Serialize)]
pub struct QueueThrottled;

#[derive(Debug, Serialize)]
pub enum Animal {
    Dog,
    Cat,
    Bird,
}

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

impl oxanus::Queue for QueueTwo {
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

impl oxanus::Queue for QueueThrottled {
    fn to_config() -> oxanus::QueueConfig {
        oxanus::QueueConfig {
            kind: oxanus::QueueKind::Static {
                key: "throttled".to_string(),
            },
            concurrency: 1,
            throttle: Some(oxanus::QueueThrottle {
                limit: 1,
                window_ms: 1500,
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

    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL is not set");
    let redis_client = redis::Client::open(redis_url.clone()).expect("Failed to open Redis client");
    let data = oxanus::WorkerState::new(WorkerState {});

    let storage = oxanus::Storage::new(redis_client);
    let config = oxanus::Config::new(storage)
        .register_queue::<QueueOne>()
        .register_queue::<QueueTwo>()
        .register_queue::<QueueThrottled>()
        .register_worker::<Worker1Sec>()
        .register_worker::<Worker2Sec>()
        .register_worker::<WorkerInstant>()
        .register_worker::<WorkerInstant2>()
        .exit_when_processed(13);

    oxanus::enqueue(
        &config,
        QueueOne,
        Worker1Sec {
            id: 1,
            payload: "test".to_string(),
        },
    )
    .await?;
    oxanus::enqueue(
        &config,
        QueueTwo(Animal::Dog, 1),
        Worker2Sec { id: 2, foo: 42 },
    )
    .await?;
    oxanus::enqueue(
        &config,
        QueueOne,
        Worker1Sec {
            id: 3,
            payload: "test".to_string(),
        },
    )
    .await?;
    oxanus::enqueue(
        &config,
        QueueTwo(Animal::Cat, 2),
        Worker2Sec { id: 4, foo: 44 },
    )
    .await?;
    oxanus::enqueue_in(
        &config,
        QueueOne,
        Worker1Sec {
            id: 4,
            payload: "test".to_string(),
        },
        3,
    )
    .await?;
    oxanus::enqueue_in(
        &config,
        QueueTwo(Animal::Bird, 7),
        Worker2Sec { id: 5, foo: 44 },
        6,
    )
    .await?;
    oxanus::enqueue_in(
        &config,
        QueueTwo(Animal::Bird, 7),
        Worker2Sec { id: 5, foo: 44 },
        15,
    )
    .await?;
    oxanus::enqueue(&config, QueueThrottled, WorkerInstant {}).await?;
    oxanus::enqueue(&config, QueueThrottled, WorkerInstant2 {}).await?;
    oxanus::enqueue(&config, QueueThrottled, WorkerInstant {}).await?;
    oxanus::enqueue(&config, QueueThrottled, WorkerInstant2 {}).await?;
    oxanus::enqueue(&config, QueueThrottled, WorkerInstant {}).await?;
    oxanus::enqueue(&config, QueueThrottled, WorkerInstant2 {}).await?;

    oxanus::run(config, data).await?;

    Ok(())
}
