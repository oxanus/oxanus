use serde::{Deserialize, Serialize};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Debug, Serialize, Deserialize)]
struct Worker1Sec {
    id: usize,
    payload: String,
}

#[derive(Debug, thiserror::Error)]
enum WorkerError {}

#[derive(Debug, Clone)]
struct WorkerState {}

#[async_trait::async_trait]
impl oxanus::Worker for Worker1Sec {
    type Context = WorkerState;
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::Context { .. }: &oxanus::Context<WorkerState>,
    ) -> Result<(), WorkerError> {
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Worker2Sec {
    id: usize,
    foo: i32,
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
}

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
struct QueueOne;

#[derive(Serialize)]
struct QueueTwo(Animal, i32);

#[derive(Serialize)]
struct QueueThrottled;

#[derive(Debug, Serialize)]
enum Animal {
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
        oxanus::QueueConfig::as_dynamic("two")
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

    let ctx = oxanus::Context::value(WorkerState {});
    let storage = oxanus::Storage::builder().build_from_env()?;
    let config = oxanus::Config::new(&storage.clone())
        .register_queue::<QueueOne>()
        .register_queue::<QueueTwo>()
        .register_queue::<QueueThrottled>()
        .register_worker::<Worker1Sec>()
        .register_worker::<Worker2Sec>()
        .register_worker::<WorkerInstant>()
        .register_worker::<WorkerInstant2>()
        .exit_when_processed(13);

    storage
        .enqueue(
            QueueOne,
            Worker1Sec {
                id: 1,
                payload: "test".to_string(),
            },
        )
        .await?;
    storage
        .enqueue(QueueTwo(Animal::Dog, 1), Worker2Sec { id: 2, foo: 42 })
        .await?;
    storage
        .enqueue(
            QueueOne,
            Worker1Sec {
                id: 3,
                payload: "test".to_string(),
            },
        )
        .await?;
    storage
        .enqueue(QueueTwo(Animal::Cat, 2), Worker2Sec { id: 4, foo: 44 })
        .await?;
    storage
        .enqueue_in(
            QueueOne,
            Worker1Sec {
                id: 4,
                payload: "test".to_string(),
            },
            3,
        )
        .await?;
    storage
        .enqueue_in(QueueTwo(Animal::Bird, 7), Worker2Sec { id: 5, foo: 44 }, 6)
        .await?;
    storage
        .enqueue_in(QueueTwo(Animal::Bird, 7), Worker2Sec { id: 5, foo: 44 }, 15)
        .await?;
    storage.enqueue(QueueThrottled, WorkerInstant {}).await?;
    storage.enqueue(QueueThrottled, WorkerInstant2 {}).await?;
    storage.enqueue(QueueThrottled, WorkerInstant {}).await?;
    storage.enqueue(QueueThrottled, WorkerInstant {}).await?;
    storage.enqueue(QueueThrottled, WorkerInstant2 {}).await?;

    oxanus::run(config, ctx).await?;

    Ok(())
}
