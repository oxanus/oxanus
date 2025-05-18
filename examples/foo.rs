use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Worker1 {
    id: usize,
    payload: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("Generic error: {0}")]
    GenericError(String),
}

#[derive(Debug, Clone)]
pub struct Connections {
    pub db: sqlx::postgres::PgPool,
}

#[async_trait::async_trait]
impl oxanus::Worker for Worker1 {
    type Data = Connections;
    type Error = ServiceError;

    async fn process(
        &self,
        oxanus::WorkerState(_conns): &oxanus::WorkerState<Connections>,
    ) -> Result<(), ServiceError> {
        tracing::info!("Job 1 {} started", self.id);
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        tracing::info!("Job 1 {} done: {}", self.id, self.payload);
        // Err(ServiceError::server_error("test"))
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
    type Data = Connections;
    type Error = ServiceError;

    async fn process(
        &self,
        oxanus::WorkerState(_conns): &oxanus::WorkerState<Connections>,
    ) -> Result<(), ServiceError> {
        println!("Job 2 {} started", self.id);
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
        println!("Job 2 {} done: {}", self.id, self.foo);
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

impl oxanus::QueueConfigTrait for QueueOne {
    fn key(&self) -> String {
        "one".to_string()
    }

    fn to_config() -> oxanus::QueueConfig {
        oxanus::QueueConfig {
            kind: oxanus::QueueConfigKind::Static {
                key: "one".to_string(),
            },
            concurrency: 1,
            retry: oxanus::QueueConfigRetry {
                max_retries: 2,
                delay: 3,
                backoff: oxanus::QueueConfigRetryBackoff::None,
            },
        }
    }
}

impl oxanus::QueueConfigTrait for QueueTwo {
    fn key(&self) -> String {
        // probably use Display trait here or specific one
        format!("two:{:?}:{:?}", self.0, self.1)
    }

    fn to_config() -> oxanus::QueueConfig {
        oxanus::QueueConfig {
            kind: oxanus::QueueConfigKind::Dynamic {
                prefix: "two".to_string(),
            },
            concurrency: 1,
            retry: oxanus::QueueConfigRetry {
                max_retries: 2,
                delay: 3,
                backoff: oxanus::QueueConfigRetryBackoff::Exponential { factor: 2.0 },
            },
        }
    }
}

#[tokio::main]
pub async fn main() -> Result<(), oxanus::OxanusError> {
    let url =
        std::env::var("PG_URL").unwrap_or_else(|_e| "postgresql://localhost/oxanus".to_string());
    let pool = sqlx::postgres::PgPool::connect(&url).await?;
    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL is not set");
    let client = redis::Client::open(redis_url.clone()).expect("Failed to open Redis client");
    let redis = redis::aio::ConnectionManager::new(client).await?;
    let data = oxanus::WorkerState::new(Connections { db: pool.clone() });

    let config = oxanus::Config::new()
        .register_queue::<QueueOne>()
        .register_queue::<QueueTwo>()
        .register_worker::<Worker1>()
        .register_worker::<Worker2>();
    // .exit_when_idle()
    // .exit_when_finished(2);

    oxanus::enqueue(
        &redis,
        QueueOne,
        Worker1 {
            id: 1,
            payload: "test".to_string(),
        },
    )
    .await?;
    oxanus::enqueue(&redis, QueueTwo(Animal::Dog, 1), Worker2 { id: 2, foo: 42 }).await?;
    oxanus::enqueue(
        &redis,
        QueueOne,
        Worker1 {
            id: 3,
            payload: "test".to_string(),
        },
    )
    .await?;
    oxanus::enqueue(&redis, QueueTwo(Animal::Cat, 2), Worker2 { id: 4, foo: 44 }).await?;

    let client = redis::Client::open(redis_url).expect("Failed to open Redis client");
    let stats = oxanus::run(&client, config, data).await?;

    println!("Stats: {:?}", stats);

    Ok(())
}
