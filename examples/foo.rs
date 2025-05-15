use oxanus::Queue;
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
        tracing::info!("Job 2 {} started", self.id);
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
        tracing::info!("Job 2 {} done: {}", self.id, self.foo);
        Ok(())
    }
}

#[tokio::main]
pub async fn main() -> Result<(), oxanus::OxanusError> {
    let url =
        std::env::var("PG_URL").unwrap_or_else(|_e| "postgresql://localhost/oxanus".to_string());
    let pool = sqlx::postgres::PgPool::connect(&url).await?;
    let data = oxanus::WorkerState::new(Connections { db: pool.clone() });

    let queue_one = Queue::new("one", 1);
    let queue_two = Queue::new("two", 1);

    let config = oxanus::Config::new()
        .register_queue(queue_one)
        .register_queue(queue_two)
        .register_worker::<Worker1>()
        .register_worker::<Worker2>()
        .exit_when_idle();

    oxanus::setup(&pool, &config).await?;
    oxanus::enqueue(
        &pool,
        &queue_one,
        Worker1 {
            id: 1,
            payload: "test".to_string(),
        },
    )
    .await?;
    oxanus::enqueue(&pool, &queue_two, Worker2 { id: 2, foo: 42 }).await?;
    oxanus::enqueue(
        &pool,
        &queue_one,
        Worker1 {
            id: 3,
            payload: "test".to_string(),
        },
    )
    .await?;
    oxanus::enqueue(&pool, &&queue_two, Worker2 { id: 4, foo: 44 }).await?;
    let stats = oxanus::run(&pool, config, data).await?;

    println!("Stats: {:?}", stats);

    Ok(())
}
