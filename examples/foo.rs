use serde::{Deserialize, Serialize};

#[derive(Default)]
pub struct QueueOne {}

impl oxanus::Queue for QueueOne {
    fn name(&self) -> &'static str {
        "one"
    }

    fn concurrency(&self) -> usize {
        1
    }
}

#[derive(Default)]
pub struct QueueTwo {}

impl oxanus::Queue for QueueTwo {
    fn name(&self) -> &'static str {
        "two"
    }

    fn concurrency(&self) -> usize {
        1
    }
}

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

    fn queue(&self) -> Box<dyn oxanus::Queue> {
        Box::new(QueueOne::default())
    }

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

    fn queue(&self) -> Box<dyn oxanus::Queue> {
        Box::new(QueueTwo::default())
    }

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
    let url = "postgresql://localhost/oxanus";
    let pool = sqlx::postgres::PgPool::connect(&url).await?;
    let data = oxanus::WorkerState::new(Connections { db: pool.clone() });

    let config = oxanus::Config::new()
        .register_queue(Box::new(QueueOne::default()))
        .register_queue(Box::new(QueueTwo::default()))
        .register_worker::<Worker1>()
        .register_worker::<Worker2>()
        .exit_when_done();

    oxanus::setup(&pool, &config).await?;
    oxanus::enqueue(
        &pool,
        Worker1 {
            id: 1,
            payload: "test".to_string(),
        },
    )
    .await?;
    oxanus::enqueue(&pool, Worker2 { id: 2, foo: 42 }).await?;
    oxanus::enqueue(
        &pool,
        Worker1 {
            id: 3,
            payload: "test".to_string(),
        },
    )
    .await?;
    oxanus::enqueue(&pool, Worker2 { id: 4, foo: 44 }).await?;
    oxanus::run(&pool, config, data).await?;

    Ok(())
}
