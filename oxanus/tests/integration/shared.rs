use deadpool_redis::redis::AsyncCommands;
use rand::distr::{Alphanumeric, SampleString};
use serde::{Deserialize, Serialize};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("Generic error: {0}")]
    Generic(String),
    #[error("Redis error: {0}")]
    Redis(#[from] deadpool_redis::redis::RedisError),
    #[error("Redis error: {0}")]
    Pool(#[from] deadpool_redis::PoolError),
}

#[derive(Clone)]
pub struct WorkerState {
    pub redis: deadpool_redis::Pool,
}

#[derive(Serialize, Deserialize)]
pub struct WorkerNoop {}

#[async_trait::async_trait]
impl oxanus::Worker for WorkerNoop {
    type Context = ();
    type Error = WorkerError;

    async fn process(&self, _: &oxanus::Context<()>) -> Result<(), WorkerError> {
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerRedisSet {
    pub key: String,
    pub value: String,
}

#[async_trait::async_trait]
impl oxanus::Worker for WorkerRedisSet {
    type Context = WorkerState;
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::Context { ctx, .. }: &oxanus::Context<WorkerState>,
    ) -> Result<(), WorkerError> {
        let mut redis = ctx.redis.get().await?;
        let _: () = redis.set_ex(&self.key, self.value.clone(), 3).await?;
        Ok(())
    }
}

#[derive(Serialize)]
pub struct QueueOne;

impl oxanus::Queue for QueueOne {
    fn to_config() -> oxanus::QueueConfig {
        oxanus::QueueConfig::as_static("one")
    }
}

pub fn setup() -> deadpool_redis::Pool {
    dotenvy::from_filename(".env.test").ok();

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .try_init()
        .ok();

    redis_pool()
}

pub fn redis_pool() -> deadpool_redis::Pool {
    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL is not set");
    let mut cfg = deadpool_redis::Config::from_url(redis_url);
    cfg.pool = Some(deadpool_redis::PoolConfig {
        max_size: 10,
        timeouts: deadpool_redis::Timeouts {
            wait: Some(std::time::Duration::from_millis(50)),
            create: Some(std::time::Duration::from_millis(50)),
            recycle: Some(std::time::Duration::from_millis(50)),
        },
        ..Default::default()
    });
    cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .expect("Failed to create Redis pool")
}

pub fn random_string() -> String {
    Alphanumeric.sample_string(&mut rand::rng(), 16)
}
