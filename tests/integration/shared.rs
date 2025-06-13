use rand::distr::{Alphanumeric, SampleString};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("Generic error: {0}")]
    GenericError(String),
    #[error("Redis error: {0}")]
    RedisError(#[from] redis::RedisError),
}

#[derive(Clone)]
pub struct WorkerState {
    pub redis: redis::aio::ConnectionManager,
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
        oxanus::WorkerContext { ctx, .. }: &oxanus::WorkerContext<WorkerState>,
    ) -> Result<(), WorkerError> {
        let mut redis = ctx.redis.clone();
        let _: () = redis.set_ex(&self.key, self.value.clone(), 3).await?;
        Ok(())
    }
}

#[derive(Serialize)]
pub struct QueueOne;

impl oxanus::Queue for QueueOne {
    fn to_config() -> oxanus::QueueConfig {
        oxanus::QueueConfig::key("one")
    }
}

pub fn setup() -> redis::Client {
    dotenvy::from_filename(".env.test").ok();

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .try_init()
        .ok();

    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL is not set");
    redis::Client::open(redis_url.clone()).expect("Failed to open Redis client")
}

pub fn random_string() -> String {
    Alphanumeric.sample_string(&mut rand::rng(), 16)
}
