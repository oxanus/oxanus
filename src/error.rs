#[derive(Debug, thiserror::Error)]
pub enum OxanusError {
    #[error("Generic error: {0}")]
    GenericError(String),
    #[error("Json error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Semaphore error: {0}")]
    SemaphoreError(#[from] tokio::sync::AcquireError),
    #[error("Job factory error: {0}")]
    JobFactoryError(String),
    #[error("Worker error: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),
    #[error("Try from int error: {0}")]
    TryFromIntError(#[from] std::num::TryFromIntError),
    #[error("Std IO error: {0}")]
    StdIoError(#[from] std::io::Error),
    #[error("Deadpool Redis error: {0}")]
    DeadpoolRedisError(#[from] deadpool_redis::redis::RedisError),
    #[error("Deadpool Redis pool error: {0}")]
    DeadpoolRedisPoolError(#[from] deadpool_redis::PoolError),
    #[error("Deadpool Redis create pool error: {0}")]
    DeadpoolRedisCreatePoolError(#[from] deadpool_redis::CreatePoolError),
    #[error("Redis not configured")]
    ConfigRedisNotConfigured,
    #[error("Job panicked: {0}")]
    JobPanicked(String),
}
