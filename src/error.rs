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
    #[error("Sqlx error: {0}")]
    SqlxError(#[from] sqlx::Error),
    #[error("Redis error: {0}")]
    RedisError(#[from] redis::RedisError),
}
