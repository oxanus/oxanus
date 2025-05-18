pub trait Queue: Send + Sync {
    fn key(&self) -> String;
    fn to_config() -> QueueConfig;
}

#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub kind: QueueKind,
    pub concurrency: usize,
    pub retry: QueueRetry,
}

#[derive(Debug, Clone)]
pub enum QueueKind {
    Static { key: String },
    Dynamic { prefix: String },
}

#[derive(Debug, Clone)]
pub struct QueueRetry {
    pub max_retries: usize,
    pub delay: u64,
    pub backoff: QueueRetryBackoff,
}

#[derive(Debug, Clone)]
pub enum QueueRetryBackoff {
    None,
    Exponential { factor: f64 },
}
