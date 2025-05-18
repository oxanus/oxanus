#[derive(Debug, Clone)]
pub struct QueueStatic {
    pub name: String,
}

pub trait QueueConfigTrait: Send + Sync {
    fn key(&self) -> String;
    fn to_config() -> QueueConfig;
}

#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub kind: QueueConfigKind,
    pub concurrency: usize,
    pub retry: QueueConfigRetry,
}

#[derive(Debug, Clone)]
pub enum QueueConfigKind {
    Static { key: String },
    Dynamic { prefix: String },
}

#[derive(Debug, Clone)]
pub struct QueueConfigRetry {
    pub max_retries: usize,
    pub delay: u64,
    pub backoff: QueueConfigRetryBackoff,
}

#[derive(Debug, Clone)]
pub enum QueueConfigRetryBackoff {
    None,
    Exponential { factor: f64 },
}

#[derive(Debug, Clone)]
pub struct QueueDynamic {
    pub prefix: String,
}

impl QueueStatic {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}
