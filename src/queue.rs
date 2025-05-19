pub trait Queue: Send + Sync {
    fn key(&self) -> String;
    fn to_config() -> QueueConfig;
}

#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub kind: QueueKind,
    pub concurrency: usize,
    pub throttle: QueueThrottle,
    // pub retry: QueueRetry,
}

#[derive(Debug, Clone)]
pub enum QueueKind {
    Static { key: String },
    Dynamic { prefix: String },
}

impl QueueKind {
    pub fn is_dynamic(&self) -> bool {
        matches!(self, QueueKind::Dynamic { .. })
    }

    pub fn is_static(&self) -> bool {
        matches!(self, QueueKind::Static { .. })
    }
}

#[derive(Debug, Clone)]
pub struct QueueRetry {
    pub max_retries: usize,
    pub delay: u64,
    pub backoff: QueueRetryBackoff,
}

#[derive(Debug, Clone)]
pub enum QueueThrottle {
    None,
    SlidingWindow { window: u64, limit: u64 },
}

#[derive(Debug, Clone)]
pub enum QueueRetryBackoff {
    None,
    Exponential { factor: f64 },
}
