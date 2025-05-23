pub trait Queue: Send + Sync {
    fn key(&self) -> String;
    fn to_config() -> QueueConfig;
}

#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub kind: QueueKind,
    pub concurrency: usize,
    pub throttle: Option<QueueThrottle>,
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
pub struct QueueThrottle {
    pub window_ms: u64,
    pub limit: u64,
}
