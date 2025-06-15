use serde::Serialize;

pub trait Queue: Send + Sync + Serialize {
    fn key(&self) -> String {
        match Self::to_config().kind {
            QueueKind::Static { key } => key,
            QueueKind::Dynamic { prefix } => {
                let value = serde_json::to_value(self).unwrap_or_default();
                format!("{}:{}", prefix, value_to_queue_key(value))
            }
        }
    }
    fn to_config() -> QueueConfig;
}

#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub kind: QueueKind,
    pub concurrency: usize,
    pub throttle: Option<QueueThrottle>,
}

impl QueueConfig {
    pub fn as_dynamic(prefix: impl Into<String>) -> Self {
        Self {
            kind: QueueKind::Dynamic {
                prefix: prefix.into(),
            },
            concurrency: 1,
            throttle: None,
        }
    }

    pub fn as_static(key: impl Into<String>) -> Self {
        Self {
            kind: QueueKind::Static { key: key.into() },
            concurrency: 1,
            throttle: None,
        }
    }

    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn throttle(mut self, throttle: QueueThrottle) -> Self {
        self.throttle = Some(throttle);
        self
    }
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

fn value_to_queue_key(value: serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "".to_string(),
        serde_json::Value::String(s) => s,
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Array(a) => a
            .into_iter()
            .map(value_to_queue_key)
            .collect::<Vec<String>>()
            .join(":"),
        serde_json::Value::Object(object) => object
            .into_iter()
            .map(|(k, v)| format!(":{}={}", k, value_to_queue_key(v)))
            .collect::<Vec<String>>()
            .join(":"),
    }
}
