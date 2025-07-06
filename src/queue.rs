use serde::Serialize;
use std::{
    hash::{Hash, Hasher},
    time::Duration,
};

pub trait Queue: Send + Sync + Serialize {
    fn key(&self) -> String {
        match Self::to_config().kind {
            QueueKind::Static { key } => key,
            QueueKind::Dynamic { prefix, .. } => {
                let value = serde_json::to_value(self).unwrap_or_default();
                format!("{}#{}", prefix, value_to_queue_key(value))
            }
        }
    }
    fn to_config() -> QueueConfig;
    fn config(&self) -> QueueConfig {
        Self::to_config()
    }
}

#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub kind: QueueKind,
    pub concurrency: usize,
    pub throttle: Option<QueueThrottle>,
}

impl PartialEq for QueueConfig {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

impl Eq for QueueConfig {}

impl Hash for QueueConfig {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
    }
}

impl QueueConfig {
    pub fn as_dynamic(prefix: impl Into<String>) -> Self {
        Self {
            kind: QueueKind::Dynamic {
                prefix: prefix.into(),
                sleep_period: Duration::from_millis(500),
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
    Static {
        key: String,
    },
    Dynamic {
        prefix: String,
        sleep_period: Duration,
    },
}

impl PartialEq for QueueKind {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (QueueKind::Static { key: k1 }, QueueKind::Static { key: k2 }) => k1 == k2,
            (QueueKind::Dynamic { prefix: p1, .. }, QueueKind::Dynamic { prefix: p2, .. }) => {
                p1 == p2
            }
            _ => false,
        }
    }
}

impl Eq for QueueKind {}

impl Hash for QueueKind {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            QueueKind::Static { key } => key.hash(state),
            QueueKind::Dynamic { prefix, .. } => prefix.hash(state),
        }
    }
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
            .map(|(k, v)| format!("{}={}", k, value_to_queue_key(v)))
            .collect::<Vec<String>>()
            .join(":"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct TestStaticQueue;

    impl Queue for TestStaticQueue {
        fn to_config() -> QueueConfig {
            QueueConfig::as_static("test_static_queue")
        }
    }

    #[derive(Serialize)]
    struct TestDynamicQueue {
        name: String,
        age: u32,
        is_student: bool,
    }

    impl Queue for TestDynamicQueue {
        fn to_config() -> QueueConfig {
            QueueConfig::as_dynamic("test_dynamic_queue")
        }
    }

    #[test]
    fn test_queue_key() {
        let static_queue = TestStaticQueue;
        let dynamic_queue = TestDynamicQueue {
            name: "John".to_string(),
            age: 30,
            is_student: true,
        };

        assert_eq!(static_queue.key(), "test_static_queue");
        assert_eq!(
            dynamic_queue.key(),
            "test_dynamic_queue#name=John:age=30:is_student=true"
        );
    }
}
