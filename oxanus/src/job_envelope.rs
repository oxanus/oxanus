use serde::{Deserialize, Serialize};
use std::any::type_name;
use uuid::Uuid;

use crate::{OxanusError, Worker};

pub type JobId = String;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JobEnvelope {
    pub id: JobId,
    pub job: Job,
    pub queue: String,
    pub meta: JobMeta,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Job {
    pub name: String,
    pub args: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct JobMeta {
    pub id: JobId,
    pub retries: u32,
    pub unique: bool,
    pub created_at: i64,
    #[serde(default)]
    pub scheduled_at: i64,
    pub state: Option<serde_json::Value>,
}

impl JobEnvelope {
    pub fn new<T, DT, ET>(queue: String, job: T) -> Result<Self, OxanusError>
    where
        T: Worker<Context = DT, Error = ET> + serde::Serialize,
        DT: Send + Sync + Clone + 'static,
        ET: std::error::Error + Send + Sync + 'static,
    {
        let unique_id = job.unique_id();
        let unique = unique_id.is_some();
        let id = unique_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        Ok(Self {
            id: id.clone(),
            queue,
            job: Job {
                name: type_name::<T>().to_string(),
                args: serde_json::to_value(&job)?,
            },
            meta: JobMeta {
                id,
                retries: 0,
                unique,
                created_at: chrono::Utc::now().timestamp_micros(),
                scheduled_at: chrono::Utc::now().timestamp_micros(),
                state: None,
            },
        })
    }

    pub fn new_cron(
        queue: String,
        id: String,
        name: String,
        scheduled_at: i64,
    ) -> Result<Self, OxanusError> {
        Ok(Self {
            id: id.clone(),
            queue,
            job: Job {
                name,
                args: serde_json::to_value(serde_json::json!({}))?,
            },
            meta: JobMeta {
                id,
                retries: 0,
                unique: true,
                created_at: chrono::Utc::now().timestamp_micros(),
                scheduled_at,
                state: None,
            },
        })
    }

    pub fn with_retries_incremented(self) -> Self {
        Self {
            id: self.id.clone(),
            queue: self.queue,
            job: self.job,
            meta: JobMeta {
                id: self.id,
                retries: self.meta.retries + 1,
                unique: self.meta.unique,
                created_at: self.meta.created_at,
                scheduled_at: self.meta.scheduled_at,
                state: self.meta.state,
            },
        }
    }
}

impl JobMeta {
    pub fn created_at_secs(&self) -> i64 {
        self.created_at / 1000000
    }

    pub fn created_at_millis(&self) -> i64 {
        self.created_at / 1000
    }

    pub fn scheduled_at_millis(&self) -> i64 {
        self.scheduled_at / 1000
    }

    pub fn scheduled_at_secs(&self) -> i64 {
        self.scheduled_at / 1000000
    }

    fn latency_micros(&self) -> i64 {
        (chrono::Utc::now().timestamp_micros() - self.scheduled_at).max(0)
    }

    pub fn latency_secs(&self) -> i64 {
        self.latency_micros() / 1000000
    }

    pub fn latency_millis(&self) -> i64 {
        self.latency_micros() / 1000
    }
}
