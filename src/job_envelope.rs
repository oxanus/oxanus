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
    pub created_at: u64,
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
                created_at: u64::try_from(chrono::Utc::now().timestamp_micros())?,
                state: None,
            },
        })
    }

    pub fn new_cron(queue: String, id: String, name: String) -> Result<Self, OxanusError> {
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
                created_at: u64::try_from(chrono::Utc::now().timestamp_micros())?,
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
                state: self.meta.state,
            },
        }
    }
}
