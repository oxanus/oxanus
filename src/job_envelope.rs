use serde::{Deserialize, Serialize};
use std::any::type_name;
use uuid::Uuid;

use crate::{OxanusError, Worker};

#[derive(Debug, Deserialize, Serialize)]
pub struct JobEnvelope {
    pub id: String,
    pub job: Job,
    pub meta: JobEnvelopeMeta,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Job {
    pub name: String,
    pub queue: String,
    pub args: serde_json::Value,
    pub created_at: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct JobEnvelopeMeta {
    pub retries: u32,
    pub unique: bool,
}

impl JobEnvelope {
    pub fn new<T, DT, ET>(queue: String, job: T) -> Result<Self, OxanusError>
    where
        T: Worker<Data = DT, Error = ET> + serde::Serialize,
        DT: Send + Sync + Clone + 'static,
        ET: std::error::Error + Send + Sync + 'static,
    {
        let unique_id = job.unique_id();
        let unique = unique_id.is_some();
        let id = unique_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        Ok(Self {
            id,
            job: Job {
                name: type_name::<T>().to_string(),
                queue,
                args: serde_json::to_value(&job)?,
                created_at: u64::try_from(chrono::Utc::now().timestamp_micros())?,
            },
            meta: JobEnvelopeMeta { retries: 0, unique },
        })
    }

    pub fn with_retries_incremented(self) -> Self {
        Self {
            id: self.id,
            job: self.job,
            meta: JobEnvelopeMeta {
                retries: self.meta.retries + 1,
                unique: self.meta.unique,
            },
        }
    }
}
