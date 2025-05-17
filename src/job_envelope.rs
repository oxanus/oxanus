use std::any::type_name;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{OxanusError, Worker};

#[derive(Debug, Deserialize, Serialize)]
pub struct JobEnvelope {
    #[serde(default = "uuid::Uuid::new_v4")]
    pub uuid: Uuid,
    pub queue: String,
    pub job: String,
    pub args: serde_json::Value,
    #[serde(default)]
    pub meta: JobEnvelopeMeta,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct JobEnvelopeMeta {
    pub retries: u32,
}

impl JobEnvelope {
    pub fn new<T, DT, ET>(queue: String, job: T) -> Result<Self, OxanusError>
    where
        T: Worker<Data = DT, Error = ET> + serde::Serialize,
        DT: Send + Sync + Clone + 'static,
        ET: std::error::Error + Send + Sync + 'static,
    {
        Ok(Self {
            uuid: Uuid::new_v4(),
            queue,
            job: type_name::<T>().to_string(),
            args: serde_json::to_value(&job)?,
            meta: JobEnvelopeMeta::default(),
        })
    }

    pub fn with_retries_incremented(self) -> Self {
        Self {
            uuid: self.uuid,
            queue: self.queue,
            job: self.job,
            args: self.args,
            meta: JobEnvelopeMeta {
                retries: self.meta.retries + 1,
            },
        }
    }
}
