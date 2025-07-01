use serde::{Serialize, de::DeserializeOwned};

use crate::{JobId, OxanusError, Storage, job_envelope::JobMeta};

#[derive(Clone)]
pub struct Context<T: Clone + Send + Sync> {
    pub ctx: T,
    pub meta: JobMeta,
    pub state: JobState,
}

impl<T: Clone + Send + Sync> Context<T> {
    pub fn value(v: T) -> ContextValue<T> {
        ContextValue(v)
    }
}

#[derive(Debug, Clone)]
pub struct ContextValue<T: Clone + Send + Sync>(pub(crate) T);

#[derive(Clone)]
pub struct JobState {
    storage: Storage,
    job_id: JobId,
    value: Option<serde_json::Value>,
}

impl JobState {
    pub(crate) fn new(storage: Storage, job_id: JobId, value: Option<serde_json::Value>) -> Self {
        Self {
            storage,
            job_id,
            value,
        }
    }

    pub async fn update(&self, state: impl Serialize) -> Result<(), OxanusError> {
        self.storage
            .internal
            .update_state(
                &self.job_id,
                serde_json::to_value(state).map_err(OxanusError::JobStateJsonError)?,
            )
            .await?;
        Ok(())
    }

    pub async fn get<S: DeserializeOwned>(&self) -> Result<Option<S>, OxanusError> {
        Ok(match self.value.clone() {
            Some(state) => {
                Some(serde_json::from_value(state).map_err(OxanusError::JobStateJsonError)?)
            }
            None => None,
        })
    }
}
