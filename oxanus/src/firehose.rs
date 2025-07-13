use deadpool_redis::redis::AsyncCommands;
use serde::Serialize;

use crate::{
    OxanusError, job_envelope::JobEnvelope, result_collector::JobResult, storage_internal::Process,
};

#[derive(Clone)]
pub struct Firehose {
    key: String,
    pool: deadpool_redis::Pool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FirehouseEvent {
    JobExecuted(JobResult),
    JobEnqueued(JobEnvelope),
    ProcessStarted(Process),
    ProcessExited(Process),
}

impl Firehose {
    pub fn new(pool: deadpool_redis::Pool, key: String) -> Self {
        Self { pool, key }
    }

    pub async fn event(&self, event: FirehouseEvent) -> Result<(), OxanusError> {
        let firehose = self.clone();
        tokio::spawn(async move {
            firehose.event_sync(event).await.ok();
        });
        Ok(())
    }

    pub async fn event_sync(&self, event: FirehouseEvent) -> Result<(), OxanusError> {
        let mut redis = self.pool.get().await?;
        self.event_w_redis_sync(&mut redis, event).await
    }

    pub async fn event_w_redis_sync(
        &self,
        redis: &mut deadpool_redis::Connection,
        event: FirehouseEvent,
    ) -> Result<(), OxanusError> {
        let event_json = serde_json::to_string(&event)?;
        let _: () = redis.publish(self.key.clone(), event_json).await?;
        Ok(())
    }
}
