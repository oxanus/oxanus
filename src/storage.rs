use crate::{
    JobEnvelope, JobId, OxanusError, Queue, StorageInternal, Worker,
    storage_builder::StorageBuilder,
};

#[derive(Clone)]
pub struct Storage {
    pub(crate) internal: StorageInternal,
}

impl Storage {
    pub fn builder() -> StorageBuilder {
        StorageBuilder::new()
    }

    pub async fn enqueue<T, DT, ET>(&self, queue: impl Queue, job: T) -> Result<JobId, OxanusError>
    where
        T: Worker<Context = DT, Error = ET> + serde::Serialize,
        DT: Send + Sync + Clone + 'static,
        ET: std::error::Error + Send + Sync + 'static,
    {
        self.enqueue_in(queue, job, 0).await
    }

    pub async fn enqueue_in<T, DT, ET>(
        &self,
        queue: impl Queue,
        job: T,
        delay: u64,
    ) -> Result<JobId, OxanusError>
    where
        T: Worker<Context = DT, Error = ET> + serde::Serialize,
        DT: Send + Sync + Clone + 'static,
        ET: std::error::Error + Send + Sync + 'static,
    {
        let envelope = JobEnvelope::new(queue.key().clone(), job)?;

        tracing::trace!("Enqueuing job: {:?}", envelope);

        if delay > 0 {
            self.internal.enqueue_in(envelope, delay).await
        } else {
            self.internal.enqueue(envelope).await
        }
    }

    pub async fn enqueued_count(&self, queue: impl Queue) -> Result<usize, OxanusError> {
        self.internal.enqueued_count(&queue.key()).await
    }

    pub async fn dead_count(&self) -> Result<usize, OxanusError> {
        self.internal.dead_count().await
    }

    pub async fn retries_count(&self) -> Result<usize, OxanusError> {
        self.internal.retries_count().await
    }

    pub async fn scheduled_count(&self) -> Result<usize, OxanusError> {
        self.internal.scheduled_count().await
    }

    pub fn namespace(&self) -> &str {
        self.internal.namespace()
    }
}
