use crate::{
    error::OxanusError,
    job_envelope::{JobEnvelope, JobId},
    queue::Queue,
    storage_builder::StorageBuilder,
    storage_internal::StorageInternal,
    worker::Worker,
};

/// Storage provides the main interface for job management in Oxanus.
///
/// It handles all job operations including enqueueing, scheduling, and monitoring.
/// Storage instances are created using the [`Storage::builder()`] method.
///
/// # Examples
///
/// ```rust
/// use oxanus::{Storage, Queue, Worker};
///
/// async fn example() -> Result<(), oxanus::OxanusError> {
///     let storage = Storage::builder().from_env()?.build()?;
///
///     // Enqueue a job
///     storage.enqueue(MyQueue, MyWorker { data: "hello" }).await?;
///
///     // Schedule a job for later
///     storage.enqueue_in(MyQueue, MyWorker { data: "delayed" }, 300).await?;
///
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct Storage {
    pub(crate) internal: StorageInternal,
}

impl Storage {
    /// Creates a new [`StorageBuilder`] for configuring and building a Storage instance.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use oxanus::Storage;
    ///
    /// let builder = Storage::builder();
    /// let storage = builder.from_env()?.build()?;
    /// ```
    pub fn builder() -> StorageBuilder {
        StorageBuilder::new()
    }

    /// Enqueues a job to be processed immediately.
    ///
    /// # Arguments
    ///
    /// * `queue` - The queue to enqueue the job to
    /// * `job` - The job to enqueue
    ///
    /// # Returns
    ///
    /// A [`JobId`] that can be used to track the job, or an [`OxanusError`] if the operation fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use oxanus::{Storage, Queue, Worker};
    ///
    /// async fn example(storage: &Storage) -> Result<(), oxanus::OxanusError> {
    ///     let job_id = storage.enqueue(MyQueue, MyWorker { data: "hello" }).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn enqueue<T, DT, ET>(&self, queue: impl Queue, job: T) -> Result<JobId, OxanusError>
    where
        T: Worker<Context = DT, Error = ET> + serde::Serialize,
        DT: Send + Sync + Clone + 'static,
        ET: std::error::Error + Send + Sync + 'static,
    {
        self.enqueue_in(queue, job, 0).await
    }

    /// Enqueues a job to be processed after a specified delay.
    ///
    /// # Arguments
    ///
    /// * `queue` - The queue to enqueue the job to
    /// * `job` - The job to enqueue
    /// * `delay` - The delay in seconds before the job should be processed
    ///
    /// # Returns
    ///
    /// A [`JobId`] that can be used to track the job, or an [`OxanusError`] if the operation fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use oxanus::{Storage, Queue, Worker};
    ///
    /// async fn example(storage: &Storage) -> Result<(), oxanus::OxanusError> {
    ///     // Schedule a job to run in 5 minutes
    ///     let job_id = storage.enqueue_in(MyQueue, MyWorker { data: "delayed" }, 300).await?;
    ///     Ok(())
    /// }
    /// ```
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

    /// Returns the number of jobs currently enqueued in the specified queue.
    ///
    /// # Arguments
    ///
    /// * `queue` - The queue to count jobs for
    ///
    /// # Returns
    ///
    /// The number of enqueued jobs, or an [`OxanusError`] if the operation fails.
    pub async fn enqueued_count(&self, queue: impl Queue) -> Result<usize, OxanusError> {
        self.internal.enqueued_count(&queue.key()).await
    }

    /// Returns the number of jobs that have failed and moved to the dead queue.
    ///
    /// # Returns
    ///
    /// The number of dead jobs, or an [`OxanusError`] if the operation fails.
    pub async fn dead_count(&self) -> Result<usize, OxanusError> {
        self.internal.dead_count().await
    }

    /// Returns the number of jobs that are currently being retried.
    ///
    /// # Returns
    ///
    /// The number of retrying jobs, or an [`OxanusError`] if the operation fails.
    pub async fn retries_count(&self) -> Result<usize, OxanusError> {
        self.internal.retries_count().await
    }

    /// Returns the number of jobs that are scheduled for future execution.
    ///
    /// # Returns
    ///
    /// The number of scheduled jobs, or an [`OxanusError`] if the operation fails.
    pub async fn scheduled_count(&self) -> Result<usize, OxanusError> {
        self.internal.scheduled_count().await
    }

    /// Returns the number of jobs that are currently enqueued or scheduled for future execution.
    ///
    /// # Returns
    ///
    /// The number of jobs, or an [`OxanusError`] if the operation fails.
    pub async fn jobs_count(&self) -> Result<usize, OxanusError> {
        self.internal.jobs_count().await
    }

    /// Returns the namespace this storage instance is using.
    ///
    /// # Returns
    ///
    /// The namespace string.
    pub fn namespace(&self) -> &str {
        self.internal.namespace()
    }
}
