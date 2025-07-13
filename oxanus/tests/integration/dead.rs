use serde::{Deserialize, Serialize};
use testresult::TestResult;

use crate::shared::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerFail {}

#[async_trait::async_trait]
impl oxanus::Worker for WorkerFail {
    type Context = ();
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::Context { .. }: &oxanus::Context<()>,
    ) -> Result<(), WorkerError> {
        Err(WorkerError::Generic(
            "I have nothing to live for...".to_string(),
        ))
    }

    fn retry_delay(&self, _retries: u32) -> u64 {
        0
    }

    fn max_retries(&self) -> u32 {
        0
    }
}

#[tokio::test]
pub async fn test_dead() -> TestResult {
    let redis_pool = setup();
    let ctx = oxanus::Context::value(());
    let storage = oxanus::Storage::builder()
        .from_redis_pool(redis_pool.clone())
        .namespace(random_string())
        .build()?;
    let config = oxanus::Config::new(&storage)
        .register_queue::<QueueOne>()
        .register_worker::<WorkerFail>()
        .exit_when_processed(1);

    storage.enqueue(QueueOne, WorkerFail {}).await?;

    assert_eq!(storage.enqueued_count(QueueOne).await?, 1);

    oxanus::run(config, ctx).await?;

    assert_eq!(storage.dead_count().await?, 1);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);
    assert_eq!(storage.jobs_count().await?, 0);

    Ok(())
}
