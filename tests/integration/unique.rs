use oxanus::Queue;
use serde::{Deserialize, Serialize};
use testresult::TestResult;

use crate::shared::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerUnique {
    pub id: i32,
}

#[async_trait::async_trait]
impl oxanus::Worker for WorkerUnique {
    type Context = ();
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::WorkerContext { .. }: &oxanus::WorkerContext<()>,
    ) -> Result<(), WorkerError> {
        Ok(())
    }

    fn unique_id(&self) -> Option<String> {
        Some(format!("unique:{}", self.id))
    }

    fn retry_delay(&self, _retries: u32) -> u64 {
        0
    }

    fn max_retries(&self) -> u32 {
        0
    }
}

#[tokio::test]
pub async fn test_unique() -> TestResult {
    let redis_client = setup();

    let ctx = oxanus::WorkerContextValue::new(());

    let storage = oxanus::Storage::new(redis_client).namespace(random_string());
    let config = oxanus::Config::new(storage.clone())
        .register_queue::<QueueOne>()
        .register_worker::<WorkerUnique>()
        .exit_when_processed(2);

    oxanus::enqueue(&storage, QueueOne, WorkerUnique { id: 1 }).await?;
    oxanus::enqueue(&storage, QueueOne, WorkerUnique { id: 1 }).await?;
    oxanus::enqueue(&storage, QueueOne, WorkerUnique { id: 2 }).await?;
    oxanus::enqueue(&storage, QueueOne, WorkerUnique { id: 2 }).await?;
    oxanus::enqueue(&storage, QueueOne, WorkerUnique { id: 2 }).await?;

    assert_eq!(storage.enqueued_count(&QueueOne.key()).await?, 2);

    oxanus::run(config, ctx).await?;

    assert_eq!(storage.dead_count().await?, 0);
    assert_eq!(storage.enqueued_count(&QueueOne.key()).await?, 0);

    Ok(())
}
