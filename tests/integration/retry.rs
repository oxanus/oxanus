use deadpool_redis::redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use testresult::TestResult;

use crate::shared::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerRedisSetWithRetry {
    pub key: String,
    pub value_first: String,
    pub value_second: String,
}

#[async_trait::async_trait]
impl oxanus::Worker for WorkerRedisSetWithRetry {
    type Context = WorkerState;
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::Context { ctx, .. }: &oxanus::Context<WorkerState>,
    ) -> Result<(), WorkerError> {
        let mut redis = ctx.redis.get().await?;
        let value: Option<String> = redis.get(&self.key).await?;
        if value.is_some() {
            let _: () = redis
                .set_ex(&self.key, self.value_second.clone(), 3)
                .await?;
            return Ok(());
        }
        let _: () = redis.set_ex(&self.key, self.value_first.clone(), 3).await?;
        Err(WorkerError::GenericError("Key not set".to_string()))
    }

    fn retry_delay(&self, _retries: u32) -> u64 {
        0
    }

    fn max_retries(&self) -> u32 {
        1
    }
}

#[tokio::test]
pub async fn test_retry() -> TestResult {
    let redis_pool = setup();
    let mut redis_conn = redis_pool.get().await?;

    let ctx = oxanus::Context::value(WorkerState {
        redis: redis_pool.clone(),
    });

    let storage = oxanus::Storage::builder()
        .from_redis_pool(redis_pool.clone())
        .namespace(random_string())
        .build()?;
    let config = oxanus::Config::new(&storage)
        .register_queue::<QueueOne>()
        .register_worker::<WorkerRedisSetWithRetry>()
        .exit_when_processed(2);

    let random_key = uuid::Uuid::new_v4().to_string();
    let random_value_first = uuid::Uuid::new_v4().to_string();
    let random_value_second = uuid::Uuid::new_v4().to_string();

    storage
        .enqueue(
            QueueOne,
            WorkerRedisSetWithRetry {
                key: random_key.clone(),
                value_first: random_value_first.clone(),
                value_second: random_value_second.clone(),
            },
        )
        .await?;

    assert_eq!(storage.enqueued_count(QueueOne).await?, 1);

    oxanus::run(config, ctx).await?;

    let value: Option<String> = redis_conn.get(random_key).await?;

    assert_eq!(value, Some(random_value_second));
    assert_eq!(storage.dead_count().await?, 0);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);
    assert_eq!(storage.jobs_count().await?, 0);

    Ok(())
}
