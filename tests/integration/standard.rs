use crate::shared::*;
use deadpool_redis::redis::AsyncCommands;
use oxanus::Queue;
use testresult::TestResult;

#[tokio::test]
pub async fn test_standard() -> TestResult {
    let redis_pool = setup();
    let mut redis_conn = redis_pool.get().await?;

    let ctx = oxanus::WorkerContextValue::new(WorkerState {
        redis: redis_pool.clone(),
    });

    let storage = oxanus::Storage::from_env().namespace(random_string());
    let config = oxanus::Config::new(storage.clone())
        .register_queue::<QueueOne>()
        .register_worker::<WorkerRedisSet>()
        .exit_when_processed(1);

    let random_key = uuid::Uuid::new_v4().to_string();
    let random_value = uuid::Uuid::new_v4().to_string();

    oxanus::enqueue(
        &storage,
        QueueOne,
        WorkerRedisSet {
            key: random_key.clone(),
            value: random_value.clone(),
        },
    )
    .await?;

    assert_eq!(storage.enqueued_count(&QueueOne.key()).await?, 1);

    oxanus::run(config, ctx).await?;

    let value: Option<String> = redis_conn.get(random_key).await?;

    assert_eq!(value, Some(random_value));
    assert_eq!(storage.enqueued_count(&QueueOne.key()).await?, 0);

    Ok(())
}
