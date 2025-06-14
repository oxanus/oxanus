use crate::shared::*;
use deadpool_redis::redis::AsyncCommands;
use testresult::TestResult;

#[tokio::test]
pub async fn test_standard() -> TestResult {
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
        .register_worker::<WorkerRedisSet>()
        .exit_when_processed(1);

    let random_key = uuid::Uuid::new_v4().to_string();
    let random_value = uuid::Uuid::new_v4().to_string();

    storage
        .enqueue(
            QueueOne,
            WorkerRedisSet {
                key: random_key.clone(),
                value: random_value.clone(),
            },
        )
        .await?;

    assert_eq!(storage.enqueued_count(QueueOne).await?, 1);

    oxanus::run(config, ctx).await?;

    let value: Option<String> = redis_conn.get(random_key).await?;

    assert_eq!(value, Some(random_value));
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);

    Ok(())
}
