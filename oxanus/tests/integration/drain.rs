use crate::shared::*;
use serde::Serialize;
use testresult::TestResult;

#[derive(Serialize)]
struct QueueDynamic(i32);

#[derive(Serialize)]
struct QueueStatic;

impl oxanus::Queue for QueueDynamic {
    fn to_config() -> oxanus::QueueConfig {
        oxanus::QueueConfig::as_dynamic("dynamic")
    }
}

impl oxanus::Queue for QueueStatic {
    fn to_config() -> oxanus::QueueConfig {
        oxanus::QueueConfig::as_static("static")
    }
}

#[tokio::test]
pub async fn test_drain() -> TestResult {
    let redis_pool = setup();
    let ctx = oxanus::Context::value(());
    let storage = oxanus::Storage::builder()
        .from_redis_pool(redis_pool)
        .namespace(random_string())
        .build()?;
    let config = oxanus::Config::new(&storage)
        .register_queue::<QueueDynamic>()
        .register_worker::<WorkerNoop>()
        .exit_when_processed(2);

    storage.enqueue(QueueDynamic(1), WorkerNoop {}).await?;
    storage.enqueue(QueueDynamic(2), WorkerNoop {}).await?;
    storage.enqueue(QueueStatic, WorkerNoop {}).await?;
    storage.enqueue(QueueStatic, WorkerNoop {}).await?;

    assert_eq!(storage.jobs_count().await?, 4);
    assert_eq!(storage.enqueued_count(QueueDynamic(1)).await?, 1);
    assert_eq!(storage.enqueued_count(QueueDynamic(2)).await?, 1);
    assert_eq!(storage.enqueued_count(QueueDynamic(3)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueStatic).await?, 2);

    let stats = oxanus::drain(&config, ctx.clone(), QueueDynamic(1)).await?;

    assert_eq!(storage.jobs_count().await?, 3);
    assert_eq!(stats.processed, 1);
    assert_eq!(stats.succeeded, 1);
    assert_eq!(stats.failed, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(1)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(2)).await?, 1);
    assert_eq!(storage.enqueued_count(QueueDynamic(3)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueStatic).await?, 2);

    let stats = oxanus::drain(&config, ctx.clone(), QueueDynamic(2)).await?;

    assert_eq!(storage.jobs_count().await?, 2);
    assert_eq!(stats.processed, 1);
    assert_eq!(stats.succeeded, 1);
    assert_eq!(stats.failed, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(1)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(2)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(3)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueStatic).await?, 2);

    let stats = oxanus::drain(&config, ctx, QueueStatic).await?;

    assert_eq!(storage.jobs_count().await?, 0);
    assert_eq!(stats.processed, 2);
    assert_eq!(stats.succeeded, 2);
    assert_eq!(stats.failed, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(1)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(2)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(3)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueStatic).await?, 0);

    Ok(())
}
