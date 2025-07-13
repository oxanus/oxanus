use crate::shared::*;
use serde::Serialize;
use testresult::TestResult;

#[derive(Serialize)]
struct QueueDynamic(i32);

#[derive(Serialize)]
struct QueueStatic;

impl oxanus::Queue for QueueDynamic {
    fn to_config() -> oxanus::QueueConfig {
        oxanus::QueueConfig::as_dynamic("dynamic").concurrency(6)
    }
}

impl oxanus::Queue for QueueStatic {
    fn to_config() -> oxanus::QueueConfig {
        oxanus::QueueConfig::as_static("static").concurrency(2)
    }
}

#[tokio::test]
pub async fn test_stats() -> TestResult {
    let redis_pool = setup();
    let ctx = oxanus::Context::value(());
    let storage = oxanus::Storage::builder()
        .from_redis_pool(redis_pool)
        .namespace(random_string())
        .build()?;
    let config = oxanus::Config::new(&storage)
        .register_queue::<QueueDynamic>()
        .register_queue::<QueueStatic>()
        .register_worker::<WorkerNoop>()
        .exit_when_processed(8);

    storage.enqueue(QueueDynamic(1), WorkerNoop {}).await?;
    storage.enqueue(QueueDynamic(2), WorkerNoop {}).await?;
    storage.enqueue(QueueStatic, WorkerNoop {}).await?;
    storage.enqueue(QueueStatic, WorkerNoop {}).await?;
    storage.enqueue(QueueDynamic(1), WorkerNoop {}).await?;
    storage.enqueue(QueueDynamic(2), WorkerNoop {}).await?;
    storage.enqueue(QueueDynamic(3), WorkerNoop {}).await?;
    storage.enqueue(QueueDynamic(4), WorkerNoop {}).await?;

    let stats = storage.stats().await?;

    assert_eq!(stats.queues.len(), 2);
    assert_eq!(stats.queues[0].key, "dynamic");
    assert_eq!(stats.queues[0].queues.len(), 4);

    assert_eq!(stats.queues[0].queues[0].suffix, "1");
    assert_eq!(stats.queues[0].queues[0].enqueued, 2);
    assert_eq!(stats.queues[0].queues[0].processed, 0);
    assert_eq!(stats.queues[0].queues[0].succeeded, 0);
    assert_eq!(stats.queues[0].queues[0].panicked, 0);
    assert_eq!(stats.queues[0].queues[0].failed, 0);
    assert!(stats.queues[0].queues[0].latency > 0.0);

    assert_eq!(stats.queues[0].queues[1].suffix, "2");
    assert_eq!(stats.queues[0].queues[1].enqueued, 2);
    assert_eq!(stats.queues[0].queues[1].processed, 0);
    assert_eq!(stats.queues[0].queues[1].succeeded, 0);
    assert_eq!(stats.queues[0].queues[1].panicked, 0);
    assert_eq!(stats.queues[0].queues[1].failed, 0);
    assert!(stats.queues[0].queues[1].latency > 0.0);

    assert_eq!(stats.queues[0].queues[2].suffix, "3");
    assert_eq!(stats.queues[0].queues[2].enqueued, 1);
    assert_eq!(stats.queues[0].queues[2].processed, 0);
    assert_eq!(stats.queues[0].queues[2].succeeded, 0);
    assert_eq!(stats.queues[0].queues[2].panicked, 0);
    assert_eq!(stats.queues[0].queues[2].failed, 0);
    assert!(stats.queues[0].queues[2].latency > 0.0);

    assert_eq!(stats.queues[0].queues[3].suffix, "4");
    assert_eq!(stats.queues[0].queues[3].enqueued, 1);
    assert_eq!(stats.queues[0].queues[3].processed, 0);
    assert_eq!(stats.queues[0].queues[3].succeeded, 0);
    assert_eq!(stats.queues[0].queues[3].panicked, 0);
    assert_eq!(stats.queues[0].queues[3].failed, 0);
    assert!(stats.queues[0].queues[3].latency > 0.0);

    assert_eq!(stats.queues[0].enqueued, 6);
    assert_eq!(stats.queues[0].processed, 0);
    assert_eq!(stats.queues[0].succeeded, 0);
    assert_eq!(stats.queues[0].panicked, 0);
    assert_eq!(stats.queues[0].failed, 0);
    assert!(stats.queues[0].latency > 0.0);

    assert_eq!(stats.queues[1].key, "static");
    assert_eq!(stats.queues[1].queues.len(), 0);

    assert_eq!(stats.queues[1].enqueued, 2);
    assert_eq!(stats.queues[1].processed, 0);
    assert_eq!(stats.queues[1].succeeded, 0);
    assert_eq!(stats.queues[1].panicked, 0);
    assert_eq!(stats.queues[1].failed, 0);
    assert!(stats.queues[1].latency > 0.0);

    let stats = oxanus::run(config, ctx).await?;

    assert_eq!(stats.processed, 8);

    let stats = storage.stats().await?;

    assert_eq!(stats.global.processed, 8);

    assert_eq!(stats.queues.len(), 2);
    assert_eq!(stats.queues[0].key, "dynamic");
    assert_eq!(stats.queues[0].queues.len(), 4);

    assert_eq!(stats.queues[0].enqueued, 0);
    assert_eq!(stats.queues[0].processed, 6);
    assert_eq!(stats.queues[0].succeeded, 6);
    assert_eq!(stats.queues[0].panicked, 0);
    assert_eq!(stats.queues[0].failed, 0);
    assert_eq!(stats.queues[0].latency, 0.0);

    assert_eq!(stats.queues[0].queues[0].suffix, "1");
    assert_eq!(stats.queues[0].queues[0].enqueued, 0);
    assert_eq!(stats.queues[0].queues[0].processed, 2);
    assert_eq!(stats.queues[0].queues[0].succeeded, 2);
    assert_eq!(stats.queues[0].queues[0].panicked, 0);
    assert_eq!(stats.queues[0].queues[0].failed, 0);
    assert_eq!(stats.queues[0].queues[0].latency, 0.0);

    assert_eq!(stats.queues[0].queues[1].suffix, "2");
    assert_eq!(stats.queues[0].queues[1].enqueued, 0);
    assert_eq!(stats.queues[0].queues[1].processed, 2);
    assert_eq!(stats.queues[0].queues[1].succeeded, 2);
    assert_eq!(stats.queues[0].queues[1].panicked, 0);
    assert_eq!(stats.queues[0].queues[1].failed, 0);
    assert_eq!(stats.queues[0].queues[1].latency, 0.0);

    assert_eq!(stats.queues[0].queues[2].suffix, "3");
    assert_eq!(stats.queues[0].queues[2].enqueued, 0);
    assert_eq!(stats.queues[0].queues[2].processed, 1);
    assert_eq!(stats.queues[0].queues[2].succeeded, 1);
    assert_eq!(stats.queues[0].queues[2].panicked, 0);
    assert_eq!(stats.queues[0].queues[2].failed, 0);
    assert_eq!(stats.queues[0].queues[2].latency, 0.0);

    assert_eq!(stats.queues[0].queues[3].suffix, "4");
    assert_eq!(stats.queues[0].queues[3].enqueued, 0);
    assert_eq!(stats.queues[0].queues[3].processed, 1);
    assert_eq!(stats.queues[0].queues[3].succeeded, 1);
    assert_eq!(stats.queues[0].queues[3].panicked, 0);
    assert_eq!(stats.queues[0].queues[3].failed, 0);
    assert_eq!(stats.queues[0].queues[3].latency, 0.0);

    assert_eq!(stats.queues[1].key, "static");
    assert_eq!(stats.queues[1].queues.len(), 0);

    assert_eq!(stats.queues[1].enqueued, 0);
    assert_eq!(stats.queues[1].processed, 2);
    assert_eq!(stats.queues[1].succeeded, 2);
    assert_eq!(stats.queues[1].panicked, 0);
    assert_eq!(stats.queues[1].failed, 0);
    assert_eq!(stats.queues[1].latency, 0.0);

    Ok(())
}
