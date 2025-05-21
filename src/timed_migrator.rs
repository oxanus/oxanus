use crate::storage;

pub async fn run(redis_client: redis::Client, queue: String) {
    tracing::info!("Starting timed migrator for queue: {}", queue);

    let mut redis_manager = redis::aio::ConnectionManager::new(redis_client)
        .await
        .unwrap();

    loop {
        storage::enqueue_scheduled(&mut redis_manager, &queue)
            .await
            .expect("Failed to enqueue scheduled jobs");

        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    }
}
