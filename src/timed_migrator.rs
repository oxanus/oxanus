use redis::AsyncCommands;
use std::collections::HashMap;

use crate::{JobEnvelope, storage};

pub async fn run(redis_client: redis::Client, queue: String) {
    tracing::info!("Starting timed migrator for queue: {}", queue);

    let mut redis_manager = redis::aio::ConnectionManager::new(redis_client)
        .await
        .unwrap();

    loop {
        let now = chrono::Utc::now().timestamp_micros();

        let job_ids: Vec<String> = redis_manager.zrangebyscore(&queue, 0, now).await.unwrap();

        if job_ids.is_empty() {
            tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
            continue;
        }

        let envelopes = storage::get_many(&mut redis_manager, &job_ids)
            .await
            .unwrap();

        let mut map: HashMap<String, Vec<JobEnvelope>> = HashMap::new();

        for envelope in envelopes {
            map.entry(envelope.job.queue.clone())
                .or_insert(vec![])
                .push(envelope);
        }

        for (queue, envelopes) in map {
            let job_ids: Vec<&str> = envelopes
                .iter()
                .map(|envelope| envelope.id.as_str())
                .collect();

            let _: i32 = redis_manager.rpush(queue, job_ids).await.unwrap();
        }

        let _: i32 = redis_manager.zrembyscore(&queue, 0, now).await.unwrap();
    }
}
