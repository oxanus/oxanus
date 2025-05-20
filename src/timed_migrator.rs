use redis::AsyncCommands;
use std::collections::HashMap;

use crate::{JobEnvelope, redis_helper};

const SCHEDULED_QUEUE: &str = "oxanus:scheduled";

pub async fn run(redis_client: redis::Client, queue: String) {
    tracing::info!("Starting timed migrator for queue: {}", queue);

    let mut redis_manager = redis::aio::ConnectionManager::new(redis_client)
        .await
        .unwrap();

    loop {
        let now = chrono::Utc::now().timestamp_micros();

        let envelopes_str: Vec<String> = redis_manager.zrangebyscore(&queue, 0, now).await.unwrap();

        if envelopes_str.is_empty() {
            tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
            continue;
        }

        let mut envelopes: HashMap<String, Vec<JobEnvelope>> = HashMap::new();

        for envelope in envelopes_str {
            let envelope: JobEnvelope = serde_json::from_str(&envelope).unwrap();
            envelopes
                .entry(envelope.job.queue.clone())
                .or_insert(vec![])
                .push(envelope);
        }

        for (queue, envelopes) in envelopes {
            redis_helper::rpush_many(&mut redis_manager, &queue, &envelopes)
                .await
                .unwrap();
        }

        let _: i32 = redis_manager
            .zrembyscore(SCHEDULED_QUEUE, 0, now)
            .await
            .unwrap();
    }
}
