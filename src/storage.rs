use redis::AsyncCommands;
use std::{collections::HashMap, num::NonZero};

use crate::{JobEnvelope, OxanusError};

pub const SCHEDULE_QUEUE: &str = "oxanus:schedule";
pub const RETRY_QUEUE: &str = "oxanus:retry";
pub const PROCESSING_QUEUE_PREFIX: &str = "oxanus:processing";
const PROCESSES_KEY: &str = "oxanus:processes";
const JOBS_KEY: &str = "oxanus:jobs";
const DEAD_QUEUE: &str = "oxanus:dead";
const JOB_EXPIRE_TIME: i64 = 7 * 24 * 3600; // 7 days
const RESURRECT_THRESHOLD_SECS: i64 = 5;

pub async fn enqueue(
    redis: &redis::aio::ConnectionManager,
    envelope: &JobEnvelope,
) -> Result<(), OxanusError> {
    let mut redis = redis.clone();

    if should_skip_job(&mut redis, envelope).await? {
        tracing::warn!("Unique job {} already exists, skipping", envelope.id);
        return Ok(());
    }

    let _: () = redis::pipe()
        .hset(JOBS_KEY, &envelope.id, serde_json::to_string(envelope)?)
        .hexpire(
            JOBS_KEY,
            JOB_EXPIRE_TIME,
            redis::ExpireOption::NONE,
            &envelope.id,
        )
        .lpush(&envelope.job.queue, &envelope.id)
        .query_async(&mut redis)
        .await?;

    Ok(())
}

async fn should_skip_job(
    redis: &mut redis::aio::ConnectionManager,
    envelope: &JobEnvelope,
) -> Result<bool, OxanusError> {
    if !envelope.meta.unique {
        return Ok(false);
    }

    let exists: bool = redis.hexists(JOBS_KEY, &envelope.id).await?;
    Ok(exists)
}

pub async fn enqueue_in(
    redis: &redis::aio::ConnectionManager,
    envelope: &JobEnvelope,
    delay_s: u64,
) -> Result<(), OxanusError> {
    let mut redis = redis.clone();

    if should_skip_job(&mut redis, &envelope).await? {
        tracing::warn!("Unique job {} already exists, skipping", envelope.id);
        return Ok(());
    }

    let _: () = redis::pipe()
        .hset(JOBS_KEY, &envelope.id, serde_json::to_string(&envelope)?)
        .hexpire(
            JOBS_KEY,
            JOB_EXPIRE_TIME,
            redis::ExpireOption::NONE,
            &envelope.id,
        )
        .zadd(
            SCHEDULE_QUEUE,
            &envelope.id,
            envelope.job.created_at + delay_s * 1_000_000,
        )
        .query_async(&mut redis)
        .await?;
    Ok(())
}

pub async fn blocking_dequeue(
    redis: &redis::aio::ConnectionManager,
    queue: &str,
    timeout: f64,
) -> Result<Option<String>, OxanusError> {
    let mut redis = redis.clone();
    let job_id: Option<String> = redis
        .blmove(
            queue,
            current_processing_queue(),
            redis::Direction::Right,
            redis::Direction::Left,
            timeout,
        )
        .await?;
    Ok(job_id)
}

pub async fn dequeue(
    redis: &redis::aio::ConnectionManager,
    queue: &str,
) -> Result<Option<String>, OxanusError> {
    let mut redis = redis.clone();
    let job_id: Option<String> = redis
        .lmove(
            queue,
            current_processing_queue(),
            redis::Direction::Right,
            redis::Direction::Left,
        )
        .await?;
    Ok(job_id)
}

pub async fn retry_in(
    redis: &redis::aio::ConnectionManager,
    envelope: JobEnvelope,
    delay_s: u64,
) -> Result<(), OxanusError> {
    let updated_envelope = envelope.with_retries_incremented();
    let mut redis = redis.clone();
    let _: () = redis::pipe()
        .hset(
            JOBS_KEY,
            &updated_envelope.id,
            serde_json::to_string(&updated_envelope)?,
        )
        .hexpire(
            JOBS_KEY,
            JOB_EXPIRE_TIME,
            redis::ExpireOption::NONE,
            &updated_envelope.id,
        )
        .zadd(
            RETRY_QUEUE,
            updated_envelope.id,
            updated_envelope.job.created_at + delay_s * 1_000_000,
        )
        .query_async(&mut redis)
        .await?;
    Ok(())
}

pub async fn get(
    redis: &redis::aio::ConnectionManager,
    id: &str,
) -> Result<Option<JobEnvelope>, OxanusError> {
    let mut redis = redis.clone();
    let envelope: Option<String> = redis.hget(JOBS_KEY, id).await?;
    match envelope {
        Some(envelope) => Ok(Some(serde_json::from_str(&envelope)?)),
        None => Ok(None),
    }
}

pub async fn get_many(
    redis: &redis::aio::ConnectionManager,
    ids: &[String],
) -> Result<Vec<JobEnvelope>, OxanusError> {
    let mut redis = redis.clone();
    let mut cmd = redis::cmd("HMGET");
    cmd.arg(JOBS_KEY);
    cmd.arg(ids);
    let envelopes_str: Vec<String> = cmd.query_async(&mut redis).await?;
    let mut envelopes: Vec<JobEnvelope> = vec![];
    for envelope_str in envelopes_str {
        envelopes.push(serde_json::from_str(&envelope_str)?);
    }
    Ok(envelopes)
}

pub async fn kill(
    redis: &redis::aio::ConnectionManager,
    envelope: &JobEnvelope,
) -> Result<(), OxanusError> {
    let mut redis = redis.clone();
    let _: () = redis::pipe()
        .hdel(JOBS_KEY, &envelope.id)
        .lpush(DEAD_QUEUE, &serde_json::to_string(envelope)?)
        .query_async(&mut redis)
        .await?;
    Ok(())
}

pub async fn finish(
    redis: &redis::aio::ConnectionManager,
    envelope: &JobEnvelope,
) -> Result<(), OxanusError> {
    let mut redis = redis.clone();
    let _: () = redis::pipe()
        .hdel(JOBS_KEY, &envelope.id)
        .lrem(current_processing_queue(), 1, &envelope.id)
        .query_async(&mut redis)
        .await?;
    Ok(())
}

pub async fn enqueue_scheduled(
    redis: &redis::aio::ConnectionManager,
    schedule_queue: &str,
) -> Result<usize, OxanusError> {
    let mut redis = redis.clone();
    let now = chrono::Utc::now().timestamp_micros();

    let job_ids: Vec<String> = redis.zrangebyscore(schedule_queue, 0, now).await?;

    if job_ids.is_empty() {
        return Ok(0);
    }

    let envelopes = get_many(&redis, &job_ids).await?;

    let mut map: HashMap<&str, Vec<&JobEnvelope>> = HashMap::new();
    let envelopes_count = envelopes.len();

    for envelope in envelopes.iter() {
        map.entry(envelope.job.queue.as_str())
            .or_insert(vec![])
            .push(envelope);
    }

    for (queue, envelopes) in map {
        let job_ids: Vec<&str> = envelopes
            .iter()
            .map(|envelope| envelope.id.as_str())
            .collect();

        let _: i32 = redis.lpush(queue, job_ids).await?;
    }

    let _: i32 = redis.zrembyscore(&schedule_queue, 0, now).await?;

    Ok(envelopes_count)
}

pub async fn retry_loop(redis_client: redis::Client) -> Result<(), OxanusError> {
    tracing::info!("Starting retry loop");

    let mut redis_manager = redis::aio::ConnectionManager::new(redis_client).await?;

    loop {
        enqueue_scheduled(&mut redis_manager, RETRY_QUEUE).await?;

        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    }
}

pub async fn schedule_loop(redis_client: redis::Client) -> Result<(), OxanusError> {
    tracing::info!("Starting schedule loop");

    let mut redis_manager = redis::aio::ConnectionManager::new(redis_client).await?;

    loop {
        enqueue_scheduled(&mut redis_manager, SCHEDULE_QUEUE).await?;

        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    }
}

pub async fn ping_loop(redis_client: redis::Client) -> Result<(), OxanusError> {
    let mut redis_manager = redis::aio::ConnectionManager::new(redis_client).await?;

    loop {
        ping(&mut redis_manager).await?;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

pub async fn ping(redis: &redis::aio::ConnectionManager) -> Result<(), OxanusError> {
    let mut redis = redis.clone();
    let _: () = redis
        .zadd(
            PROCESSES_KEY,
            current_process_id(),
            chrono::Utc::now().timestamp(),
        )
        .await?;
    Ok(())
}

pub async fn resurrect_loop(redis_client: redis::Client) -> Result<(), OxanusError> {
    tracing::info!("Starting resurrect loop");

    let mut redis_manager = redis::aio::ConnectionManager::new(redis_client).await?;

    loop {
        resurrect(&mut redis_manager).await?;
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }
}

pub async fn resurrect(redis: &mut redis::aio::ConnectionManager) -> Result<(), OxanusError> {
    let processes: Vec<String> = redis
        .zrangebyscore(
            PROCESSES_KEY,
            0,
            chrono::Utc::now().timestamp() - RESURRECT_THRESHOLD_SECS,
        )
        .await?;

    for process_id in processes {
        tracing::info!("Dead process detected: {}", process_id);

        let processing_queue = processing_queue(&process_id);

        loop {
            let job_ids: Vec<String> = redis
                .lpop(&processing_queue, Some(NonZero::new(10).unwrap()))
                .await?;

            if job_ids.is_empty() {
                break;
            }

            for job_id in job_ids {
                match get(&redis, &job_id).await? {
                    Some(envelope) => {
                        tracing::info!(
                            job_id = job_id,
                            queue = envelope.job.queue,
                            job = envelope.job.name,
                            "Resurrecting job"
                        );
                        enqueue(&redis, &envelope).await?;
                        let _: () = redis.lrem(&processing_queue, 1, &job_id).await?;
                    }
                    None => tracing::warn!("Job {} not found", job_id),
                }
            }
        }

        let _: () = redis.zrem(&PROCESSES_KEY, &process_id).await?;
    }

    Ok(())
}

fn processing_queue(process_id: &str) -> String {
    format!("{}:{}", PROCESSING_QUEUE_PREFIX, process_id)
}

fn current_processing_queue() -> String {
    format!("{}:{}", PROCESSING_QUEUE_PREFIX, current_process_id())
}

fn current_process_id() -> String {
    let hostname = gethostname::gethostname()
        .into_string()
        .unwrap_or_else(|_| "unknown".to_string());
    let pid = std::process::id();
    format!("{}-{}", hostname, pid)
}
