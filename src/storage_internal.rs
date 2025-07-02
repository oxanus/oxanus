use chrono::{DateTime, Utc};
use deadpool_redis::redis::{self, AsyncCommands};
use std::{
    collections::{HashMap, HashSet},
    num::NonZero,
};
use tokio_util::sync::CancellationToken;

use crate::{
    OxanusError,
    job_envelope::{JobEnvelope, JobId},
    result_collector::JobResultKind,
    worker_registry::CronJob,
};

// const JOB_EXPIRE_TIME: i64 = 7 * 24 * 3600; // 7 days
const RESURRECT_THRESHOLD_SECS: i64 = 5;

#[derive(Clone)]
pub(crate) struct StorageInternal {
    pool: deadpool_redis::Pool,
    keys: StorageKeys,
}

#[derive(Clone)]
struct StorageKeys {
    namespace: String,
    jobs: String,
    dead: String,
    schedule: String,
    retry: String,
    processing_queue_prefix: String,
    processes: String,
    stats: String,
}

#[derive(Debug, Clone)]
pub struct QueueStats {
    pub queue_key: String,
    pub processed: i64,
    pub succeeded: i64,
    pub panicked: i64,
    pub failed: i64,
    pub latency: f64,
}

impl StorageKeys {
    pub fn new(namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        let namespace = if namespace.is_empty() {
            "oxanus".to_string()
        } else {
            format!("oxanus:{namespace}")
        };

        Self {
            jobs: format!("{namespace}:jobs"),
            dead: format!("{namespace}:dead"),
            schedule: format!("{namespace}:schedule"),
            retry: format!("{namespace}:retry"),
            processing_queue_prefix: format!("{namespace}:processing:"),
            processes: format!("{namespace}:processes"),
            stats: format!("{namespace}:stats"),
            namespace,
        }
    }
}

impl StorageInternal {
    pub fn new(pool: deadpool_redis::Pool, namespace: Option<String>) -> Self {
        Self {
            pool,
            keys: StorageKeys::new(namespace.unwrap_or_default()),
        }
    }

    pub fn namespace(&self) -> &str {
        &self.keys.namespace
    }

    pub async fn pool(&self) -> Result<deadpool_redis::Pool, OxanusError> {
        Ok(self.pool.clone())
    }

    pub async fn connection(&self) -> Result<deadpool_redis::Connection, OxanusError> {
        self.pool
            .get()
            .await
            .map_err(OxanusError::DeadpoolRedisPoolError)
    }

    pub async fn queues(&self, pattern: &str) -> Result<HashSet<String>, OxanusError> {
        let mut conn = self.connection().await?;
        let keys: Vec<String> = (*conn).keys(self.namespace_queue(pattern)).await?;
        Ok(keys.into_iter().collect())
    }

    pub async fn enqueue(&self, envelope: JobEnvelope) -> Result<JobId, OxanusError> {
        let mut redis = self.connection().await?;

        if self.should_skip_job(&mut redis, &envelope).await? {
            tracing::warn!("Unique job {} already exists, skipping", envelope.id);
            return Ok(envelope.id);
        }

        let _: () = deadpool_redis::redis::pipe()
            .hset(
                &self.keys.jobs,
                &envelope.id,
                serde_json::to_string(&envelope)?,
            )
            // .hexpire(
            //     &self.keys.jobs,
            //     JOB_EXPIRE_TIME,
            //     deadpool_redis::redis::ExpireOption::NONE,
            //     &envelope.id,
            // )
            .lpush(self.namespace_queue(&envelope.queue), &envelope.id)
            .query_async(&mut *redis)
            .await?;

        Ok(envelope.id)
    }

    async fn should_skip_job(
        &self,
        redis: &mut deadpool_redis::Connection,
        envelope: &JobEnvelope,
    ) -> Result<bool, OxanusError> {
        if !envelope.meta.unique {
            return Ok(false);
        }

        let exists: bool = redis.hexists(&self.keys.jobs, &envelope.id).await?;
        Ok(exists)
    }

    pub async fn enqueue_in(
        &self,
        envelope: JobEnvelope,
        delay_s: u64,
    ) -> Result<JobId, OxanusError> {
        if delay_s == 0 {
            self.enqueue(envelope).await
        } else {
            let time = chrono::Utc::now() + chrono::Duration::seconds(delay_s as i64);
            self.enqueue_at(envelope, time).await
        }
    }

    pub async fn enqueue_at(
        &self,
        envelope: JobEnvelope,
        time: DateTime<Utc>,
    ) -> Result<JobId, OxanusError> {
        let mut redis = self.connection().await?;

        if self.should_skip_job(&mut redis, &envelope).await? {
            tracing::warn!("Unique job {} already exists, skipping", envelope.id);
            return Ok(envelope.id);
        }

        let _: () = redis::pipe()
            .hset(
                &self.keys.jobs,
                &envelope.id,
                serde_json::to_string(&envelope)?,
            )
            // .hexpire(
            //     &self.keys.jobs,
            //     JOB_EXPIRE_TIME,
            //     redis::ExpireOption::NONE,
            //     &envelope.id,
            // )
            .zadd(&self.keys.schedule, &envelope.id, time.timestamp_micros())
            .query_async(&mut redis)
            .await?;

        Ok(envelope.id)
    }

    pub async fn retry_in(&self, job_id: JobId, delay_s: u64) -> Result<(), OxanusError> {
        let updated_envelope = self
            .get(&job_id)
            .await?
            .ok_or(OxanusError::JobNotFound)?
            .with_retries_incremented();

        if delay_s == 0 {
            self.enqueue(updated_envelope).await?;
            return Ok(());
        }

        let now = chrono::Utc::now().timestamp_micros() as u64;

        let mut redis = self.connection().await?;
        let _: () = redis::pipe()
            .hset(
                &self.keys.jobs,
                &updated_envelope.id,
                serde_json::to_string(&updated_envelope)?,
            )
            // .hexpire(
            //     &self.keys.jobs,
            //     JOB_EXPIRE_TIME,
            //     redis::ExpireOption::NONE,
            //     &updated_envelope.id,
            // )
            .zadd(
                &self.keys.retry,
                updated_envelope.id,
                now + delay_s * 1_000_000,
            )
            .query_async(&mut redis)
            .await?;
        Ok(())
    }

    pub async fn blocking_dequeue(
        &self,
        queue: &str,
        timeout: f64,
    ) -> Result<Option<JobId>, OxanusError> {
        let mut redis = self.connection().await?;
        let job_id: Option<String> = redis
            .blmove(
                self.namespace_queue(queue),
                self.current_processing_queue(),
                redis::Direction::Right,
                redis::Direction::Left,
                timeout,
            )
            .await?;
        Ok(job_id)
    }

    pub async fn dequeue(&self, queue: &str) -> Result<Option<JobId>, OxanusError> {
        let mut redis = self.connection().await?;
        let job_id: Option<JobId> = redis
            .lmove(
                self.namespace_queue(queue),
                self.current_processing_queue(),
                redis::Direction::Right,
                redis::Direction::Left,
            )
            .await?;
        Ok(job_id)
    }

    pub async fn get(&self, id: &JobId) -> Result<Option<JobEnvelope>, OxanusError> {
        let mut redis = self.connection().await?;
        let envelope: Option<String> = redis.hget(&self.keys.jobs, id).await?;
        match envelope {
            Some(envelope) => Ok(Some(serde_json::from_str(&envelope)?)),
            None => Ok(None),
        }
    }

    pub async fn update(&self, envelope: &JobEnvelope) -> Result<(), OxanusError> {
        let mut redis = self.connection().await?;
        let _: () = redis::pipe()
            .hset(
                &self.keys.jobs,
                &envelope.id,
                serde_json::to_string(envelope)?,
            )
            .query_async(&mut redis)
            .await?;
        Ok(())
    }

    pub async fn update_state(
        &self,
        id: &JobId,
        state: serde_json::Value,
    ) -> Result<Option<JobEnvelope>, OxanusError> {
        let mut envelope = match self.get(id).await? {
            Some(envelope) => envelope,
            None => return Ok(None),
        };
        envelope.meta.state = Some(state);
        self.update(&envelope).await?;
        Ok(Some(envelope))
    }

    pub async fn delete(&self, id: &JobId) -> Result<(), OxanusError> {
        let mut redis = self.connection().await?;
        let _: () = redis::pipe()
            .hdel(&self.keys.jobs, id)
            .lrem(self.current_processing_queue(), 1, id)
            .query_async(&mut redis)
            .await?;
        Ok(())
    }

    pub async fn get_many(&self, ids: &[JobId]) -> Result<Vec<JobEnvelope>, OxanusError> {
        let mut redis = self.connection().await?;
        let mut cmd = redis::cmd("HMGET");
        cmd.arg(&self.keys.jobs);
        cmd.arg(ids);
        let envelopes_str: Vec<String> = cmd.query_async(&mut redis).await?;
        let mut envelopes: Vec<JobEnvelope> = vec![];
        for envelope_str in envelopes_str {
            envelopes.push(serde_json::from_str(&envelope_str)?);
        }
        Ok(envelopes)
    }

    pub async fn kill(&self, envelope: &JobEnvelope) -> Result<(), OxanusError> {
        let mut redis = self.connection().await?;
        let _: () = redis::pipe()
            .lrem(self.current_processing_queue(), 1, &envelope.id)
            .hdel(&self.keys.jobs, &envelope.id)
            .lpush(&self.keys.dead, &serde_json::to_string(envelope)?)
            .ltrim(&self.keys.dead, 0, 1000)
            .query_async(&mut redis)
            .await?;
        Ok(())
    }

    pub async fn finish_with_success(&self, envelope: &JobEnvelope) -> Result<(), OxanusError> {
        let mut redis = self.connection().await?;
        let _: () = redis::pipe()
            .hdel(&self.keys.jobs, &envelope.id)
            .lrem(self.current_processing_queue(), 1, &envelope.id)
            .query_async(&mut redis)
            .await?;
        Ok(())
    }

    pub async fn finish_with_failure(&self, envelope: &JobEnvelope) -> Result<(), OxanusError> {
        let mut redis = self.connection().await?;
        let _: () = redis::pipe()
            .lrem(self.current_processing_queue(), 1, &envelope.id)
            .query_async(&mut redis)
            .await?;
        Ok(())
    }

    pub async fn enqueue_scheduled(
        &self,
        redis: &mut deadpool_redis::Connection,
        schedule_queue: &str,
    ) -> Result<usize, OxanusError> {
        let now = chrono::Utc::now().timestamp_micros();
        let job_ids: Vec<String> = (*redis).zrangebyscore(schedule_queue, 0, now).await?;

        if job_ids.is_empty() {
            return Ok(0);
        }

        let envelopes = self.get_many(&job_ids).await?;
        let mut map: HashMap<&str, Vec<&JobEnvelope>> = HashMap::new();
        let envelopes_count = envelopes.len();

        for envelope in envelopes.iter() {
            map.entry(&envelope.queue).or_default().push(envelope);
        }

        for (queue, envelopes) in map {
            let job_ids: Vec<&str> = envelopes
                .iter()
                .map(|envelope| envelope.id.as_str())
                .collect();

            let _: i32 = redis.lpush(self.namespace_queue(queue), job_ids).await?;
        }

        let _: i32 = redis.zrembyscore(schedule_queue, 0, now).await?;

        Ok(envelopes_count)
    }

    pub async fn enqueued_count(&self, queue: &str) -> Result<usize, OxanusError> {
        let mut redis = self.connection().await?;
        let count: i64 = (*redis).llen(self.namespace_queue(queue)).await?;
        Ok(count as usize)
    }

    pub async fn latency_ms(&self, queue: &str) -> Result<f64, OxanusError> {
        self.latency_micros(queue)
            .await
            .map(|latency| latency / 1_000.0)
    }

    pub async fn latency_micros(&self, queue: &str) -> Result<f64, OxanusError> {
        let mut redis = self.connection().await?;
        let result: Vec<String> = (*redis).lrange(self.namespace_queue(queue), 0, 0).await?;
        match result.first() {
            Some(job_id) => {
                let envelope = self.get(job_id).await?;
                Ok(envelope.map_or(0.0, |envelope| {
                    let now = chrono::Utc::now().timestamp_micros() as u64;
                    (now - envelope.meta.created_at) as f64
                }))
            }
            None => Ok(0.0),
        }
    }

    pub async fn dead_count(&self) -> Result<usize, OxanusError> {
        let mut redis = self.connection().await?;
        let count: i64 = (*redis).llen(&self.keys.dead).await?;
        Ok(count as usize)
    }

    pub async fn retries_count(&self) -> Result<usize, OxanusError> {
        let mut redis = self.connection().await?;
        let count: i64 = (*redis).zcard(&self.keys.retry).await?;
        Ok(count as usize)
    }

    pub async fn scheduled_count(&self) -> Result<usize, OxanusError> {
        let mut redis = self.connection().await?;
        let count: i64 = (*redis).zcard(&self.keys.schedule).await?;
        Ok(count as usize)
    }

    pub async fn jobs_count(&self) -> Result<usize, OxanusError> {
        let mut redis = self.connection().await?;
        let count: i64 = (*redis).hlen(&self.keys.jobs).await?;
        Ok(count as usize)
    }

    pub async fn stats(&self) -> Result<Vec<QueueStats>, OxanusError> {
        let mut redis = self.connection().await?;
        let list: HashMap<String, i64> = (*redis).hgetall(&self.keys.stats).await?;

        let mut map = HashMap::new();

        for (key, value) in list {
            let parts: Vec<&str> = key.rsplitn(2, ':').collect();
            let mut iter = parts.into_iter();
            let stat_key = iter.next();
            let queue_key = iter.next();

            if let (Some(queue_key), Some(stat_key)) = (queue_key, stat_key) {
                let queue_stats =
                    map.entry(queue_key.to_string())
                        .or_insert_with(|| QueueStats {
                            queue_key: queue_key.to_string(),
                            processed: 0,
                            succeeded: 0,
                            panicked: 0,
                            failed: 0,
                            latency: 0.0,
                        });

                match stat_key {
                    "processed" => queue_stats.processed = value,
                    "succeeded" => queue_stats.succeeded = value,
                    "panicked" => queue_stats.panicked = value,
                    "failed" => queue_stats.failed = value,
                    _ => {}
                }
            }
        }

        let mut values: Vec<QueueStats> = map.into_values().collect();

        for value in values.iter_mut() {
            value.latency = self.latency_ms(&value.queue_key).await?;
        }

        values.sort_by(|a, b| a.queue_key.cmp(&b.queue_key));

        Ok(values)
    }

    pub async fn update_stats(
        &self,
        queue_key: &str,
        kind: JobResultKind,
    ) -> Result<(), OxanusError> {
        let mut redis = self.connection().await?;

        let processed_key = format!("{queue_key}:processed");
        let status_key = match kind {
            JobResultKind::Success => format!("{queue_key}:succeeded"),
            JobResultKind::Panicked => format!("{queue_key}:panicked"),
            JobResultKind::Failed => format!("{queue_key}:failed"),
        };

        let _: () = redis::pipe()
            .hincr(&self.keys.stats, processed_key, 1)
            .hincr(&self.keys.stats, status_key, 1)
            .query_async(&mut redis)
            .await?;

        Ok(())
    }

    pub async fn retry_loop(&self, cancel_token: CancellationToken) -> Result<(), OxanusError> {
        tracing::info!("Starting retry loop");

        let mut redis = self.connection().await?;

        loop {
            if cancel_token.is_cancelled() {
                return Ok(());
            }

            self.enqueue_scheduled(&mut redis, &self.keys.retry).await?;
            tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        }
    }

    pub async fn schedule_loop(&self, cancel_token: CancellationToken) -> Result<(), OxanusError> {
        tracing::info!("Starting schedule loop");

        let mut redis = self.connection().await?;

        loop {
            if cancel_token.is_cancelled() {
                return Ok(());
            }

            self.enqueue_scheduled(&mut redis, &self.keys.schedule)
                .await?;
            tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        }
    }

    pub async fn ping_loop(&self, cancel_token: CancellationToken) -> Result<(), OxanusError> {
        let mut redis = self.connection().await?;

        loop {
            if cancel_token.is_cancelled() {
                return Ok(());
            }

            self.ping(&mut redis).await?;
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    pub async fn ping(&self, redis: &mut deadpool_redis::Connection) -> Result<(), OxanusError> {
        let _: () = (*redis)
            .zadd(
                &self.keys.processes,
                self.current_process_id(),
                chrono::Utc::now().timestamp(),
            )
            .await?;
        Ok(())
    }

    pub async fn resurrect_loop(&self, cancel_token: CancellationToken) -> Result<(), OxanusError> {
        tracing::info!("Starting resurrect loop");

        let mut redis = self.connection().await?;

        loop {
            if cancel_token.is_cancelled() {
                return Ok(());
            }

            self.resurrect(&mut redis).await?;
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        }
    }

    pub async fn cron_job_loop(
        &self,
        cancel_token: CancellationToken,
        job_name: String,
        cron_job: CronJob,
    ) -> Result<(), OxanusError> {
        let iterator = cron_job
            .schedule
            .after(&(chrono::Utc::now() + chrono::Duration::seconds(3)));

        let mut previous: Option<chrono::DateTime<chrono::Utc>> = None;

        for next in iterator {
            loop {
                if cancel_token.is_cancelled() {
                    return Ok(());
                }

                let now = chrono::Utc::now();
                let max_schedule_time = now + chrono::Duration::minutes(30);

                if next > max_schedule_time {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }

                if let Some(previous) = previous {
                    if previous > now {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        continue;
                    }
                }

                break;
            }

            let job_id = format!("{}-{}", job_name, next.timestamp_micros());
            let envelope =
                JobEnvelope::new_cron(cron_job.queue_key.clone(), job_id, job_name.clone())?;

            self.enqueue_at(envelope, next).await?;

            previous = Some(next);
        }

        Ok(())
    }

    pub async fn resurrect(
        &self,
        redis: &mut deadpool_redis::Connection,
    ) -> Result<(), OxanusError> {
        let processes: Vec<String> = (*redis)
            .zrangebyscore(
                &self.keys.processes,
                0,
                chrono::Utc::now().timestamp() - RESURRECT_THRESHOLD_SECS,
            )
            .await?;

        for process_id in processes {
            tracing::info!("Dead process detected: {}", process_id);

            let processing_queue = self.processing_queue(&process_id);

            loop {
                let job_ids: Vec<String> = (*redis)
                    .lpop(&processing_queue, Some(NonZero::new(10).unwrap()))
                    .await?;

                if job_ids.is_empty() {
                    break;
                }

                for job_id in job_ids {
                    match self.get(&job_id).await? {
                        Some(envelope) => {
                            tracing::info!(
                                job_id = job_id,
                                queue = envelope.queue,
                                job = envelope.job.name,
                                "Resurrecting job"
                            );
                            self.enqueue(envelope).await?;
                            let _: () = (*redis).lrem(&processing_queue, 1, &job_id).await?;
                        }
                        None => tracing::warn!("Job {} not found", job_id),
                    }
                }
            }

            let _: () = (*redis).zrem(&self.keys.processes, &process_id).await?;
        }

        Ok(())
    }

    fn processing_queue(&self, process_id: &str) -> String {
        format!("{}:{}", self.keys.processing_queue_prefix, process_id)
    }

    fn current_processing_queue(&self) -> String {
        format!(
            "{}:{}",
            self.keys.processing_queue_prefix,
            self.current_process_id()
        )
    }

    fn current_process_id(&self) -> String {
        let hostname = gethostname::gethostname()
            .into_string()
            .unwrap_or_else(|_| "unknown".to_string());
        let pid = std::process::id();
        format!("{}-{}", hostname, pid)
    }

    fn namespace_queue(&self, queue: &str) -> String {
        if queue.starts_with(self.keys.namespace.as_str()) {
            queue.to_string()
        } else {
            format!("{}:{}", self.keys.namespace, queue)
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::Serialize;
    use testresult::TestResult;

    use super::*;
    use crate::test_helper::{random_string, redis_pool};

    #[derive(Serialize)]
    struct TestWorker {}

    #[async_trait::async_trait]
    impl crate::Worker for TestWorker {
        type Context = ();
        type Error = std::io::Error;

        async fn process(&self, _: &crate::Context<Self::Context>) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_latency() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();

        let mut envelope = JobEnvelope::new(queue.clone(), TestWorker {})?;
        let now = chrono::Utc::now();
        let actual_latency = 777;
        envelope.meta.created_at = now.timestamp_micros() as u64 - actual_latency * 1_000;
        storage.enqueue(envelope).await?;

        let latency = storage.latency_ms(&queue).await?;

        assert!((latency - actual_latency as f64).abs() < 5.0);

        Ok(())
    }
}
