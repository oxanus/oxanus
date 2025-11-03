use chrono::{DateTime, Utc};
use deadpool_redis::redis::{self, AsyncCommands};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    num::NonZero,
};
use tokio_util::sync::CancellationToken;

use crate::{
    OxanusError,
    job_envelope::{JobEnvelope, JobId},
    result_collector::{JobResult, JobResultKind},
    worker_registry::CronJob,
};

const JOB_EXPIRE_TIME: i64 = 7 * 24 * 3600; // 7 days
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
    queue_prefix: String,
    processing_queue_prefix: String,
    processes: String,
    processes_data: String,
    stats: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Stats {
    pub global: StatsGlobal,
    pub processes: Vec<Process>,
    pub processing: Vec<StatsProcessing>,
    pub queues: Vec<QueueStats>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatsGlobal {
    pub jobs: usize,
    pub enqueued: usize,
    pub processed: i64,
    pub dead: usize,
    pub scheduled: usize,
    pub retries: usize,
    pub latency_s_max: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatsProcessing {
    pub process_id: String,
    pub job_envelope: JobEnvelope,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueStats {
    pub key: String,

    pub enqueued: usize,
    pub processed: i64,
    pub succeeded: i64,
    pub panicked: i64,
    pub failed: i64,
    pub latency_s: f64,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub queues: Vec<DynamicQueueStats>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DynamicQueueStats {
    pub suffix: String,

    pub enqueued: usize,
    pub processed: i64,
    pub succeeded: i64,
    pub panicked: i64,
    pub failed: i64,
    pub latency_s: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Process {
    pub hostname: String,
    pub pid: u32,
    pub heartbeat_at: i64,
}

impl Process {
    pub fn id(&self) -> String {
        format!("{}-{}", self.hostname, self.pid)
    }
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
            queue_prefix: format!("{namespace}:queue"),
            processing_queue_prefix: format!("{namespace}:processing"),
            processes: format!("{namespace}:processes"),
            processes_data: format!("{namespace}:processes_data"),
            stats: format!("{namespace}:stats"),
            namespace,
        }
    }
}

impl StorageInternal {
    pub fn new(pool: deadpool_redis::Pool, namespace: Option<String>) -> Self {
        let keys = StorageKeys::new(namespace.unwrap_or_default());
        Self { pool, keys }
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

    pub async fn queue_keys(&self, pattern: &str) -> Result<HashSet<String>, OxanusError> {
        let mut conn = self.connection().await?;
        let keys: Vec<String> = (*conn).keys(self.namespace_queue(pattern)).await?;
        Ok(keys.into_iter().collect())
    }

    pub async fn queues(&self, pattern: &str) -> Result<Vec<String>, OxanusError> {
        let queue_keys = self.queue_keys(pattern).await?;
        // remove namespace prefix from beginning of each key
        let queues = queue_keys
            .into_iter()
            .map(|key| key.replace(&format!("{}:", &self.keys.queue_prefix), ""))
            .collect();
        Ok(queues)
    }

    pub async fn enqueue(&self, envelope: JobEnvelope) -> Result<JobId, OxanusError> {
        let mut redis = self.connection().await?;
        self.enqueue_w_conn(&mut redis, envelope).await
    }

    async fn enqueue_w_conn(
        &self,
        redis: &mut deadpool_redis::Connection,
        envelope: JobEnvelope,
    ) -> Result<JobId, OxanusError> {
        if self.should_skip_job(redis, &envelope).await? {
            tracing::warn!("Unique job {} already exists, skipping", envelope.id);
            return Ok(envelope.id);
        }

        let _: () = deadpool_redis::redis::pipe()
            .hset(
                &self.keys.jobs,
                &envelope.id,
                serde_json::to_string(&envelope)?,
            )
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
            .zadd(&self.keys.schedule, &envelope.id, time.timestamp_micros())
            .query_async(&mut redis)
            .await?;

        Ok(envelope.id)
    }

    pub async fn retry_in(&self, job_id: JobId, delay_s: u64) -> Result<(), OxanusError> {
        let updated_envelope = self
            .get_job(&job_id)
            .await?
            .ok_or(OxanusError::JobNotFound)?
            .with_retries_incremented();

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

    pub async fn get_job(&self, id: &JobId) -> Result<Option<JobEnvelope>, OxanusError> {
        let mut redis = self.connection().await?;
        self.get_job_w_conn(&mut redis, id).await
    }

    async fn get_job_w_conn(
        &self,
        redis: &mut deadpool_redis::Connection,
        id: &JobId,
    ) -> Result<Option<JobEnvelope>, OxanusError> {
        let envelope: Option<String> = redis.hget(&self.keys.jobs, id).await?;
        match envelope {
            Some(envelope) => Ok(Some(serde_json::from_str(&envelope)?)),
            None => Ok(None),
        }
    }

    pub async fn update_job(&self, envelope: &JobEnvelope) -> Result<(), OxanusError> {
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
        let mut envelope = match self.get_job(id).await? {
            Some(envelope) => envelope,
            None => return Ok(None),
        };
        envelope.meta.state = Some(state);
        self.update_job(&envelope).await?;
        Ok(Some(envelope))
    }

    pub async fn delete_job(&self, id: &JobId) -> Result<(), OxanusError> {
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
        let envelopes_str: Vec<Option<String>> = cmd.query_async(&mut redis).await?;
        let mut envelopes: Vec<JobEnvelope> = vec![];
        for envelope_str in envelopes_str.into_iter().flatten() {
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

    pub async fn enqueue_scheduled(&self, schedule_queue: &str) -> Result<usize, OxanusError> {
        let now = chrono::Utc::now().timestamp_micros();
        let mut redis = self.connection().await?;
        let job_ids: Vec<String> = redis.zrangebyscore(schedule_queue, 0, now).await?;

        if job_ids.is_empty() {
            return Ok(0);
        }

        let envelopes = self.get_many(&job_ids).await?;
        let mut map: HashMap<&str, Vec<&JobEnvelope>> = HashMap::new();
        let envelopes_count = envelopes.len();

        for envelope in envelopes.iter() {
            map.entry(&envelope.queue).or_default().push(envelope);
        }

        let mut pipe = redis::pipe();

        for (queue, envelopes) in map {
            let job_ids: Vec<&str> = envelopes
                .iter()
                .map(|envelope| envelope.id.as_str())
                .collect();

            pipe.lpush(self.namespace_queue(queue), job_ids);
        }

        pipe.zrembyscore(schedule_queue, 0, now);

        let _: () = pipe.query_async(&mut redis).await?;

        Ok(envelopes_count)
    }

    pub async fn enqueued_count(&self, queue: &str) -> Result<usize, OxanusError> {
        let mut redis = self.connection().await?;
        self.enqueued_count_w_conn(&mut redis, queue).await
    }

    async fn enqueued_count_w_conn(
        &self,
        redis: &mut deadpool_redis::Connection,
        queue: &str,
    ) -> Result<usize, OxanusError> {
        let count: i64 = (*redis).llen(self.namespace_queue(queue)).await?;
        Ok(count as usize)
    }

    async fn latency_s_w_conn(
        &self,
        redis: &mut deadpool_redis::Connection,
        queue: &str,
    ) -> Result<f64, OxanusError> {
        self.latency_micros_w_conn(redis, queue)
            .await
            .map(|latency| latency / 1_000_000.0)
    }

    pub async fn latency_ms(&self, queue: &str) -> Result<f64, OxanusError> {
        self.latency_micros(queue)
            .await
            .map(|latency| latency / 1_000.0)
    }

    pub async fn latency_micros(&self, queue: &str) -> Result<f64, OxanusError> {
        let mut redis = self.connection().await?;
        self.latency_micros_w_conn(&mut redis, queue).await
    }

    async fn latency_micros_w_conn(
        &self,
        redis: &mut deadpool_redis::Connection,
        queue: &str,
    ) -> Result<f64, OxanusError> {
        let result: Vec<String> = (*redis).lrange(self.namespace_queue(queue), 0, 0).await?;
        match result.first() {
            Some(job_id) => {
                let envelope = self.get_job_w_conn(redis, job_id).await?;
                Ok(envelope.map_or(0.0, |envelope| {
                    let now = chrono::Utc::now().timestamp_micros();
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

    pub async fn stats(&self) -> Result<Stats, OxanusError> {
        let mut redis = self.connection().await?;
        let list: HashMap<String, i64> = (*redis).hgetall(&self.keys.stats).await?;

        let mut map = HashMap::new();
        let mut queue_values = Vec::new();

        for queue in self.queues("*").await? {
            queue_values.push((queue, "processed".to_string(), 0));
        }

        for (key, value) in list {
            let parts: Vec<&str> = key.rsplitn(2, ':').collect();
            let mut parts_iter = parts.into_iter();
            let stat_key = match parts_iter.next() {
                Some(stat_key) => stat_key,
                None => continue,
            };
            let queue_full_key = match parts_iter.next() {
                Some(queue_key) => queue_key,
                None => continue,
            };

            queue_values.push((queue_full_key.to_string(), stat_key.to_string(), value));
        }

        for (queue_full_key, stat_key, value) in queue_values {
            let queue_key_parts: Vec<&str> = queue_full_key.splitn(2, '#').collect();
            let mut queue_key_parts_iter = queue_key_parts.into_iter();

            let queue_key = match queue_key_parts_iter.next() {
                Some(queue_key) => queue_key,
                None => continue,
            };

            let queue_dynamic_key = queue_key_parts_iter.next();

            let queue_stats = map
                .entry(queue_key.to_string())
                .or_insert_with(|| QueueStats {
                    key: queue_key.to_string(),
                    enqueued: 0,
                    processed: 0,
                    succeeded: 0,
                    panicked: 0,
                    failed: 0,
                    latency_s: 0.0,
                    queues: vec![],
                });

            if let Some(queue_dynamic_key) = queue_dynamic_key {
                if !queue_stats
                    .queues
                    .iter_mut()
                    .any(|q| q.suffix == queue_dynamic_key)
                {
                    queue_stats.queues.push(DynamicQueueStats {
                        suffix: queue_dynamic_key.to_string(),
                        enqueued: 0,
                        processed: 0,
                        succeeded: 0,
                        panicked: 0,
                        failed: 0,
                        latency_s: 0.0,
                    });
                }

                if let Some(existing) = queue_stats
                    .queues
                    .iter_mut()
                    .find(|q| q.suffix == queue_dynamic_key)
                {
                    match stat_key.as_str() {
                        "processed" => existing.processed += value,
                        "succeeded" => existing.succeeded += value,
                        "panicked" => existing.panicked += value,
                        "failed" => existing.failed += value,
                        _ => {}
                    }
                }
            }

            match stat_key.as_str() {
                "processed" => queue_stats.processed += value,
                "succeeded" => queue_stats.succeeded += value,
                "panicked" => queue_stats.panicked += value,
                "failed" => queue_stats.failed += value,
                _ => {}
            }
        }

        let mut values: Vec<QueueStats> = map.into_values().collect();

        let mut processed_count_total = 0;
        let mut enqueued_count_total = 0;
        let mut latency_s_max = 0.0;

        for value in values.iter_mut() {
            if value.queues.is_empty() {
                value.enqueued = self.enqueued_count_w_conn(&mut redis, &value.key).await?;
                value.latency_s = self.latency_s_w_conn(&mut redis, &value.key).await?;
                if value.latency_s > latency_s_max {
                    latency_s_max = value.latency_s;
                }
            } else {
                for dynamic_queue in value.queues.iter_mut() {
                    let dynamic_queue_key = format!("{}#{}", value.key, dynamic_queue.suffix);
                    let enqueued = self
                        .enqueued_count_w_conn(&mut redis, &dynamic_queue_key)
                        .await?;
                    let latency_s = self
                        .latency_s_w_conn(&mut redis, &dynamic_queue_key)
                        .await?;

                    dynamic_queue.enqueued = enqueued;
                    dynamic_queue.latency_s = latency_s;

                    if value.latency_s < latency_s {
                        value.latency_s = latency_s;
                    }
                    if dynamic_queue.latency_s > latency_s_max {
                        latency_s_max = dynamic_queue.latency_s;
                    }
                    value.enqueued += enqueued;
                }
            }

            processed_count_total += value.processed;
            enqueued_count_total += value.enqueued;

            value.queues.sort_by(|a, b| a.suffix.cmp(&b.suffix));
        }

        values.sort_by(|a, b| a.key.cmp(&b.key));

        let processes = self.processes().await?;

        let mut processing = vec![];

        for process in processes.iter() {
            let processing_queue = self.processing_queue(&process.id());
            let job_ids: Vec<String> = (*redis).lrange(&processing_queue, 0, -1).await?;

            for job_id in job_ids {
                if let Some(envelope) = self.get_job(&job_id).await? {
                    processing.push(StatsProcessing {
                        process_id: process.id(),
                        job_envelope: envelope,
                    });
                }
            }
        }

        Ok(Stats {
            global: StatsGlobal {
                jobs: self.jobs_count().await?,
                enqueued: enqueued_count_total,
                processed: processed_count_total,
                dead: self.dead_count().await?,
                scheduled: self.scheduled_count().await?,
                retries: self.retries_count().await?,
                latency_s_max,
            },
            processing,
            processes,
            queues: values,
        })
    }

    pub async fn update_stats(&self, result: JobResult) -> Result<(), OxanusError> {
        let mut redis = self.connection().await?;
        let queue = result.envelope.queue.clone();

        let processed_key = format!("{queue}:processed");
        let status_key = match result.kind {
            JobResultKind::Success => format!("{queue}:succeeded"),
            JobResultKind::Panicked => format!("{queue}:panicked"),
            JobResultKind::Failed => format!("{queue}:failed"),
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

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    return Ok(());
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(300)) => {
                    self.enqueue_scheduled(&self.keys.retry).await?;
                }
            }
        }
    }

    pub async fn schedule_loop(&self, cancel_token: CancellationToken) -> Result<(), OxanusError> {
        tracing::info!("Starting schedule loop");

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    return Ok(());
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(300)) => {
                    self.enqueue_scheduled(&self.keys.schedule)
                        .await?;
                }
            }
        }
    }

    pub async fn cleanup_loop(&self, cancel_token: CancellationToken) -> Result<(), OxanusError> {
        tracing::info!("Starting cleanup loop");

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    return Ok(());
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(600)) => {
                    self.cleanup().await?;
                }
            }
        }
    }

    pub async fn ping_loop(&self, cancel_token: CancellationToken) -> Result<(), OxanusError> {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    return Ok(());
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(500)) => {
                    self.ping().await?;
                }
            }
        }
    }

    pub async fn ping(&self) -> Result<(), OxanusError> {
        let mut redis = self.connection().await?;
        let process = self.current_process();
        let _: () = redis::pipe()
            .zadd(
                &self.keys.processes,
                process.id(),
                chrono::Utc::now().timestamp(),
            )
            .hset(
                &self.keys.processes_data,
                process.id(),
                serde_json::to_string(&process)?,
            )
            .query_async(&mut redis)
            .await?;
        Ok(())
    }

    pub async fn cleanup(&self) -> Result<usize, OxanusError> {
        let mut redis = self.connection().await?;

        let job_ids_to_clean = {
            let mut iter: redis::AsyncIter<'_, (JobId, String)> =
                redis.hscan(&self.keys.jobs).await?;
            let now = chrono::Utc::now().timestamp();

            let mut job_ids = vec![];

            while let Some(item) = iter.next_item().await {
                let (job_id, job_str) = item?;

                let parsed: Result<JobEnvelope, _> = serde_json::from_str(&job_str);

                match parsed {
                    Ok(job_envelope) => {
                        if job_envelope.meta.created_at_secs() < now - JOB_EXPIRE_TIME {
                            job_ids.push(job_id);
                        }
                    }
                    Err(_) => job_ids.push(job_id),
                }
            }

            job_ids
        };

        let count = job_ids_to_clean.len();
        if count > 0 {
            tracing::info!("Cleaning up {count} expired jobs");
            let () = redis.hdel(&self.keys.jobs, job_ids_to_clean).await?;
        }

        Ok(count)
    }

    pub async fn get_process_data(&self, id: &str) -> Result<Option<Process>, OxanusError> {
        let mut redis = self.connection().await?;
        let process_str: Option<String> = (*redis).hget(&self.keys.processes_data, id).await?;
        match process_str {
            Some(process_str) => Ok(Some(serde_json::from_str(&process_str)?)),
            None => Ok(None),
        }
    }

    pub async fn processes(&self) -> Result<Vec<Process>, OxanusError> {
        let mut redis = self.connection().await?;
        let process_ids: Vec<String> = (*redis)
            .zrangebyscore(
                &self.keys.processes,
                chrono::Utc::now().timestamp() - RESURRECT_THRESHOLD_SECS,
                chrono::Utc::now().timestamp(),
            )
            .await?;

        let mut processes = vec![];

        for process_id in process_ids {
            if let Some(process) = self.get_process_data(&process_id).await? {
                processes.push(process);
            }
        }

        Ok(processes)
    }

    pub async fn resurrect_loop(&self, cancel_token: CancellationToken) -> Result<(), OxanusError> {
        tracing::info!("Starting resurrect loop");

        self.ping().await?;

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    return Ok(());
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(2)) => {
                    self.resurrect().await?;
                }
            }
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

                if let Some(previous) = previous
                    && previous > now
                {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }

                break;
            }

            let scheduled_at = next.timestamp_micros();
            let job_id = format!("{}-{}", job_name, scheduled_at);
            let envelope = JobEnvelope::new_cron(
                cron_job.queue_key.clone(),
                job_id,
                job_name.clone(),
                scheduled_at,
            )?;

            self.enqueue_at(envelope, next).await?;

            previous = Some(next);
        }

        Ok(())
    }

    pub async fn resurrect(&self) -> Result<(), OxanusError> {
        let mut redis = self.connection().await?;
        for process_id in self.dead_process_ids(&mut redis).await? {
            tracing::info!("Dead process detected: {}", process_id);

            let processing_queue = self.processing_queue(&process_id);
            let mut resurrected_count = 0;

            loop {
                let job_ids: Vec<String> = (*redis)
                    .lpop(&processing_queue, Some(NonZero::new(10).unwrap()))
                    .await?;

                if job_ids.is_empty() {
                    break;
                }

                resurrected_count += job_ids.len();

                for job_id in job_ids {
                    match self.get_job_w_conn(&mut redis, &job_id).await? {
                        Some(envelope) => {
                            tracing::info!(
                                job_id = job_id,
                                queue = envelope.queue,
                                worker = envelope.job.name,
                                "Resurrecting job"
                            );
                            self.enqueue_w_conn(&mut redis, envelope).await?;
                            let _: () = (*redis).lrem(&processing_queue, 1, &job_id).await?;
                        }
                        None => tracing::warn!("Job {} not found", job_id),
                    }
                }
            }

            let _: () = redis::pipe()
                .zrem(&self.keys.processes, &process_id)
                .hdel(&self.keys.processes_data, &process_id)
                .query_async(&mut redis)
                .await?;

            if resurrected_count > 0 {
                tracing::info!(
                    "Resurrected process: {} ({} jobs)",
                    process_id,
                    resurrected_count
                );
            }
        }

        Ok(())
    }

    pub async fn self_cleanup(&self) -> Result<(), OxanusError> {
        let mut redis = self.connection().await?;
        let process = self.current_process();
        let _: () = redis::pipe()
            .zrem(&self.keys.processes, process.id())
            .hdel(&self.keys.processes_data, process.id())
            .query_async(&mut redis)
            .await?;

        Ok(())
    }

    fn processing_queue(&self, process_id: &str) -> String {
        format!("{}:{}", self.keys.processing_queue_prefix, process_id)
    }

    fn current_processing_queue(&self) -> String {
        self.processing_queue(&self.current_process().id())
    }

    #[cfg(test)]
    async fn currently_processing_job_ids(&self) -> Result<Vec<String>, OxanusError> {
        let mut redis = self.connection().await?;
        let job_ids: Vec<String> = (*redis)
            .lrange(self.current_processing_queue(), 0, 0)
            .await?;
        Ok(job_ids)
    }

    fn current_process(&self) -> Process {
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        let pid = std::process::id();
        Process {
            hostname,
            pid,
            heartbeat_at: chrono::Utc::now().timestamp(),
        }
    }

    fn namespace_queue(&self, queue: &str) -> String {
        if queue.starts_with(self.keys.namespace.as_str()) {
            queue.to_string()
        } else {
            format!("{}:{}", self.keys.queue_prefix, queue)
        }
    }

    async fn dead_process_ids(
        &self,
        redis: &mut deadpool_redis::Connection,
    ) -> Result<Vec<String>, OxanusError> {
        let process_ids: Vec<(String, f64)> = (*redis)
            .zrange_withscores(&self.keys.processes, 0, -1)
            .await?;

        let all_process_ids: Vec<String> = process_ids.iter().map(|(id, _)| id.clone()).collect();
        let mut dead_process_ids: Vec<String> = process_ids
            .iter()
            .filter(|(_, score)| {
                *score < (chrono::Utc::now().timestamp() - RESURRECT_THRESHOLD_SECS) as f64
            })
            .map(|(id, _)| id.clone())
            .collect();

        let all_processing_queues: Vec<String> = redis
            .keys(format!("{}:*", self.keys.processing_queue_prefix))
            .await?;

        for processing_queue in all_processing_queues {
            let process_id = match processing_queue.rsplit(':').next() {
                Some(process_id) => process_id.to_string(),
                None => continue,
            };

            if !all_process_ids.contains(&process_id) {
                dead_process_ids.push(process_id);
            }
        }

        Ok(dead_process_ids)
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
    async fn test_ping() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        storage.ping().await?;

        let process = storage.current_process();
        let process_data = storage.get_process_data(&process.id()).await?;
        assert!(process_data.is_some());
        let process = process_data.unwrap();
        assert_eq!(
            process.hostname,
            gethostname::gethostname().to_string_lossy().to_string()
        );
        assert_eq!(process.pid, std::process::id());
        assert!(process.heartbeat_at > chrono::Utc::now().timestamp() - 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_latency() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();

        let mut envelope = JobEnvelope::new(queue.clone(), TestWorker {})?;
        let now = chrono::Utc::now();
        let actual_latency = 7777;
        envelope.meta.created_at = now.timestamp_micros() - actual_latency * 1_000;
        storage.enqueue(envelope).await?;

        let latency = storage.latency_ms(&queue).await?;

        assert!((latency - actual_latency as f64).abs() < 50.0);

        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();
        let mut expired_envelope1 = JobEnvelope::new(queue.clone(), TestWorker {})?;
        expired_envelope1.meta.created_at =
            (chrono::Utc::now().timestamp() - JOB_EXPIRE_TIME - 1) * 1000000;
        let mut expired_envelope2 = JobEnvelope::new(queue.clone(), TestWorker {})?;
        expired_envelope2.meta.created_at =
            (chrono::Utc::now().timestamp() - JOB_EXPIRE_TIME - 1) * 1000000;

        let active_envelope = JobEnvelope::new(queue.clone(), TestWorker {})?;

        storage.enqueue(expired_envelope1.clone()).await?;
        storage.enqueue(expired_envelope2.clone()).await?;
        storage.enqueue(active_envelope.clone()).await?;

        assert!(storage.get_job(&expired_envelope1.id).await?.is_some());
        assert!(storage.get_job(&expired_envelope2.id).await?.is_some());
        assert!(storage.get_job(&active_envelope.id).await?.is_some());

        assert_eq!(storage.cleanup().await?, 2);

        assert!(storage.get_job(&expired_envelope1.id).await?.is_none());
        assert!(storage.get_job(&expired_envelope2.id).await?.is_none());
        assert!(storage.get_job(&active_envelope.id).await?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_empty() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));

        assert_eq!(storage.cleanup().await?, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_resurrect() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();
        let envelope = JobEnvelope::new(queue.clone(), TestWorker {})?;

        storage.enqueue(envelope.clone()).await?;

        assert_eq!(storage.enqueued_count(&queue).await?, 1);
        assert!(storage.currently_processing_job_ids().await?.is_empty());

        let job_id = storage.dequeue(&queue).await?;

        assert_eq!(job_id, Some(envelope.id));

        assert_eq!(storage.enqueued_count(&queue).await?, 0);
        assert_eq!(
            storage.currently_processing_job_ids().await?,
            vec![job_id.unwrap()]
        );

        let mut redis = storage.connection().await?;

        // fake ping in the past
        let _: () = redis
            .zadd(
                &storage.keys.processes,
                storage.current_process().id(),
                chrono::Utc::now().timestamp() - RESURRECT_THRESHOLD_SECS - 1,
            )
            .await?;

        storage.resurrect().await?;

        assert_eq!(storage.enqueued_count(&queue).await?, 1);
        assert!(storage.currently_processing_job_ids().await?.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_resurrect_when_process_is_missing() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();
        let envelope = JobEnvelope::new(queue.clone(), TestWorker {})?;

        storage.enqueue(envelope.clone()).await?;

        assert_eq!(storage.enqueued_count(&queue).await?, 1);
        assert!(storage.currently_processing_job_ids().await?.is_empty());

        let job_id = storage.dequeue(&queue).await?;

        assert_eq!(job_id, Some(envelope.id));

        assert_eq!(storage.enqueued_count(&queue).await?, 0);
        assert_eq!(
            storage.currently_processing_job_ids().await?,
            vec![job_id.unwrap()]
        );

        storage.resurrect().await?;

        assert_eq!(storage.enqueued_count(&queue).await?, 1);
        assert!(storage.currently_processing_job_ids().await?.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_many_with_missing_jobs() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();

        let envelope1 = JobEnvelope::new(queue.clone(), TestWorker {})?;
        let envelope2 = JobEnvelope::new(queue.clone(), TestWorker {})?;
        let envelope3 = JobEnvelope::new(queue.clone(), TestWorker {})?;

        storage.enqueue(envelope1.clone()).await?;
        storage.enqueue(envelope2.clone()).await?;
        storage.enqueue(envelope3.clone()).await?;

        storage.delete_job(&envelope2.id).await?;

        let job_ids = vec![
            envelope1.id.clone(),
            envelope2.id.clone(),
            envelope3.id.clone(),
        ];
        let envelopes = storage.get_many(&job_ids).await?;

        assert_eq!(envelopes.len(), 2);

        let returned_ids: Vec<String> = envelopes.iter().map(|e| e.id.clone()).collect();
        assert!(returned_ids.contains(&envelope1.id));
        assert!(!returned_ids.contains(&envelope2.id));
        assert!(returned_ids.contains(&envelope3.id));

        Ok(())
    }
}
