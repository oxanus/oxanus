fn main() {
    divan::main();
}

// use oxanus::Queue;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerNoop {
    pub sleep_ms: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("Generic error: {0}")]
    GenericError(String),
}

#[derive(Debug, Clone)]
pub struct Connections {
    pub db: sqlx::postgres::PgPool,
}

const JOBS_COUNT: u64 = 1000;
const CONCURRENCY: &[usize] = &[1, 2, 4, 8, 12, 16, 512];

#[async_trait::async_trait]
impl oxanus::Worker for WorkerNoop {
    type Data = Connections;
    type Error = ServiceError;

    async fn process(
        &self,
        oxanus::WorkerState(_conns): &oxanus::WorkerState<Connections>,
    ) -> Result<(), ServiceError> {
        tokio::time::sleep(std::time::Duration::from_millis(self.sleep_ms)).await;
        Ok(())
    }
}

#[divan::bench(args = CONCURRENCY, sample_size = 1, sample_count = 1)]
fn run_1000_jobs_taking_0_ms(bencher: divan::Bencher, n: usize) {
    let rt = &tokio::runtime::Runtime::new().unwrap();
    let sleep_ms = 0;
    let pool = rt.block_on(async { setup(n, JOBS_COUNT, sleep_ms).await.unwrap() });

    bencher.bench(|| {
        let pool = pool.clone();
        rt.block_on(async {
            execute(pool, n, JOBS_COUNT).await.unwrap();
        })
    });
}

#[divan::bench(args = CONCURRENCY, sample_size = 1, sample_count = 1)]
fn run_1000_jobs_taking_1_ms(bencher: divan::Bencher, n: usize) {
    let rt = &tokio::runtime::Runtime::new().unwrap();
    let sleep_ms = 1;
    let pool = rt.block_on(async { setup(n, JOBS_COUNT, sleep_ms).await.unwrap() });

    bencher.bench(|| {
        let pool = pool.clone();
        rt.block_on(async {
            execute(pool, n, JOBS_COUNT).await.unwrap();
        })
    });
}

#[divan::bench(args = CONCURRENCY, sample_size = 1, sample_count = 1)]
fn run_1000_jobs_taking_2_ms(bencher: divan::Bencher, n: usize) {
    let rt = &tokio::runtime::Runtime::new().unwrap();
    let sleep_ms = 2;
    let pool = rt.block_on(async { setup(n, JOBS_COUNT, sleep_ms).await.unwrap() });

    bencher.bench(|| {
        let pool = pool.clone();
        rt.block_on(async {
            execute(pool, n, JOBS_COUNT).await.unwrap();
        })
    });
}

#[divan::bench(args = CONCURRENCY, sample_size = 1, sample_count = 1)]
fn run_1000_jobs_taking_10_ms(bencher: divan::Bencher, n: usize) {
    let rt = &tokio::runtime::Runtime::new().unwrap();
    let sleep_ms = 10;
    let pool = rt.block_on(async { setup(n, JOBS_COUNT, sleep_ms).await.unwrap() });

    bencher.bench(|| {
        let pool = pool.clone();
        rt.block_on(async {
            execute(pool, n, JOBS_COUNT).await.unwrap();
        })
    });
}

async fn setup(
    concurrency: usize,
    jobs_count: u64,
    sleep_ms: u64,
) -> Result<sqlx::postgres::PgPool, oxanus::OxanusError> {
    let url =
        std::env::var("PG_URL").unwrap_or_else(|_e| "postgresql://localhost/oxanus".to_string());
    let pool = sqlx::postgres::PgPool::connect(&url).await?;
    let redis = redis::aio::ConnectionManager::new(
        redis::Client::open(std::env::var("REDIS_URL").expect("REDIS_URL is not set")).unwrap(),
    )
    .await?;

    let queue = oxanus::QueueStatic::new("one");
    let config = build_config(queue.clone(), concurrency);

    oxanus::setup(&redis, &config).await?;

    for _ in 0..jobs_count {
        oxanus::enqueue(&redis, &queue, WorkerNoop { sleep_ms }).await?;
    }

    Ok(pool)
}

async fn execute(
    pool: sqlx::postgres::PgPool,
    concurrency: usize,
    jobs_count: u64,
) -> Result<(), oxanus::OxanusError> {
    let queue = oxanus::QueueStatic::new("one");
    let redis = redis::aio::ConnectionManager::new(
        redis::Client::open(std::env::var("REDIS_URL").expect("REDIS_URL is not set")).unwrap(),
    )
    .await?;
    let config = build_config(queue.clone(), concurrency).exit_when_finished(jobs_count);
    let data = oxanus::WorkerState::new(Connections { db: pool.clone() });

    let stats = oxanus::run(&redis, config, data).await?;

    assert_eq!(stats.processed, jobs_count);
    assert_eq!(stats.succeeded, jobs_count);
    assert_eq!(stats.failed, 0);

    Ok(())
}

fn build_config(
    queue: oxanus::QueueStatic,
    concurrency: usize,
) -> oxanus::Config<Connections, ServiceError> {
    oxanus::Config::new()
        .register_processor(
            oxanus::Processor::new()
                .concurrency(concurrency)
                .queue(&queue.name),
        )
        // .register_queue(queue)
        .register_worker::<WorkerNoop>()
        .exit_when_idle()
}
