use serde::{Deserialize, Serialize};

fn main() {
    divan::main();
}

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
pub struct WorkerState {}

#[derive(Serialize)]
pub struct QueueOne;

impl oxanus::Queue for QueueOne {
    fn to_config() -> oxanus::QueueConfig {
        oxanus::QueueConfig {
            kind: oxanus::QueueKind::Static {
                key: "one".to_string(),
            },
            concurrency: 1,
            throttle: None,
        }
    }
}

const JOBS_COUNT: u64 = 1000;
const CONCURRENCY: &[usize] = &[1, 2, 4, 8, 12, 16, 512];

#[async_trait::async_trait]
impl oxanus::Worker for WorkerNoop {
    type Data = WorkerState;
    type Error = ServiceError;

    async fn process(
        &self,
        oxanus::WorkerState(_conns): &oxanus::WorkerState<WorkerState>,
    ) -> Result<(), ServiceError> {
        tokio::time::sleep(std::time::Duration::from_millis(self.sleep_ms)).await;
        Ok(())
    }
}

#[divan::bench(args = CONCURRENCY, sample_size = 1, sample_count = 1)]
fn run_1000_jobs_taking_0_ms(bencher: divan::Bencher, n: usize) {
    let rt = &tokio::runtime::Runtime::new().unwrap();
    let sleep_ms = 0;
    let client =
        redis::Client::open(std::env::var("REDIS_URL").expect("REDIS_URL is not set")).unwrap();
    let config = build_config(client, n);
    rt.block_on(async { setup(config, JOBS_COUNT, sleep_ms).await.unwrap() });

    bencher.bench(|| {
        rt.block_on(async {
            execute(n, JOBS_COUNT).await.unwrap();
        })
    });
}

#[divan::bench(args = CONCURRENCY, sample_size = 1, sample_count = 1)]
fn run_1000_jobs_taking_1_ms(bencher: divan::Bencher, n: usize) {
    let rt = &tokio::runtime::Runtime::new().unwrap();
    let sleep_ms = 1;
    let client =
        redis::Client::open(std::env::var("REDIS_URL").expect("REDIS_URL is not set")).unwrap();
    let config = build_config(client, n);
    rt.block_on(async { setup(config, JOBS_COUNT, sleep_ms).await.unwrap() });

    bencher.bench(|| {
        rt.block_on(async {
            execute(n, JOBS_COUNT).await.unwrap();
        })
    });
}

#[divan::bench(args = CONCURRENCY, sample_size = 1, sample_count = 1)]
fn run_1000_jobs_taking_2_ms(bencher: divan::Bencher, n: usize) {
    let rt = &tokio::runtime::Runtime::new().unwrap();
    let sleep_ms = 2;
    let client =
        redis::Client::open(std::env::var("REDIS_URL").expect("REDIS_URL is not set")).unwrap();
    let config = build_config(client, n);
    rt.block_on(async { setup(config, JOBS_COUNT, sleep_ms).await.unwrap() });

    bencher.bench(|| {
        rt.block_on(async {
            execute(n, JOBS_COUNT).await.unwrap();
        })
    });
}

#[divan::bench(args = CONCURRENCY, sample_size = 1, sample_count = 1)]
fn run_1000_jobs_taking_10_ms(bencher: divan::Bencher, n: usize) {
    let rt = &tokio::runtime::Runtime::new().unwrap();
    let sleep_ms = 10;
    let client =
        redis::Client::open(std::env::var("REDIS_URL").expect("REDIS_URL is not set")).unwrap();
    let config = build_config(client, n);
    rt.block_on(async { setup(config, JOBS_COUNT, sleep_ms).await.unwrap() });

    bencher.bench(|| {
        rt.block_on(async {
            execute(n, JOBS_COUNT).await.unwrap();
        })
    });
}

async fn setup(
    config: oxanus::Config<WorkerState, ServiceError>,
    jobs_count: u64,
    sleep_ms: u64,
) -> Result<(), oxanus::OxanusError> {
    for _ in 0..jobs_count {
        oxanus::enqueue(&config, QueueOne, WorkerNoop { sleep_ms }).await?;
    }

    Ok(())
}

async fn execute(concurrency: usize, jobs_count: u64) -> Result<(), oxanus::OxanusError> {
    let client =
        redis::Client::open(std::env::var("REDIS_URL").expect("REDIS_URL is not set")).unwrap();
    let config = build_config(client, concurrency).exit_when_processed(jobs_count);
    let data = oxanus::WorkerState::new(WorkerState {});

    let stats = oxanus::run(config, data).await?;

    assert_eq!(stats.processed, jobs_count);
    assert_eq!(stats.succeeded, jobs_count);
    assert_eq!(stats.failed, 0);

    Ok(())
}

fn build_config(
    redis_client: redis::Client,
    concurrency: usize,
) -> oxanus::Config<WorkerState, ServiceError> {
    let storage = oxanus::Storage::new(redis_client);
    oxanus::Config::new(storage)
        .register_queue_with_concurrency::<QueueOne>(concurrency)
        .register_worker::<WorkerNoop>()
}
