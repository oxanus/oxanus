fn main() {
    divan::main();
}

use oxanus::Queue;
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

#[divan::bench(args = [1, 2, 4, 8], max_time = 1)]
fn run_1000_jobs_taking_1_ms(bencher: divan::Bencher, n: usize) {
    let rt = &tokio::runtime::Runtime::new().unwrap();
    bencher.bench(|| {
        rt.block_on(async {
            divan::black_box(inner_run(n, 1000, 1).await).unwrap();
        })
    });
}

#[divan::bench(args = [1, 2, 4, 8], max_time = 1)]
fn run_1000_jobs_taking_2_ms(bencher: divan::Bencher, n: usize) {
    let rt = &tokio::runtime::Runtime::new().unwrap();
    bencher.bench(|| {
        rt.block_on(async {
            divan::black_box(inner_run(n, 1000, 2).await).unwrap();
        })
    });
}

async fn inner_run(
    concurrency: usize,
    jobs_count: usize,
    sleep_ms: u64,
) -> Result<(), oxanus::OxanusError> {
    let url =
        std::env::var("PG_URL").unwrap_or_else(|_e| "postgresql://localhost/oxanus".to_string());
    let pool = sqlx::postgres::PgPool::connect(&url).await?;
    let data = oxanus::WorkerState::new(Connections { db: pool.clone() });

    let queue = Queue::new("one", concurrency);

    let config = oxanus::Config::new()
        .register_queue(queue)
        .register_worker::<WorkerNoop>()
        .exit_when_idle();

    oxanus::setup(&pool, &config).await?;

    for _ in 0..jobs_count {
        oxanus::enqueue(&pool, &queue, WorkerNoop { sleep_ms }).await?;
    }
    oxanus::run(&pool, config, data).await?;

    Ok(())
}
