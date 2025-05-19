use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

pub struct SemaphoresMap {
    permits: usize,
    inner: Mutex<HashMap<String, Arc<Semaphore>>>,
}

impl SemaphoresMap {
    pub fn new(permits: usize) -> Self {
        Self {
            permits,
            inner: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get_or_create(&self, key: String) -> Arc<Semaphore> {
        let mut map = self.inner.lock().await;
        Arc::clone(
            map.entry(key)
                .or_insert_with(|| Arc::new(Semaphore::new(self.permits))),
        )
    }
}
