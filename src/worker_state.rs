#[derive(Debug, Clone)]
pub struct WorkerState<T: Clone + Send + Sync>(pub T);

impl<T: Clone + Send + Sync> WorkerState<T> {
    pub fn new(t: T) -> Self {
        Self(t)
    }
}
