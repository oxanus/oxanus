#[derive(Debug, Clone)]
pub struct WorkerState<T: Clone>(pub T);

impl<T: Clone> WorkerState<T> {
    pub fn new(t: T) -> Self {
        Self(t)
    }
}
