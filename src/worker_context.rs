use crate::job_envelope::JobMeta;

#[derive(Debug, Clone)]
pub struct WorkerContext<T: Clone + Send + Sync> {
    pub ctx: T,
    pub meta: JobMeta,
}

#[derive(Debug, Clone)]
pub struct WorkerContextValue<T: Clone + Send + Sync>(pub T);

impl<T: Clone + Send + Sync> WorkerContextValue<T> {
    pub fn new(t: T) -> Self {
        Self(t)
    }
}
