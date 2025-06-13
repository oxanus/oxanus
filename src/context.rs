use crate::job_envelope::JobMeta;

#[derive(Debug, Clone)]
pub struct Context<T: Clone + Send + Sync> {
    pub ctx: T,
    pub meta: JobMeta,
}

impl<T: Clone + Send + Sync> Context<T> {
    pub fn value(v: T) -> ContextValue<T> {
        ContextValue(v)
    }
}

#[derive(Debug, Clone)]
pub struct ContextValue<T: Clone + Send + Sync>(pub(crate) T);
