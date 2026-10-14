use super::*;

pub(super) async fn run_job<L, F, Fut, T>(
    mut sender: oneshot::Sender<Result<T, Error<L::Error>>>,
    admission: Admission<L>,
    operation: F,
) where
    L: Lifecycle,
    F: FnOnce(ResourceLease<L>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T, Error<L::Error>>> + Send + 'static,
    T: Send + 'static,
{
    if sender.is_closed() {
        return;
    }
    let lease = match admission {
        Ok(lease) => lease,
        Err(error) => {
            let _ = sender.send(Err(error));
            return;
        }
    };
    tokio::select! {
        biased;
        _ = sender.closed() => {},
        result = operation(lease) => { let _ = sender.send(result); }
    }
}
pub(super) struct CancelOnDrop {
    pub(super) cancelled: Arc<AtomicBool>,
    pub(super) dispatcher: Arc<Notify>,
    pub(super) armed: bool,
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.cancelled.store(true, Ordering::Release);
        self.dispatcher.notify_one();
    }
}
