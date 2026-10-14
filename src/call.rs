use super::*;

/// Shared call deadline policy for single attempts and explicit retry scopes.
/// Zero never polls work; stop wins ties.
pub(super) async fn with_call_timeout<T, E>(
    duration: Duration,
    mut stopping: watch::Receiver<bool>,
    work: impl Future<Output = Result<T, Error<E>>>,
) -> Result<T, Error<E>> {
    if *stopping.borrow() {
        return Err(Error::Stopped);
    }
    if duration.is_zero() {
        return Err(Error::Timeout);
    }
    let result = tokio::select! {
        biased;
        _ = shutdown(&mut stopping) => Err(Error::Stopped),
        result = timeout(duration, work) => result.unwrap_or(Err(Error::Timeout)),
    };
    if *stopping.borrow() {
        Err(Error::Stopped)
    } else {
        result
    }
}
pub(super) async fn shutdown(receiver: &mut watch::Receiver<bool>) {
    loop {
        if *receiver.borrow_and_update() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}
