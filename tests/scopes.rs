use etherbird::DisconnectOnDrop;
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, oneshot, watch};

struct ObserveRelease(watch::Sender<bool>);
impl Drop for ObserveRelease {
    fn drop(&mut self) {
        assert!(
            !*self.0.borrow(),
            "loss must be published before ownership release"
        );
    }
}

#[test]
fn incomplete_scope_signals_loss_before_releasing_ownership() {
    let connected = watch::channel(true).0;
    drop(DisconnectOnDrop::new(
        ObserveRelease(connected.clone()),
        &connected,
    ));
    assert!(!*connected.borrow());
}

#[test]
fn completed_healthy_rejection_retains_connection_without_resurrecting_a_lost_one() {
    let ownership = std::sync::Mutex::new(());
    let connected = watch::channel(true).0;
    {
        let mut scope = DisconnectOnDrop::new(ownership.lock().unwrap(), &connected);
        // Adapter classifies this rejection as a completed, healthy workflow.
        scope.complete();
        scope.complete();
        assert!(ownership.try_lock().is_err());
    }
    assert!(*connected.borrow());
    assert!(ownership.try_lock().is_ok());
    {
        let mut scope = DisconnectOnDrop::new(ownership.lock().unwrap(), &connected);
        connected.send_replace(false);
        scope.complete();
    }
    assert!(!*connected.borrow());
}

#[tokio::test]
async fn abort_between_exchanges_signals_loss_before_a_successor_acquires_ownership() {
    let ownership = Arc::new(Mutex::new(()));
    let connected = watch::channel(true).0;
    let (entered, ready) = oneshot::channel();
    let signal = connected.clone();
    let owner = ownership.clone();
    let task = tokio::spawn(async move {
        let _scope = DisconnectOnDrop::new(owner.lock().await, &signal);
        // A first exchange has completed; there is no per-exchange guard now.
        entered.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    ready.await.unwrap();
    assert!(ownership.try_lock().is_err());
    let next_signal = connected.clone();
    let next_owner = ownership.clone();
    let successor = tokio::spawn(async move {
        let _guard = next_owner.lock().await;
        assert!(!*next_signal.borrow());
    });
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    successor.await.unwrap();
    assert!(ownership.try_lock().is_ok());
}

#[tokio::test(start_paused = true)]
async fn deadline_between_exchanges_releases_ownership_and_signals_loss() {
    let ownership = Mutex::new(());
    let connected = watch::channel(true).0;
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        let _scope = DisconnectOnDrop::new(ownership.lock().await, &connected);
        std::future::pending::<()>().await;
    })
    .await;
    assert!(result.is_err());
    assert!(!*connected.borrow());
    assert!(ownership.try_lock().is_ok());
}

#[test]
fn panic_and_early_return_also_invalidate_incomplete_scopes() {
    let connected = watch::channel(true).0;
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _scope = DisconnectOnDrop::new(ObserveRelease(connected.clone()), &connected);
        panic!("adapter panic");
    }));
    assert!(panic.is_err());
    assert!(!*connected.borrow());
    let connected = watch::channel(true).0;
    fn incomplete(connected: &watch::Sender<bool>) -> Result<(), &'static str> {
        let _scope = DisconnectOnDrop::new(ObserveRelease(connected.clone()), connected);
        Err("unvalidated workflow")
    }
    assert!(incomplete(&connected).is_err());
    assert!(!*connected.borrow());
}
