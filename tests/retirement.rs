use etherbird::RetireOnDrop;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{oneshot, watch};

struct Resource {
    drops: Arc<AtomicUsize>,
    connected: watch::Sender<bool>,
}
impl Drop for Resource {
    fn drop(&mut self) {
        // Close/drop the resource before publishing connection loss.
        assert!(*self.connected.borrow());
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
fn resource() -> (Option<Resource>, watch::Sender<bool>, Arc<AtomicUsize>) {
    let (connected, _) = watch::channel(true);
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Some(Resource {
            drops: drops.clone(),
            connected: connected.clone(),
        }),
        connected,
        drops,
    )
}

#[test]
fn incomplete_scope_drops_resource_before_notifying_and_empty_slots_are_safe() {
    let (mut slot, connected, drops) = resource();
    drop(RetireOnDrop::new(&mut slot, &connected));
    assert!(slot.is_none());
    assert!(!*connected.borrow());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(RetireOnDrop::new(&mut slot, &connected));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn adapter_can_complete_a_healthy_device_rejection_without_retirement() {
    let (mut slot, connected, drops) = resource();
    {
        let mut exchange = RetireOnDrop::new(&mut slot, &connected);
        assert!(exchange.resource().is_some());
        // A rejection can still be a complete, validated exchange.
        exchange.complete();
        exchange.complete();
    }
    assert!(slot.is_some());
    assert!(*connected.borrow());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn aborted_inflight_future_retires_its_resource() {
    let (mut slot, connected, drops) = resource();
    let signal = connected.clone();
    let (started, ready) = oneshot::channel();
    let task = tokio::spawn(async move {
        let _exchange = RetireOnDrop::new(&mut slot, &signal);
        started.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    ready.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(!*connected.borrow());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn response_timeout_retires_its_resource() {
    let (mut slot, connected, drops) = resource();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        let _exchange = RetireOnDrop::new(&mut slot, &connected);
        std::future::pending::<()>().await;
    })
    .await;
    assert!(result.is_err());
    assert!(slot.is_none());
    assert!(!*connected.borrow());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
