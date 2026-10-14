//! Adapter-owned cancellation safety for resources with an outstanding reply.
use tokio::sync::watch;

/// Retire a resource slot when a scope ends before the adapter confirms completion.
///
/// Hold this guard across the entire exchange, including reply validation. On
/// cancellation, timeout, panic unwinding or an early return, it drops the slot's
/// resource and publishes `false` to the adapter's connection indicator.
/// Call [`Self::complete`] only after a valid exchange, including a valid device
/// rejection that leaves the transport usable. The guard does not infer health
/// from a `Result`, retry operations, acquire locks or await lifecycle cleanup.
/// Dropping a resource is not a guarantee of remote cancellation or secret erasure.
///
/// This is optional adapter plumbing, independent of direct versus pooled dispatch.
///
/// ```
/// use etherbird::RetireOnDrop;
/// let (connected, _) = tokio::sync::watch::channel(true);
/// let mut slot = Some(42);
/// {
///     let mut exchange = RetireOnDrop::new(&mut slot, &connected);
///     assert_eq!(exchange.resource(), Some(&mut 42));
///     exchange.complete(); // The adapter has validated the complete reply.
/// }
/// assert_eq!(slot, Some(42));
/// assert!(*connected.borrow());
/// {
///     let _incomplete = RetireOnDrop::new(&mut slot, &connected);
/// }
/// assert!(slot.is_none());
/// assert!(!*connected.borrow());
/// ```
#[must_use = "hold the guard across the exchange and explicitly complete valid replies"]
pub struct RetireOnDrop<'a, T> {
    slot: &'a mut Option<T>,
    connected: &'a watch::Sender<bool>,
    complete: bool,
}
impl<'a, T> RetireOnDrop<'a, T> {
    pub fn new(slot: &'a mut Option<T>, connected: &'a watch::Sender<bool>) -> Self {
        Self {
            slot,
            connected,
            complete: false,
        }
    }
    /// Mutably borrow the value in the guarded slot.
    pub fn resource(&mut self) -> Option<&mut T> {
        self.slot.as_mut()
    }
    /// Preserve the slot after the adapter has validated completion.
    pub fn complete(&mut self) {
        self.complete = true;
    }
}
impl<T> Drop for RetireOnDrop<'_, T> {
    fn drop(&mut self) {
        if !self.complete {
            self.slot.take();
            self.connected.send_replace(false);
        }
    }
}
