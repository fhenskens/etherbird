//! Supervised asynchronous resources with automatic recovery.
//!
//! Implement [`Lifecycle`] and expose a typed client wrapper around [`Supervisor`].
//! Operations run once by default; retries require explicit per-operation opt-in.
//! Lifecycle futures must be cancellation-safe.

pub use async_trait::async_trait;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::{fmt, future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{sleep, timeout};
#[cfg(feature = "pool")]
mod pool;
#[cfg(feature = "pool")]
mod queue;
#[cfg(feature = "pool")]
pub use pool::{ManagedResourceProxy, Pool, PoolConfig, QueuedOperation, ResourceLease};
mod attributes;
use attributes::{Registrations, value_setter};
mod call;
mod driver;
use call::{shutdown, with_call_timeout};
use driver::{WorkerStatus, maintain};
mod proxy;
mod retirement;
mod validation;
pub use proxy::{ManagedClientBackend, SupervisedResourceProxy};
#[cfg(feature = "pool")]
pub use queue::{BoundedFifoQueue, FifoQueue, OperationQueue, PriorityQueue, QueueError};
pub use retirement::RetireOnDrop;
pub use validation::ConfigError;

/// Protocol-specific lifecycle hooks. Resources usually contain their own interior mutability.
///
/// Hooks must yield to the executor and tolerate cancellation by future drop. Creation must
/// release partially allocated resources on cancellation. Connect/setup failures run teardown.
/// Cleanup, disconnect and destroy each get a separate timeout; failure never skips later hooks.
#[async_trait]
pub trait Lifecycle: Send + Sync + 'static {
    type Resource: Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync + 'static;
    async fn create(&self) -> Result<Self::Resource, Self::Error>;
    async fn connect(&self, resource: &Self::Resource) -> Result<(), Self::Error>;
    async fn on_resource_created(&self, _resource: &Self::Resource) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn setup(&self, _resource: &Self::Resource) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn cleanup(&self, _resource: &Self::Resource) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn disconnect(&self, resource: &Self::Resource) -> Result<(), Self::Error>;
    async fn destroy(&self, _resource: &Self::Resource) -> Result<(), Self::Error> {
        Ok(())
    }
    /// Optional connection-loss watch. No watchdog task is spawned when absent.
    fn watch_disconnect(
        &self,
        _resource: Arc<Self::Resource>,
    ) -> Option<LifecycleFuture<Self::Error>> {
        None
    }
    /// Read the transport's live connection indicator. Override alongside `wait_connected`.
    fn is_connected(&self, _resource: &Self::Resource) -> bool {
        true
    }
    /// Wait until the transport becomes connected, checking its indicator before waiting.
    async fn wait_connected(&self, _resource: &Self::Resource) {
        std::future::pending::<()>().await
    }
    /// Terminal errors skip disconnect, but still run cleanup and destroy.
    fn is_terminal(&self, _error: &Self::Error) -> bool {
        false
    }
    fn is_expected(&self, _error: &Self::Error) -> bool {
        false
    }
    /// Decide whether an operation error retires its resource. Independent of
    /// logging and replay: retaining a resource never authorizes another attempt.
    fn operation_failure(&self, _error: &Self::Error) -> OperationFailurePolicy {
        OperationFailurePolicy::Recover
    }
    /// Classify create/created/connect/setup and disconnect-watch errors.
    /// Fail latches a typed cause until explicit reset or stop/restart. It does
    /// not change cleanup/disconnect/destroy error handling or `is_terminal`.
    fn lifecycle_failure(&self, _error: &Self::Error) -> LifecycleFailurePolicy {
        LifecycleFailurePolicy::Retry
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Recovery decision for an operation error; independent of replay permission.
pub enum OperationFailurePolicy {
    /// Retire the failed generation using the existing teardown policy.
    Recover,
    /// Return the operation error while retaining the healthy generation.
    Retain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Scheduling decision for an initialization or disconnect-watch error.
pub enum LifecycleFailurePolicy {
    /// Continue automatic lifecycle recovery with configured backoff.
    Retry,
    /// Suspend attempts and fail readiness/admission until explicit reset.
    Fail,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceState {
    Connecting,
    Connected,
    Recovering,
    Disconnected,
    /// Automatic lifecycle attempts are suspended until explicit reset.
    Failed,
    Stopping,
    Stopped,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub resource_name: String,
    pub create_timeout: Option<Duration>,
    pub created_timeout: Option<Duration>,
    pub connect_timeout: Duration,
    pub setup_timeout: Duration,
    pub cleanup_timeout: Duration,
    pub disconnect_timeout: Duration,
    pub destroy_timeout: Option<Duration>,
    pub retry_delay: Duration,
    pub max_retry_delay: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            resource_name: "Resource".into(),
            create_timeout: None,
            created_timeout: None,
            connect_timeout: Duration::from_secs(20),
            setup_timeout: Duration::from_secs(10),
            cleanup_timeout: Duration::from_secs(10),
            disconnect_timeout: Duration::from_secs(10),
            destroy_timeout: None,
            retry_delay: Duration::from_secs(5),
            max_retry_delay: Duration::from_secs(300),
        }
    }
}

pub enum Error<E> {
    Stopped,
    /// The overall deadline for an operation or readiness wait elapsed.
    Timeout,
    Operation(E),
    /// A non-retryable lifecycle cause shared by waiting callers. Display and
    /// Debug redact it; inspect the value/source explicitly with appropriate care.
    Lifecycle(Arc<E>),
    #[cfg(feature = "pool")]
    Queue(QueueError),
}
impl<E: fmt::Debug> fmt::Debug for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped => f.write_str("Stopped"),
            Self::Timeout => f.write_str("Timeout"),
            Self::Operation(e) => f.debug_tuple("Operation").field(e).finish(),
            Self::Lifecycle(_) => f.write_str("Lifecycle(<redacted>)"),
            #[cfg(feature = "pool")]
            Self::Queue(e) => f.debug_tuple("Queue").field(e).finish(),
        }
    }
}
impl<E: fmt::Display> fmt::Display for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped => f.write_str("resource supervisor stopped"),
            Self::Timeout => f.write_str("operation retry deadline elapsed"),
            Self::Operation(e) => e.fmt(f),
            Self::Lifecycle(_) => f.write_str("non-retryable resource lifecycle failure"),
            #[cfg(feature = "pool")]
            Self::Queue(e) => e.fmt(f),
        }
    }
}
impl<E: std::error::Error + 'static> std::error::Error for Error<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Operation(e) => Some(e),
            Self::Lifecycle(e) => Some(e.as_ref()),
            Self::Stopped | Self::Timeout => None,
            #[cfg(feature = "pool")]
            Self::Queue(e) => Some(e),
        }
    }
}

/// Bounds for an explicitly retryable operation. The caller must ensure replay is safe.
///
/// The attempt count includes the initial call. The deadline covers acquisition,
/// every operation attempt, and recovery/teardown between attempts. Connection
/// failures while acquiring a ready resource do not consume operation attempts.
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    pub max_attempts: std::num::NonZeroUsize,
    pub timeout: Duration,
}

/// A specific resource generation. Old handles do not silently become new resources.
pub struct ResourceHandle<R> {
    resource: Arc<R>,
    generation: u64,
}
impl<R> Clone for ResourceHandle<R> {
    fn clone(&self) -> Self {
        Self {
            resource: self.resource.clone(),
            generation: self.generation,
        }
    }
}
impl<R> ResourceHandle<R> {
    pub fn resource(&self) -> &Arc<R> {
        &self.resource
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
}
pub struct ResourceStatus<R> {
    pub state: ResourceState,
    pub handle: Option<ResourceHandle<R>>,
}
type Snapshot<R> = ResourceStatus<R>;
impl<R> Clone for Snapshot<R> {
    fn clone(&self) -> Self {
        Self {
            state: self.state,
            handle: self.handle.clone(),
        }
    }
}
struct Owner {
    stop: watch::Sender<bool>,
    started: AtomicBool,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
struct Recovery {
    generation: u64,
    terminal: bool,
    completed: Option<oneshot::Sender<()>>,
}
pub type LifecycleFuture<E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send>>;
pub type CreatedHook<R, E> = Arc<dyn Fn(Arc<R>) -> LifecycleFuture<E> + Send + Sync>;
pub type AttributeSetter<R> = Arc<dyn Fn(&R) + Send + Sync>;
struct Attributes<R, E> {
    registrations: Registrations<R>,
    last: Option<Arc<R>>,
    on_created: Option<CreatedHook<R, E>>,
    after_created: Option<CreatedHook<R, E>>,
}

/// Cloneable client-facing handle. Start inside a Tokio runtime.
/// Dropping the last supervisor requests shutdown; `stop().await` waits for teardown.
pub struct Supervisor<L: Lifecycle> {
    lifecycle: Arc<L>,
    owner: Arc<Owner>,
    snapshot: watch::Receiver<Snapshot<L::Resource>>,
    recovery: mpsc::UnboundedSender<Recovery>,
    attributes: Arc<Mutex<Attributes<L::Resource, L::Error>>>,
    config: Config,
    sender: watch::Sender<Snapshot<L::Resource>>,
    driver: Arc<tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<Recovery>>>>,
    generations: Arc<AtomicU64>,
    failure: watch::Sender<Option<Arc<L::Error>>>,
}
impl<L: Lifecycle> Clone for Supervisor<L> {
    fn clone(&self) -> Self {
        Self {
            lifecycle: self.lifecycle.clone(),
            owner: self.owner.clone(),
            snapshot: self.snapshot.clone(),
            recovery: self.recovery.clone(),
            attributes: self.attributes.clone(),
            config: self.config.clone(),
            sender: self.sender.clone(),
            driver: self.driver.clone(),
            generations: self.generations.clone(),
            failure: self.failure.clone(),
        }
    }
}
impl<L: Lifecycle> Supervisor<L> {
    /// Validate before constructing channels or starting lifecycle work.
    pub fn try_new(lifecycle: L, config: Config) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Self::new(lifecycle, config))
    }
    pub fn try_start(lifecycle: L, config: Config) -> Result<Self, ConfigError> {
        let supervisor = Self::try_new(lifecycle, config)?;
        supervisor.begin();
        Ok(supervisor)
    }
    pub fn start(lifecycle: L, config: Config) -> Self {
        let supervisor = Self::new(lifecycle, config);
        supervisor.begin();
        supervisor
    }
    /// Construct without spawning tasks, so hooks and setters can be configured first.
    pub fn new(lifecycle: L, config: Config) -> Self {
        let lifecycle = Arc::new(lifecycle);
        let (stop, stopping) = watch::channel(false);
        let (sender, snapshot) = watch::channel(Snapshot {
            state: ResourceState::Disconnected,
            handle: None,
        });
        let (recovery, requests) = mpsc::unbounded_channel();
        let attributes = Arc::new(Mutex::new(Attributes {
            registrations: Registrations::default(),
            last: None,
            on_created: None,
            after_created: None,
        }));
        drop(stopping);
        Self {
            lifecycle,
            owner: Arc::new(Owner {
                stop,
                started: AtomicBool::new(false),
            }),
            snapshot,
            recovery,
            attributes,
            config,
            sender,
            driver: Arc::new(tokio::sync::Mutex::new(Some(requests))),
            generations: Arc::new(AtomicU64::new(0)),
            failure: watch::channel(None).0,
        }
    }
    /// Start supervision once. Acquisition starts an unstarted supervisor automatically.
    pub fn begin(&self) {
        if self.sender.send_if_modified(|status| {
            if status.state == ResourceState::Stopped {
                status.state = ResourceState::Connecting;
                true
            } else {
                false
            }
        }) {
            self.owner.stop.send_replace(false);
            self.owner.started.store(true, Ordering::Release);
            self.launch();
            return;
        }
        if !*self.owner.stop.borrow() && !self.owner.started.swap(true, Ordering::AcqRel) {
            publish(&self.sender, ResourceState::Connecting, None);
            self.launch();
        }
    }
    fn launch(&self) {
        let driver = self.driver.clone();
        let lifecycle = self.lifecycle.clone();
        let config = self.config.clone();
        let stopping = self.owner.stop.subscribe();
        let sender = self.sender.clone();
        let attributes = self.attributes.clone();
        let generations = self.generations.clone();
        let failure = self.failure.clone();
        tokio::spawn(async move {
            let mut guard = driver.lock().await;
            let requests = guard.take().expect("only one lifecycle worker");
            *guard = Some(
                maintain(
                    lifecycle,
                    config,
                    stopping,
                    WorkerStatus { sender, failure },
                    requests,
                    attributes,
                    generations,
                )
                .await,
            );
        });
    }
    /// Restart a fully stopped supervisor, preserving setters and public handles.
    /// Returns false if it is already running. Pools remain permanently stopped.
    pub async fn restart(&self) -> bool {
        if self.state() != ResourceState::Stopped {
            return false;
        }
        let guard = self.driver.lock().await;
        if self.state() != ResourceState::Stopped {
            return false;
        }
        self.owner.stop.send_replace(false);
        self.owner.started.store(true, Ordering::Release);
        publish(&self.sender, ResourceState::Connecting, None);
        drop(guard);
        self.launch();
        true
    }
    pub fn subscribe(&self) -> watch::Receiver<ResourceStatus<L::Resource>> {
        self.snapshot.clone()
    }
    pub fn current(&self) -> Option<ResourceHandle<L::Resource>> {
        self.snapshot.borrow().handle.clone()
    }
    /// Read an owned attribute from the latest created resource, even during recovery.
    /// Returns None after awaited stop; stored values remain readable.
    pub fn with_latest<T>(&self, read: impl FnOnce(&L::Resource) -> T) -> Option<T> {
        self.attributes.lock().unwrap().last.as_deref().map(read)
    }
    /// Replay a synchronous attribute setter on the latest resource and all replacements.
    /// Setters must be short, nonblocking, and must not reenter this supervisor.
    pub fn set_attribute(&self, name: impl Into<String>, setter: AttributeSetter<L::Resource>) {
        let mut attributes = self.attributes.lock().unwrap();
        if let Some(resource) = &attributes.last {
            setter(resource);
        }
        attributes.registrations.insert_setter(name.into(), setter);
    }
    /// Store a typed value that is readable before creation and replay it on replacement.
    pub fn set_value<T: Clone + Send + Sync + 'static>(
        &self,
        name: impl Into<String>,
        value: T,
        apply: impl Fn(&L::Resource, &T) + Send + Sync + 'static,
    ) {
        let name = name.into();
        let (stored, setter) = value_setter(value, apply);
        let mut attributes = self.attributes.lock().unwrap();
        attributes.registrations.store(name.clone(), stored);
        if let Some(resource) = &attributes.last {
            setter(resource);
        }
        attributes.registrations.insert_setter(name, setter);
    }
    pub fn value<T: Clone + Send + Sync + 'static>(&self, name: &str) -> Option<T> {
        self.attributes.lock().unwrap().registrations.value(name)
    }
    pub fn set_on_resource_created(&self, hook: CreatedHook<L::Resource, L::Error>) {
        self.attributes.lock().unwrap().on_created = Some(hook);
    }
    pub fn is_connected(&self) -> bool {
        self.current()
            .is_some_and(|handle| self.lifecycle.is_connected(&handle.resource))
    }
    pub fn stopped(&self) -> watch::Receiver<bool> {
        self.owner.stop.subscribe()
    }
    pub fn state(&self) -> ResourceState {
        self.snapshot.borrow().state
    }
    /// Inspect the latched cause without implicitly resetting it. Error values
    /// may contain secrets; the framework does not format this cause in logs.
    pub fn failure(&self) -> Option<Arc<L::Error>> {
        self.failure.borrow().clone()
    }
    /// Explicitly permit a failed lifecycle to try again after teardown. Does not
    /// restart a stopped supervisor. Stop takes precedence over reset.
    pub fn reset_failure(&self) -> bool {
        if *self.owner.stop.borrow() {
            return false;
        }
        self.failure
            .send_if_modified(|cause| cause.take().is_some())
    }
    pub async fn acquire(&self) -> Result<ResourceHandle<L::Resource>, Error<L::Error>> {
        self.begin();
        self.acquire_running().await
    }
    /// Acquire readiness within one overall timeout, following lifecycle recovery.
    /// Zero returns Timeout without starting supervision; an already stopped
    /// supervisor returns Stopped. The timeout starts when this future is polled.
    pub async fn acquire_with_timeout(
        &self,
        duration: Duration,
    ) -> Result<ResourceHandle<L::Resource>, Error<L::Error>> {
        with_call_timeout(duration, self.stopped(), self.acquire()).await
    }
    pub(crate) async fn acquire_running(
        &self,
    ) -> Result<ResourceHandle<L::Resource>, Error<L::Error>> {
        let mut snapshot = self.snapshot.clone();
        loop {
            if *self.owner.stop.borrow() || snapshot.has_changed().is_err() {
                return Err(Error::Stopped);
            }
            if let Some(cause) = self.failure() {
                return Err(Error::Lifecycle(cause));
            }
            if let Some(handle) = snapshot.borrow_and_update().handle.clone() {
                return Ok(handle);
            }
            snapshot.changed().await.map_err(|_| Error::Stopped)?;
        }
    }
    /// Request recovery without waiting for teardown.
    pub fn request_recovery(&self, handle: &ResourceHandle<L::Resource>, terminal: bool) {
        let _ = self.submit_recovery(handle, terminal, None);
    }
    /// Wait for bounded teardown. Does not wait for reconnection.
    pub async fn recover(
        &self,
        handle: &ResourceHandle<L::Resource>,
        terminal: bool,
    ) -> Result<(), Error<L::Error>> {
        let (sender, receiver) = oneshot::channel();
        if self.submit_recovery(handle, terminal, Some(sender)) {
            receiver.await.map_err(|_| Error::Stopped)?;
        }
        Ok(())
    }
    pub async fn recover_current(&self, terminal: bool) -> Result<(), Error<L::Error>> {
        if let Some(handle) = self.current() {
            self.recover(&handle, terminal).await?;
        }
        Ok(())
    }
    fn submit_recovery(
        &self,
        handle: &ResourceHandle<L::Resource>,
        terminal: bool,
        completed: Option<oneshot::Sender<()>>,
    ) -> bool {
        // Pointer identity also rejects handles from a different supervisor.
        let mut matched = self.sender.send_if_modified(|snapshot| {
            if snapshot
                .handle
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(&current.resource, &handle.resource))
            {
                snapshot.handle = None;
                snapshot.state = ResourceState::Recovering;
                true
            } else {
                false
            }
        });
        if !matched && self.state() == ResourceState::Recovering {
            matched = self
                .attributes
                .lock()
                .unwrap()
                .last
                .as_ref()
                .is_some_and(|resource| Arc::ptr_eq(resource, &handle.resource));
        }
        if matched {
            let _ = self.recovery.send(Recovery {
                generation: handle.generation,
                terminal,
                completed,
            });
        }
        matched
    }
    /// Run once. By default failures request recovery and return the original error
    /// without replay; `operation_failure` can retain a healthy generation instead.
    /// Dropping this future cancels the operation; protocol-specific cancellation safety
    /// remains the client's responsibility. Shutdown cancels operations driven by this method.
    pub async fn execute<F, Fut, T>(&self, operation: F) -> Result<T, Error<L::Error>>
    where
        F: FnOnce(Arc<L::Resource>) -> Fut,
        Fut: Future<Output = Result<T, L::Error>>,
    {
        self.begin();
        self.execute_running(operation).await
    }
    /// Run once within an overall timeout covering readiness, the operation and
    /// any bounded failure teardown. Never replay. Zero does not start work.
    /// Shutdown takes precedence. Expiry drops the operation future; adapters
    /// remain responsible for poisoning interrupted protocol exchanges.
    pub async fn execute_with_timeout<F, Fut, T>(
        &self,
        duration: Duration,
        operation: F,
    ) -> Result<T, Error<L::Error>>
    where
        F: FnOnce(Arc<L::Resource>) -> Fut,
        Fut: Future<Output = Result<T, L::Error>>,
    {
        with_call_timeout(duration, self.stopped(), self.execute(operation)).await
    }
    pub(crate) async fn execute_running<F, Fut, T>(
        &self,
        operation: F,
    ) -> Result<T, Error<L::Error>>
    where
        F: FnOnce(Arc<L::Resource>) -> Fut,
        Fut: Future<Output = Result<T, L::Error>>,
    {
        let handle = self.acquire_running().await?;
        let mut stopping = self.owner.stop.subscribe();
        let result = tokio::select! {
            biased;
            _ = shutdown(&mut stopping) => return Err(Error::Stopped),
            result = operation(handle.resource.clone()) => result,
        };
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                if self.lifecycle.operation_failure(&error) == OperationFailurePolicy::Recover {
                    let _ = self
                        .recover(&handle, self.lifecycle.is_terminal(&error))
                        .await;
                }
                Err(Error::Operation(error))
            }
        }
    }
    /// Retry an operation only when `retry_if` accepts its error and attempts remain.
    /// Each invocation of `operation` must create a fresh future and safely repeat
    /// the remote action. The next attempt waits for lifecycle readiness; a retained
    /// healthy generation may be reused without replacement.
    /// Exhaustion returns the last operation error; the overall deadline returns
    /// [`Error::Timeout`]. Shutdown returns [`Error::Stopped`]; cancellation drops
    /// the active attempt without starting another. As with [`Self::execute`],
    /// resource-specific cancellation safety is the adapter's responsibility.
    /// Terminal classification controls teardown, not whether replay is allowed.
    pub async fn execute_with_retry<F, Fut, T, P>(
        &self,
        policy: RetryPolicy,
        operation: F,
        retry_if: P,
    ) -> Result<T, Error<L::Error>>
    where
        F: FnMut(Arc<L::Resource>) -> Fut,
        Fut: Future<Output = Result<T, L::Error>>,
        P: FnMut(&L::Error) -> bool,
    {
        if policy.timeout.is_zero() {
            return Err(Error::Timeout);
        }
        self.begin();
        self.execute_with_retry_running(policy, operation, retry_if)
            .await
    }
    pub(crate) async fn execute_with_retry_running<F, Fut, T, P>(
        &self,
        policy: RetryPolicy,
        mut operation: F,
        mut retry_if: P,
    ) -> Result<T, Error<L::Error>>
    where
        F: FnMut(Arc<L::Resource>) -> Fut,
        Fut: Future<Output = Result<T, L::Error>>,
        P: FnMut(&L::Error) -> bool,
    {
        if policy.timeout.is_zero() {
            return Err(Error::Timeout);
        }
        let stopping = self.owner.stop.subscribe();
        let retry_stopping = stopping.clone();
        let attempts = async {
            for attempt in 1..=policy.max_attempts.get() {
                // Do not implicitly restart a stopped supervisor between attempts.
                if *retry_stopping.borrow() {
                    return Err(Error::Stopped);
                }
                match self.execute_running(&mut operation).await {
                    Err(Error::Operation(error))
                        if attempt < policy.max_attempts.get() && retry_if(&error) => {}
                    result => return result,
                }
            }
            unreachable!("a nonzero final attempt always returns")
        };
        with_call_timeout(policy.timeout, stopping, attempts).await
    }
    /// Idempotent, cancellation-safe shutdown. Clears resource-derived caches,
    /// preserving stored configuration and registrations. External handles and
    /// abandoned timed-out hook tasks can retain resources beyond this return.
    pub async fn stop(&self) {
        self.request_stop();
        let mut snapshot = self.snapshot.clone();
        loop {
            if snapshot.borrow_and_update().state == ResourceState::Stopped {
                // Wait for the worker to return its receiver and release lifecycle state.
                drop(self.driver.lock().await);
                return;
            }
            if snapshot.changed().await.is_err() {
                return;
            }
        }
    }
    pub(crate) fn request_stop(&self) {
        self.owner.stop.send_replace(true);
        if !self.owner.started.load(Ordering::Acquire) {
            publish(&self.sender, ResourceState::Stopped, None);
        } else {
            self.sender.send_if_modified(|status| {
                if status.state == ResourceState::Stopped {
                    false
                } else {
                    status.state = ResourceState::Stopping;
                    status.handle = None;
                    true
                }
            });
        }
    }
}
fn publish<R>(
    sender: &watch::Sender<Snapshot<R>>,
    state: ResourceState,
    handle: Option<ResourceHandle<R>>,
) {
    sender.send_replace(Snapshot { state, handle });
}
