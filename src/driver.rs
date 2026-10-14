//! Lifecycle hook execution and replacement driver.
use super::*;

fn log_failure<L: Lifecycle>(lifecycle: &L, config: &Config, name: &str, error: &L::Error) {
    if lifecycle.is_expected(error) {
        tracing::warn!(resource = config.resource_name, hook = name, %error);
        tracing::debug!(resource = config.resource_name, hook = name, ?error);
    } else {
        tracing::error!(resource = config.resource_name, hook = name, %error);
    }
}
fn log_late_failure<L: Lifecycle>(lifecycle: &L, config: &Config, name: &str, error: &L::Error) {
    if classifiable_hook(name) && lifecycle.lifecycle_failure(error) == LifecycleFailurePolicy::Fail
    {
        if lifecycle.is_expected(error) {
            tracing::debug!(
                resource = config.resource_name,
                hook = name,
                "abandoned lifecycle hook failed (cause redacted)"
            );
        } else {
            tracing::error!(
                resource = config.resource_name,
                hook = name,
                "abandoned lifecycle hook failed (cause redacted)"
            );
        }
        return;
    }
    if lifecycle.is_expected(error) {
        tracing::debug!(
            resource = config.resource_name,
            hook = name,
            ?error,
            "abandoned hook failed"
        );
    } else {
        log_failure(lifecycle, config, name, error);
    }
}
fn classifiable_hook(name: &str) -> bool {
    matches!(
        name,
        "create"
            | "on_resource_created"
            | "connect"
            | "late connect"
            | "setup"
            | "watch_disconnect"
    )
}
struct HookFailure<E> {
    terminal: bool,
    cause: Option<Arc<E>>,
}
impl<E> HookFailure<E> {
    fn retry() -> Self {
        Self {
            terminal: false,
            cause: None,
        }
    }
}
fn hook_failure<L: Lifecycle>(
    lifecycle: &L,
    config: &Config,
    name: &str,
    error: L::Error,
) -> HookFailure<L::Error> {
    let terminal = lifecycle.is_terminal(&error);
    if classifiable_hook(name)
        && lifecycle.lifecycle_failure(&error) == LifecycleFailurePolicy::Fail
    {
        if lifecycle.is_expected(&error) {
            tracing::warn!(
                resource = config.resource_name,
                hook = name,
                "non-retryable lifecycle failure (cause redacted)"
            );
            tracing::debug!(
                resource = config.resource_name,
                hook = name,
                "non-retryable lifecycle failure (cause redacted)"
            );
        } else {
            tracing::error!(
                resource = config.resource_name,
                hook = name,
                "non-retryable lifecycle failure (cause redacted)"
            );
        }
        HookFailure {
            terminal,
            cause: Some(Arc::new(error)),
        }
    } else {
        log_failure(lifecycle, config, name, &error);
        HookFailure {
            terminal,
            cause: None,
        }
    }
}
async fn bounded<L: Lifecycle>(
    lifecycle: Arc<L>,
    config: Config,
    name: &'static str,
    duration: Option<Duration>,
    future: impl Future<Output = Result<(), L::Error>> + Send + 'static,
) -> Result<(), HookFailure<L::Error>> {
    let mut task = tokio::spawn(future);
    match deadline(duration, &mut task).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(hook_failure(lifecycle.as_ref(), &config, name, error)),
        Ok(Err(error)) => {
            tracing::error!(hook = name, %error, "hook task failed");
            Err(HookFailure::retry())
        }
        Err(_) => {
            tracing::warn!(
                resource = config.resource_name,
                hook = name,
                "hook abandoned after timeout"
            );
            tokio::spawn(async move {
                match task.await {
                    Ok(Err(error)) => log_late_failure(lifecycle.as_ref(), &config, name, &error),
                    Ok(Ok(())) => tracing::warn!(
                        resource = config.resource_name,
                        hook = name,
                        "abandoned hook completed after timeout"
                    ),
                    Err(error) => {
                        tracing::error!(hook = name, %error, "abandoned hook task failed")
                    }
                }
            });
            Err(HookFailure::retry())
        }
    }
}
async fn deadline<T>(
    duration: Option<Duration>,
    future: impl Future<Output = T>,
) -> Result<T, tokio::time::error::Elapsed> {
    match duration {
        Some(duration) => timeout(duration, future).await,
        None => Ok(future.await),
    }
}
async fn destroy<L: Lifecycle>(lifecycle: Arc<L>, config: Config, resource: Arc<L::Resource>) {
    let manager = lifecycle.clone();
    let _ = bounded(
        lifecycle,
        config.clone(),
        "destroy",
        config.destroy_timeout,
        async move { manager.destroy(&resource).await },
    )
    .await;
}
async fn close<L: Lifecycle>(
    lifecycle: Arc<L>,
    config: Config,
    resource: Arc<L::Resource>,
    terminal: bool,
) {
    let manager = lifecycle.clone();
    let value = resource.clone();
    let _ = bounded(
        lifecycle.clone(),
        config.clone(),
        "cleanup",
        Some(config.cleanup_timeout),
        async move { manager.cleanup(&value).await },
    )
    .await;
    if !terminal {
        let manager = lifecycle.clone();
        let value = resource.clone();
        let mut task = tokio::spawn(async move { manager.disconnect(&value).await });
        match timeout(config.disconnect_timeout, &mut task).await {
            Ok(Ok(Err(error))) => log_failure(lifecycle.as_ref(), &config, "disconnect", &error),
            Ok(Err(error)) => tracing::error!(%error, "disconnect task failed"),
            Ok(Ok(Ok(()))) => {}
            Err(_) => {
                tracing::warn!(
                    resource = config.resource_name,
                    "disconnect abandoned after timeout"
                );
                tokio::spawn(async move {
                    if let Ok(Err(error)) = task.await {
                        log_late_failure(lifecycle.as_ref(), &config, "disconnect", &error);
                    }
                    destroy(lifecycle, config, resource).await;
                });
                return;
            }
        }
    }
    destroy(lifecycle, config, resource).await;
}
async fn connect<L: Lifecycle>(
    lifecycle: Arc<L>,
    config: Config,
    resource: Arc<L::Resource>,
    stopping: &mut watch::Receiver<bool>,
) -> Result<(), HookFailure<L::Error>> {
    let manager = lifecycle.clone();
    let value = resource.clone();
    let mut task = tokio::spawn(async move { manager.connect(&value).await });
    tokio::select! {
        biased;
        _ = shutdown(stopping) => { task.abort(); let _ = task.await; Err(HookFailure::retry()) }
        result = timeout(config.connect_timeout, &mut task) => match result {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => {
                Err(hook_failure(lifecycle.as_ref(), &config, "connect", error))
            }
            Ok(Err(error)) => { tracing::error!(%error, "connect task failed"); Err(HookFailure::retry()) }
            Err(_) => {
                tracing::warn!(resource = config.resource_name, "connect abandoned after timeout");
                tokio::spawn(async move {
                    match task.await {
                        Ok(Ok(())) => close(lifecycle, config, resource, false).await,
                        Ok(Err(error)) => log_late_failure(lifecycle.as_ref(), &config, "late connect", &error),
                        Err(error) => tracing::error!(%error, "late connect task failed"),
                    }
                });
                Err(HookFailure::retry())
            }
        }
    }
}
pub(super) struct WorkerStatus<L: Lifecycle> {
    pub(super) sender: watch::Sender<Snapshot<L::Resource>>,
    pub(super) failure: watch::Sender<Option<Arc<L::Error>>>,
}
fn latch_failure<L: Lifecycle>(status: &WorkerStatus<L>, cause: Arc<L::Error>) {
    status.failure.send_replace(Some(cause));
    publish(&status.sender, ResourceState::Failed, None);
}
async fn wait_for_reset<E>(
    failure: &watch::Sender<Option<Arc<E>>>,
    stopping: &mut watch::Receiver<bool>,
    requests: &mut mpsc::UnboundedReceiver<Recovery>,
) {
    let mut changes = failure.subscribe();
    loop {
        if changes.borrow_and_update().is_none() {
            return;
        }
        tokio::select! {
            biased;
            _ = shutdown(stopping) => return,
            _ = changes.changed() => {},
            request = requests.recv() => {
                match request {
                    Some(request) => {
                        if let Some(completed) = request.completed {
                            let _ = completed.send(());
                        }
                    }
                    None => return,
                }
            }
        }
    }
}
pub(super) async fn maintain<L: Lifecycle>(
    lifecycle: Arc<L>,
    config: Config,
    mut stopping: watch::Receiver<bool>,
    status: WorkerStatus<L>,
    mut requests: mpsc::UnboundedReceiver<Recovery>,
    attributes: Arc<Mutex<Attributes<L::Resource, L::Error>>>,
    generations: Arc<AtomicU64>,
) -> mpsc::UnboundedReceiver<Recovery> {
    let sender = &status.sender;
    let mut delay = config.retry_delay;
    loop {
        while let Ok(request) = requests.try_recv() {
            if let Some(completed) = request.completed {
                let _ = completed.send(());
            }
        }
        if *stopping.borrow() {
            break;
        }
        publish(sender, ResourceState::Connecting, None);
        let manager = lifecycle.clone();
        let mut creating = tokio::spawn(async move { manager.create().await });
        let created = tokio::select! {
            biased;
            _ = shutdown(&mut stopping) => { creating.abort(); let _ = creating.await; break },
            result = deadline(config.create_timeout, &mut creating) => result,
        };
        match created {
            Ok(Ok(Ok(resource))) => {
                let resource = Arc::new(resource);
                let (on_created, after_created) = {
                    let mut attributes = attributes.lock().unwrap();
                    attributes.last = Some(resource.clone());
                    (
                        attributes.on_created.clone(),
                        attributes.after_created.clone(),
                    )
                };
                let manager = lifecycle.clone();
                let value = resource.clone();
                let settings = attributes.clone();
                let created = bounded(
                    lifecycle.clone(),
                    config.clone(),
                    "on_resource_created",
                    config.created_timeout,
                    async move {
                        manager.on_resource_created(&value).await?;
                        if let Some(hook) = on_created {
                            hook(value.clone()).await?;
                        }
                        {
                            let settings = settings.lock().unwrap();
                            for setter in settings.setters.values() {
                                setter(&value);
                            }
                        }
                        if let Some(hook) = after_created {
                            hook(value).await?;
                        }
                        Ok(())
                    },
                );
                let created = tokio::select! { biased; _ = shutdown(&mut stopping) => Err(HookFailure::retry()), result = created => result };
                let connected = if created.is_ok() {
                    connect(
                        lifecycle.clone(),
                        config.clone(),
                        resource.clone(),
                        &mut stopping,
                    )
                    .await
                } else {
                    created
                };
                let manager = lifecycle.clone();
                let value = resource.clone();
                let setup = if connected.is_ok() {
                    tokio::select! {
                        biased;
                        _ = shutdown(&mut stopping) => Err(HookFailure::retry()),
                        ready = bounded(lifecycle.clone(), config.clone(), "setup", Some(config.setup_timeout),
                            async move { manager.setup(&value).await }) => ready,
                    }
                } else {
                    connected
                };
                if setup.is_ok() {
                    let generation = generations.fetch_add(1, Ordering::AcqRel) + 1;
                    delay = config.retry_delay;
                    tracing::info!(resource = config.resource_name, "connected");
                    publish(
                        sender,
                        ResourceState::Connected,
                        Some(ResourceHandle {
                            resource: resource.clone(),
                            generation,
                        }),
                    );
                    let mut watchdog = lifecycle
                        .watch_disconnect(resource.clone())
                        .map(tokio::spawn);
                    let mut watchdog_done = false;
                    let mut completed = None;
                    let terminal = loop {
                        tokio::select! {
                            biased;
                            _ = shutdown(&mut stopping) => break HookFailure::retry(),
                            request = requests.recv() => {
                                match request {
                                    Some(request) if request.generation == generation => { completed = request.completed; break HookFailure { terminal: request.terminal, cause: None } },
                                    Some(request) => { if let Some(completed) = request.completed { let _ = completed.send(()); } continue },
                                    None => break HookFailure::retry(),
                                }
                            }
                            result = async { match watchdog.as_mut() { Some(task) => task.await, None => std::future::pending().await } } => {
                                watchdog_done = true;
                                break match result { Ok(Err(error)) => hook_failure(lifecycle.as_ref(), &config, "watch_disconnect", error), _ => HookFailure::retry() };
                            }
                        }
                    };
                    if !watchdog_done && let Some(watchdog) = watchdog {
                        watchdog.abort();
                        let _ = watchdog.await;
                    }
                    let state = if *stopping.borrow() {
                        ResourceState::Stopping
                    } else {
                        ResourceState::Recovering
                    };
                    publish(sender, state, None);
                    let failed = terminal.cause.is_some();
                    if let Some(cause) = terminal.cause {
                        latch_failure(&status, cause);
                    }
                    close(
                        lifecycle.clone(),
                        config.clone(),
                        resource,
                        terminal.terminal,
                    )
                    .await;
                    if let Some(completed) = completed {
                        let _ = completed.send(());
                    }
                    if failed {
                        wait_for_reset(&status.failure, &mut stopping, &mut requests).await;
                    }
                    continue;
                }
                let failure = setup.expect_err("failed setup");
                let failed = failure.cause.is_some();
                if let Some(cause) = failure.cause {
                    latch_failure(&status, cause);
                } else {
                    publish(sender, ResourceState::Disconnected, None);
                }
                close(
                    lifecycle.clone(),
                    config.clone(),
                    resource,
                    failure.terminal,
                )
                .await;
                if failed {
                    wait_for_reset(&status.failure, &mut stopping, &mut requests).await;
                    continue;
                }
            }
            Ok(Ok(Err(error))) => {
                let failure = hook_failure(lifecycle.as_ref(), &config, "create", error);
                if let Some(cause) = failure.cause {
                    latch_failure(&status, cause);
                    wait_for_reset(&status.failure, &mut stopping, &mut requests).await;
                    continue;
                }
            }
            Ok(Err(error)) => tracing::error!(%error, "creation task failed"),
            Err(_) => {
                tracing::warn!("resource creation timed out");
                let manager = lifecycle.clone();
                let settings = config.clone();
                tokio::spawn(async move {
                    if let Ok(Ok(resource)) = creating.await {
                        close(manager, settings, Arc::new(resource), false).await;
                    }
                });
            }
        }
        publish(sender, ResourceState::Disconnected, None);
        tracing::info!(resource = config.resource_name, retry_delay = ?delay, "retrying connection");
        tokio::select! { biased; _ = shutdown(&mut stopping) => break, _ = sleep(delay) => {} }
        delay = delay.saturating_mul(2).min(config.max_retry_delay);
    }
    while let Ok(request) = requests.try_recv() {
        if let Some(completed) = request.completed {
            let _ = completed.send(());
        }
    }
    attributes.lock().unwrap().last = None;
    status.failure.send_replace(None);
    publish(sender, ResourceState::Stopped, None);
    requests
}
