//! Lease admission, queued dispatch and entry retirement.
use super::*;

pub(super) async fn borrow<L: Lifecycle>(
    shared: Arc<Shared<L>>,
) -> Result<ResourceLease<L>, Error<L::Error>> {
    shared.waiters.fetch_add(1, Ordering::AcqRel);
    let _waiter = Waiter(shared.clone());
    let mut changed = shared.changed.subscribe();
    let mut stopping = shared.stopping.subscribe();
    loop {
        if let Some(lease) = try_borrow(&shared)? {
            return Ok(lease);
        }
        tokio::select! { biased; _ = crate::shutdown(&mut stopping) => return Err(Error::Stopped), _ = changed.changed() => {} }
    }
}
// Queued acquisition is checked directly by the dispatcher. Only public
// borrowers register an independent capacity/shutdown wait.
fn try_borrow<L: Lifecycle>(
    shared: &Arc<Shared<L>>,
) -> Result<Option<ResourceLease<L>>, Error<L::Error>> {
    let mut contents = shared.contents.lock().unwrap();
    if *shared.stopping.borrow() {
        return Err(Error::Stopped);
    }
    if let Some(lease) = reserve_ready(shared, &contents) {
        return Ok(Some(lease));
    }
    if let Some(cause) = all_failed(&contents) {
        return Err(Error::Lifecycle(cause));
    }
    let demand = shared.waiters.load(Ordering::Acquire)
        + contents.queue.len()
        + contents
            .entries
            .iter()
            .filter(|entry| entry.leased.load(Ordering::Acquire))
            .count();
    if contents.entries.len() + contents.retiring < shared.config.max_size.min(demand) {
        shared.grow(&mut contents);
    }
    Ok(None)
}
pub(super) fn all_failed<L: Lifecycle>(contents: &Contents<L>) -> Option<Arc<L::Error>> {
    let mut first = None;
    if contents.entries.is_empty() {
        return None;
    }
    for entry in &contents.entries {
        let cause = entry.supervisor.failure()?;
        if first.is_none() {
            first = Some(cause);
        }
    }
    first
}
pub(super) fn reserve_ready<L: Lifecycle>(
    shared: &Arc<Shared<L>>,
    contents: &Contents<L>,
) -> Option<ResourceLease<L>> {
    for entry in &contents.entries {
        if entry.supervisor.state() == ResourceState::Connected
            && entry
                .leased
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            return Some(ResourceLease {
                entry: entry.clone(),
                shared: shared.clone(),
                owner: None,
            });
        }
    }
    None
}
struct Waiter<L: Lifecycle>(Arc<Shared<L>>);
impl<L: Lifecycle> Drop for Waiter<L> {
    fn drop(&mut self) {
        self.0.waiters.fetch_sub(1, Ordering::AcqRel);
        self.0.dispatcher.notify_one();
    }
}
pub(super) async fn run<L: Lifecycle>(shared: Arc<Shared<L>>) {
    let mut stopping = shared.stopping.subscribe();
    let mut retiring = JoinSet::new();
    let mut reaper = Box::pin(sleep(
        shared.config.idle_timeout.min(Duration::from_secs(1)),
    ));
    loop {
        let queued = {
            let mut contents = shared.contents.lock().unwrap();
            contents.queue.retain(&mut |item| !item.is_cancelled());
            !contents.queue.is_empty()
        };
        let admission = if queued {
            match try_borrow(&shared) {
                Ok(lease) => lease.map(Ok),
                Err(error) => Some(Err(error)),
            }
        } else {
            None
        };
        tokio::select! {
            biased;
            _ = crate::shutdown(&mut stopping) => break,
            admission = async { match admission { Some(admission) => admission, None => std::future::pending().await } } => {
                // Reconsider priority after capacity becomes available.
                let item = shared.contents.lock().unwrap().queue.pop();
                if let Some(item) = item && !item.cancelled.load(Ordering::Acquire) { shared.operations.lock().unwrap().spawn((item.job)(admission)); }
            }
            _ = shared.dispatcher.notified() => {},
            _ = std::future::poll_fn(|cx| shared.operations.lock().unwrap().poll_join_next(cx)),
                if !shared.operations.lock().unwrap().is_empty() => {},
            _ = retiring.join_next(), if !retiring.is_empty() => {
                shared.contents.lock().unwrap().retiring -= 1; shared.notify();
            }
            _ = &mut reaper => {
                let mut contents = shared.contents.lock().unwrap();
                if contents.queue.is_empty() && shared.waiters.load(Ordering::Acquire) == 0 {
                    let mut index = 0;
                    while index < contents.entries.len() && contents.entries.len() > shared.config.min_size {
                        let entry = &contents.entries[index];
                        if entry.supervisor.failure().is_none() && !entry.leased.load(Ordering::Acquire) && entry.idle_since.lock().unwrap().elapsed() >= shared.config.idle_timeout {
                            let entry = contents.entries.remove(index); contents.retiring += 1;
                            retiring.spawn(stop_entry(entry)); shared.notify();
                        } else { index += 1; }
                    }
                }
                reaper.as_mut().reset(Instant::now() + shared.config.idle_timeout.min(Duration::from_secs(1)));
            }
        }
    }
    // Fast admission holds contents while registering its task. Crossing this
    // barrier after stopping is signalled ensures no task can arrive after drain.
    let entries = {
        let mut contents = shared.contents.lock().unwrap();
        contents.queue.clear();
        std::mem::take(&mut contents.entries)
    };
    shared.operations.lock().unwrap().abort_all();
    while std::future::poll_fn(|cx| shared.operations.lock().unwrap().poll_join_next(cx))
        .await
        .is_some()
    {}
    for entry in entries {
        retiring.spawn(stop_entry(entry));
    }
    while retiring.join_next().await.is_some() {}
    *shared.last_resource.lock().unwrap() = None;
    shared.contents.lock().unwrap().retiring = 0;
    shared.done.send_replace(true);
    shared.notify();
}
pub(super) async fn stop_entry<L: Lifecycle>(entry: Arc<Entry<L>>) {
    entry.supervisor.stop().await;
    let relay = entry.relay.lock().unwrap().take();
    if let Some(relay) = relay {
        let _ = relay.await;
    }
}
