//! Adapter helpers without pooling, sockets, protocol codecs or automatic replay.
use etherbird::{DisconnectOnDrop, Generation, GenerationValue};
use std::{io, time::Duration};
use tokio::sync::{Mutex, watch};

struct Session {
    generation: Generation,
    ownership: Mutex<()>,
    connected: watch::Sender<bool>,
}
impl Session {
    fn new() -> Self {
        Self {
            generation: Generation::new(),
            ownership: Mutex::new(()),
            connected: watch::channel(true).0,
        }
    }
    async fn read_pair(&self, handle: &GenerationValue<u16>) -> io::Result<(u16, u16)> {
        let mut scope = DisconnectOnDrop::new(self.ownership.lock().await, &self.connected);
        if !*self.connected.borrow() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "session retired",
            ));
        }
        let remote = match handle.get(&self.generation) {
            Ok(remote) => *remote,
            Err(error) => {
                scope.complete(); // Local stale-handle rejection leaves this session healthy.
                return Err(io::Error::new(io::ErrorKind::InvalidInput, error));
            }
        };
        // Stand-ins for two validated exchanges. A real stream adapter also holds
        // RetireOnDrop through each exchange's decoding, and watches connection loss.
        tokio::task::yield_now().await;
        let first = remote;
        tokio::task::yield_now().await;
        let second = remote + 1;
        scope.complete();
        Ok((first, second))
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = Session::new();
    let handle = session.generation.bind(42_u16);
    assert_eq!(session.read_pair(&handle).await?, (42, 43));

    let replacement = Session::new();
    assert_eq!(
        replacement.read_pair(&handle).await.unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(*replacement.connected.borrow());
    let fresh = replacement.generation.bind(50);
    assert_eq!(replacement.read_pair(&fresh).await?, (50, 51));

    let interrupted = Session::new();
    let result = tokio::time::timeout(Duration::from_millis(1), async {
        let _scope =
            DisconnectOnDrop::new(interrupted.ownership.lock().await, &interrupted.connected);
        // First exchange completed; cancellation can also happen before the next one.
        std::future::pending::<()>().await;
    })
    .await;
    assert!(result.is_err());
    assert!(!*interrupted.connected.borrow());
    assert!(interrupted.ownership.try_lock().is_ok());
    println!(
        "same-generation reads succeed; stale handles reject locally; interrupted scopes disconnect"
    );
    Ok(())
}
