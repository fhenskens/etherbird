//! Identity for adapter-owned state that belongs to one connection generation.
use std::{fmt, sync::Arc};

/// Opaque identity for one resource generation, independent of numeric counters.
///
/// Create a fresh token whenever an adapter replaces its session, including when
/// it reconnects in place. Clone the token only within that same generation.
/// Separate clients and pool slots cannot accidentally match equal counters.
/// Tokens are process-local; they are not persistent controller identities.
/// Keeping a token alive does not keep a connection alive or prove readiness.
///
/// ```
/// use etherbird::Generation;
/// let session = Generation::new();
/// let handle = session.bind(42);
/// assert_eq!(handle.get(&session.clone()), Ok(&42));
/// let replacement = Generation::new();
/// assert!(handle.get(&replacement).is_err());
/// ```
#[derive(Clone, Debug, Default)]
pub struct Generation(Arc<()>);
impl Generation {
    pub fn new() -> Self {
        Self::default()
    }
    /// Bind a value without exposing an unchecked value accessor.
    pub fn bind<T>(&self, value: T) -> GenerationValue<T> {
        GenerationValue {
            generation: self.clone(),
            value,
        }
    }
}

/// A value accessible only with its originating generation token.
///
/// Adapters should keep their token private and check live connection state
/// before exposing the value. This check does not invalidate previously borrowed
/// references, prevent callers copying values, or enforce remote validity.
/// Debug output never includes the value. There is no unchecked extraction API.
pub struct GenerationValue<T> {
    generation: Generation,
    value: T,
}
impl<T> GenerationValue<T> {
    pub fn get(&self, current: &Generation) -> Result<&T, GenerationMismatch> {
        if Arc::ptr_eq(&self.generation.0, &current.0) {
            Ok(&self.value)
        } else {
            Err(GenerationMismatch)
        }
    }
}
impl<T> fmt::Debug for GenerationValue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GenerationValue(<redacted>)")
    }
}

/// The supplied token belongs to a different resource generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenerationMismatch;
impl fmt::Display for GenerationMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("value belongs to a different resource generation")
    }
}
impl std::error::Error for GenerationMismatch {}
