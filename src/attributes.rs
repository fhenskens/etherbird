//! Shared registration mechanics; callers retain application order and locking.
use crate::AttributeSetter;
use std::{any::Any, collections::BTreeMap, sync::Arc};

type StoredValue = Arc<dyn Any + Send + Sync>;

pub(crate) struct Registrations<R> {
    setters: BTreeMap<String, AttributeSetter<R>>,
    values: BTreeMap<String, StoredValue>,
}
impl<R> Default for Registrations<R> {
    fn default() -> Self {
        Self {
            setters: BTreeMap::new(),
            values: BTreeMap::new(),
        }
    }
}
impl<R> Registrations<R> {
    pub fn insert_setter(&mut self, name: String, setter: AttributeSetter<R>) {
        self.setters.insert(name, setter);
    }
    pub fn store(&mut self, name: String, value: StoredValue) {
        self.values.insert(name, value);
    }
    pub fn value<T: Clone + 'static>(&self, name: &str) -> Option<T> {
        self.values.get(name)?.downcast_ref::<T>().cloned()
    }
    pub fn setters(&self) -> impl Iterator<Item = (&String, &AttributeSetter<R>)> {
        self.setters.iter()
    }
}
pub(crate) fn value_setter<R, T: Send + Sync + 'static>(
    value: T,
    apply: impl Fn(&R, &T) + Send + Sync + 'static,
) -> (StoredValue, AttributeSetter<R>) {
    let value = Arc::new(value);
    let stored = value.clone();
    (stored, Arc::new(move |resource| apply(resource, &value)))
}
