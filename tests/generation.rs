use etherbird::{Generation, GenerationMismatch};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[test]
fn cloned_tokens_match_but_replacements_and_other_resources_do_not() {
    let original = Generation::new();
    let handle = original.bind(String::from("remote handle"));
    assert_eq!(handle.get(&original.clone()).unwrap(), "remote handle");
    // These could have equal adapter counters but are distinct identities.
    let other_slot = Generation::new();
    let replacement = Generation::new();
    assert_eq!(handle.get(&other_slot), Err(GenerationMismatch));
    assert_eq!(handle.get(&replacement), Err(GenerationMismatch));
    assert_eq!(handle.get(&Generation::default()), Err(GenerationMismatch));
}

#[test]
fn bound_values_are_redacted_without_requiring_debug_on_the_value() {
    struct Secret;
    let generation = Generation::new();
    assert_eq!(
        format!("{:?}", generation.bind(Secret)),
        "GenerationValue(<redacted>)"
    );
    let error = generation.bind(42).get(&Generation::new()).unwrap_err();
    assert_eq!(
        error.to_string(),
        "value belongs to a different resource generation"
    );
}

#[test]
fn tokens_keep_only_the_binding_and_do_not_copy_or_drop_its_value() {
    struct Value(Arc<AtomicUsize>);
    impl Drop for Value {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let token = Generation::new();
    let binding = token.bind(Value(drops.clone()));
    let current = token.clone();
    drop(token);
    assert!(binding.get(&current).is_ok());
    drop(current);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(binding);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
