//! VsockListener / VmHandle / AcceptedVsockStream type-shape tests.
//!
//! `VmHandle` is constructed inside the framework callback from a real
//! `VZVirtioSocketDevice`, so these tests cover the surface shape and trait
//! derivations rather than constructing handles directly. End-to-end accept
//! semantics are exercised by the runtime/stack E2E suites.

#![allow(clippy::unwrap_used)]

use std::collections::HashSet;

use vz::VmHandle;

#[test]
fn vm_handle_traits_are_derived() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    fn assert_copy<T: Copy>() {}
    fn assert_eq<T: Eq>() {}
    fn assert_hash<T: std::hash::Hash>() {}

    assert_send::<VmHandle>();
    assert_sync::<VmHandle>();
    assert_copy::<VmHandle>();
    assert_eq::<VmHandle>();
    assert_hash::<VmHandle>();
}

#[test]
fn vm_handle_can_index_a_hashset() {
    // Property: VmHandle is usable as a hashmap/hashset key. We can't build
    // one directly (it's only minted by the accept-callback path) but we can
    // assert the traits the consumer needs are wired up. Empty set is enough
    // to exercise the bound — the compiler will reject if Hash/Eq are missing.
    let set: HashSet<VmHandle> = HashSet::new();
    assert!(set.is_empty());
}
