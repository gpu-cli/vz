use objc2_virtualization::VZGenericMachineIdentifier;

use crate::VzError;

/// Generate a new opaque generic machine identifier payload for Linux VMs.
///
/// Persist the returned bytes and reuse them when restoring VM snapshots.
pub fn generate_generic_machine_identifier_data() -> Result<Vec<u8>, VzError> {
    // SAFETY: `new` returns a valid identifier object managed by ARC.
    let identifier = unsafe { VZGenericMachineIdentifier::new() };
    // SAFETY: `dataRepresentation` returns an immutable NSData owned by ARC.
    let payload = unsafe { identifier.dataRepresentation() };
    Ok(payload.to_vec())
}
