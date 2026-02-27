use super::rollback::{apply_operations_transactional, load_payload_archive};
use super::*;

pub(super) fn apply(args: ApplyArgs) -> anyhow::Result<()> {
    let patch_state_path = patch_state_path();
    apply_with_state_path(args, &patch_state_path)
}

pub(super) fn patch_state_path() -> PathBuf {
    crate::registry::vz_home().join(PATCH_STATE_FILE)
}

pub(super) fn apply_with_state_path(
    args: ApplyArgs,
    patch_state_path: &Path,
) -> anyhow::Result<()> {
    match (args.root.as_ref(), args.image.as_ref()) {
        (Some(root), None) => apply_with_root(&args.bundle, root, patch_state_path),
        (None, Some(image)) => apply_with_image(&args.bundle, image, patch_state_path),
        _ => bail!("exactly one apply target is required: --root <path> or --image <path>"),
    }
}

pub(super) fn create_delta(args: CreateDeltaArgs) -> anyhow::Result<()> {
    let base_image = expand_home(&args.base_image);
    let bundle = expand_home(&args.bundle);
    let delta = expand_home(&args.delta);
    ensure_regular_file(&base_image, "--base-image")?;
    ensure_dir(&bundle, "--bundle")?;
    ensure_output_file_parent(&delta)?;
    ensure_output_path_absent(&delta, "--delta")?;

    let workspace = TempWorkspace::new("vz-image-delta-create")?;
    let patched_image = workspace.path().join("patched.img");

    clone_or_copy_image_with_sidecars(&base_image, &patched_image, true)?;
    let manifest = verify_bundle(&bundle)?;
    validate_patch_target_base_policy(&manifest)?;
    let state_path = workspace.path().join("patch-state.json");

    let disk = crate::provision::attach_and_mount(&patched_image).with_context(|| {
        format!(
            "failed to attach and mount image {} before patch apply",
            patched_image.display()
        )
    })?;
    let apply_result =
        apply_verified_manifest_with_root(manifest, &bundle, &disk.mount_point, &state_path);
    let detach_result = disk.detach();
    apply_result?;
    detach_result?;

    let chunk_size = mib_to_bytes(args.chunk_size_mib)?;
    let header = create_image_delta_file(&base_image, &patched_image, &delta, chunk_size)
        .with_context(|| {
            format!(
                "failed to build image delta from {} to {}",
                base_image.display(),
                patched_image.display()
            )
        })?;

    println!(
        "Image delta created at {} (chunk={} MiB, changed_chunks={}, base={}, target={})",
        delta.display(),
        args.chunk_size_mib,
        header.changed_chunks,
        base_image.display(),
        patched_image.display()
    );
    Ok(())
}

pub(super) fn apply_delta(args: ApplyDeltaArgs) -> anyhow::Result<()> {
    let base_image = expand_home(&args.base_image);
    let delta = expand_home(&args.delta);
    let output_image = expand_home(&args.output_image);
    ensure_regular_file(&base_image, "--base-image")?;
    ensure_regular_file(&delta, "--delta")?;
    ensure_output_file_parent(&output_image)?;
    ensure_output_path_absent(&output_image, "--output-image")?;

    let header = apply_image_delta_file(&base_image, &delta, &output_image).with_context(|| {
        format!(
            "failed to apply image delta {} to {}",
            delta.display(),
            base_image.display()
        )
    })?;
    println!(
        "Image delta applied to {} (changed_chunks={}, target_size={} bytes)",
        output_image.display(),
        header.changed_chunks,
        header.target_size
    );
    Ok(())
}

pub(super) fn apply_with_image(
    bundle: &Path,
    image: &Path,
    patch_state_path: &Path,
) -> anyhow::Result<()> {
    let image = expand_home(image);
    if !image.exists() {
        bail!("disk image not found: {}", image.display());
    }

    let manifest = verify_bundle(bundle)?;
    validate_patch_target_base_policy(&manifest)?;
    crate::commands::vm_base::verify_image_for_base_id(&image, &manifest.target_base_id)
        .with_context(|| {
            format!(
                "pinned base verification failed before applying patch to image {}",
                image.display()
            )
        })?;

    let disk = crate::provision::attach_and_mount(&image).with_context(|| {
        format!(
            "failed to attach and mount image {} before patch apply",
            image.display()
        )
    })?;

    let result =
        apply_verified_manifest_with_root(manifest, bundle, &disk.mount_point, patch_state_path);
    let detach_result = disk.detach();

    result?;
    detach_result?;

    println!("Patch apply completed for image {}", image.display());
    Ok(())
}

pub(super) fn apply_with_root(
    bundle: &Path,
    root: &Path,
    patch_state_path: &Path,
) -> anyhow::Result<()> {
    let manifest = verify_bundle(bundle)?;
    validate_patch_target_base_policy(&manifest)?;
    apply_verified_manifest_with_root(manifest, bundle, root, patch_state_path)
}

fn apply_verified_manifest_with_root(
    manifest: PatchBundleManifest,
    bundle: &Path,
    root_arg: &Path,
    patch_state_path: &Path,
) -> anyhow::Result<()> {
    let root = fs::canonicalize(root_arg)
        .with_context(|| format!("failed to resolve apply root {}", root_arg.display()))?;
    if !root.is_dir() {
        bail!("apply root {} is not a directory", root.display());
    }

    let apply_receipt = PatchApplyReceipt::from_manifest(&root, &manifest)?;
    let mut patch_state = PatchApplyState::load(patch_state_path)?;
    if patch_state.has_receipt(&apply_receipt) {
        println!(
            "Patch bundle '{}' already applied at {} for target base '{}'; no changes made.",
            manifest.bundle_id,
            root.display(),
            manifest.target_base_id
        );
        return Ok(());
    }
    if let Some(existing) = patch_state.find_conflicting_receipt(&apply_receipt) {
        bail!(
            "patch receipt mismatch for bundle '{}' at {}. expected(existing receipt): {}. actual(requested apply): {}. Refusing to re-apply; inspect or remove {} if this is intentional.",
            manifest.bundle_id,
            root.display(),
            existing.identity_details(),
            apply_receipt.identity_details(),
            patch_state_path.display()
        );
    }

    let paths = BundlePaths::from_bundle_dir(bundle);
    let payload_by_digest = load_payload_archive(&paths.payload)?;
    validate_apply_preflight(&manifest, &payload_by_digest)?;
    apply_operations_transactional(&root, &manifest, &payload_by_digest)?;
    patch_state.record_receipt(apply_receipt);
    patch_state.save(patch_state_path).with_context(|| {
        format!(
            "patch operations were applied at {} but failed to persist receipt in {}",
            root.display(),
            patch_state_path.display()
        )
    })?;

    println!(
        "Patch bundle '{}' applied successfully at {}",
        manifest.bundle_id,
        root.display()
    );
    Ok(())
}
