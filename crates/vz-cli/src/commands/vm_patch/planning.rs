use super::*;

pub(super) fn create(args: CreateArgs) -> anyhow::Result<()> {
    prepare_bundle_output_dir(&args.bundle)?;

    let resolved_base = crate::commands::vm_base::resolve_base_selector(&args.base_id)
        .with_context(|| {
            format!(
                "failed to resolve --base-id selector '{}' for patch creation",
                args.base_id
            )
        })?;

    let inline_mode_requested = create_inline_mode_requested(&args);
    let (operations, payload_entries) = match (
        args.operations.as_ref(),
        args.payload_dir.as_ref(),
        inline_mode_requested,
    ) {
        (Some(operations), Some(payload_dir), false) => {
            let operations = load_operations_file(operations)?;
            let payload_entries = load_payload_entries(payload_dir)?;
            (operations, payload_entries)
        }
        (None, None, true) => build_inline_create_inputs(&args)?,
        (Some(_), Some(_), true) => bail!(
            "choose one create input mode: either (--operations + --payload-dir) or inline flags (--write-file/--mkdir/--symlink/--delete-file/--set-mode/--set-owner)"
        ),
        (Some(_), None, _) | (None, Some(_), _) => {
            bail!("--operations and --payload-dir must be provided together")
        }
        (None, None, false) => bail!(
            "no patch inputs provided. Use either (--operations + --payload-dir) or inline flags (--write-file/--mkdir/--symlink/--delete-file/--set-mode/--set-owner)"
        ),
    };

    let payload_digest_index = payload_digest_index(&payload_entries);
    let payload = build_payload_archive(&payload_entries)?;

    let post_state_hashes = if let Some(path) = args.post_state_hashes.as_ref() {
        load_post_state_hashes_file(path)?
    } else {
        derive_post_state_hashes(&operations)?
    };

    let key_pair = load_ed25519_key_pair(&args.signing_key)?;
    let signing_identity = format!(
        "ed25519:{}",
        base64::engine::general_purpose::STANDARD.encode(key_pair.public_key().as_ref())
    );
    let created_at = args.created_at.unwrap_or_else(default_created_at);
    let bundle_id = args
        .bundle_id
        .unwrap_or_else(|| default_bundle_id(&resolved_base.base.base_id));
    let target_base_fingerprint = BundleBaseFingerprint {
        img_sha256: resolved_base.base.fingerprint.img_sha256.clone(),
        aux_sha256: resolved_base.base.fingerprint.aux_sha256.clone(),
        hwmodel_sha256: resolved_base.base.fingerprint.hwmodel_sha256.clone(),
        machineid_sha256: resolved_base.base.fingerprint.machineid_sha256.clone(),
    };

    let manifest = PatchBundleManifest {
        bundle_id,
        patch_version: args.patch_version,
        target_base_id: resolved_base.base.base_id.clone(),
        target_base_fingerprint,
        operations_digest: operations_digest_hex(&operations)?,
        payload_digest: sha256_bytes_hex(&payload),
        post_state_hashes,
        created_at,
        signing_identity,
        operations,
    };
    manifest.validate()?;
    validate_apply_preflight(&manifest, &payload_digest_index)?;

    let manifest_bytes =
        serde_json::to_vec_pretty(&manifest).context("failed to serialize manifest")?;
    fs::write(args.bundle.join(MANIFEST_FILE), &manifest_bytes).with_context(|| {
        format!(
            "failed to write bundle manifest {}",
            args.bundle.join(MANIFEST_FILE).display()
        )
    })?;
    fs::write(args.bundle.join(PAYLOAD_FILE), &payload).with_context(|| {
        format!(
            "failed to write bundle payload {}",
            args.bundle.join(PAYLOAD_FILE).display()
        )
    })?;
    let signature = key_pair.sign(&manifest_bytes);
    fs::write(args.bundle.join(SIGNATURE_FILE), signature.as_ref()).with_context(|| {
        format!(
            "failed to write detached signature {}",
            args.bundle.join(SIGNATURE_FILE).display()
        )
    })?;

    let verified = verify_bundle(&args.bundle)?;
    validate_patch_target_base_policy(&verified)?;

    println!(
        "Patch bundle '{}' created at {} for target base '{}'",
        verified.bundle_id,
        args.bundle.display(),
        verified.target_base_id
    );
    Ok(())
}

fn create_inline_mode_requested(args: &CreateArgs) -> bool {
    !args.write_file.is_empty()
        || !args.mkdir.is_empty()
        || !args.symlink.is_empty()
        || !args.delete_file.is_empty()
        || !args.set_mode.is_empty()
        || !args.set_owner.is_empty()
}

type InlineCreateInputs = (Vec<PatchOperation>, Vec<(String, Vec<u8>)>);

fn build_inline_create_inputs(args: &CreateArgs) -> anyhow::Result<InlineCreateInputs> {
    let mut operations = Vec::new();
    let mut payload_by_digest = BTreeMap::<String, Vec<u8>>::new();

    for spec in &args.mkdir {
        let (path, mode) = parse_mkdir_spec(spec)?;
        operations.push(PatchOperation::Mkdir { path, mode });
    }

    for spec in &args.write_file {
        let (host_path, guest_path, mode_override) = parse_write_file_spec(spec)?;
        let metadata = fs::symlink_metadata(&host_path).with_context(|| {
            format!(
                "failed to inspect host file in --write-file spec '{}'",
                host_path.display()
            )
        })?;
        if !metadata.file_type().is_file() {
            bail!(
                "host path '{}' from --write-file is not a regular file",
                host_path.display()
            );
        }
        let bytes = fs::read(&host_path).with_context(|| {
            format!(
                "failed to read host file in --write-file spec '{}'",
                host_path.display()
            )
        })?;
        let digest = sha256_bytes_hex(&bytes);
        let _ = payload_by_digest.entry(digest.clone()).or_insert(bytes);
        let mode = mode_override.or(Some(metadata.mode() & 0o7777));
        operations.push(PatchOperation::WriteFile {
            path: guest_path,
            content_digest: digest,
            mode,
        });
    }

    for spec in &args.symlink {
        let (path, target) = parse_symlink_spec(spec)?;
        operations.push(PatchOperation::Symlink { path, target });
    }

    for spec in &args.set_owner {
        let (path, uid, gid) = parse_set_owner_spec(spec)?;
        operations.push(PatchOperation::SetOwner { path, uid, gid });
    }

    for spec in &args.set_mode {
        let (path, mode) = parse_set_mode_spec(spec)?;
        operations.push(PatchOperation::SetMode { path, mode });
    }

    for spec in &args.delete_file {
        let path = parse_delete_file_spec(spec)?;
        operations.push(PatchOperation::DeleteFile { path });
    }

    if operations.is_empty() {
        bail!("inline create mode produced no operations");
    }

    let mut payload_entries = Vec::new();
    for (digest, bytes) in payload_by_digest {
        payload_entries.push((digest, bytes));
    }

    Ok((operations, payload_entries))
}

fn parse_write_file_spec(spec: &str) -> anyhow::Result<(PathBuf, String, Option<u32>)> {
    let parts: Vec<&str> = spec.split(':').collect();
    if !(2..=3).contains(&parts.len()) {
        bail!(
            "invalid --write-file spec '{}'; expected HOST_PATH:GUEST_PATH[:MODE]",
            spec
        );
    }

    let host_path_raw = parts[0].trim();
    let guest_path = parts[1].trim();
    if host_path_raw.is_empty() || guest_path.is_empty() {
        bail!(
            "invalid --write-file spec '{}'; host path and guest path must be non-empty",
            spec
        );
    }

    let mode = if parts.len() == 3 {
        Some(parse_mode_value("--write-file MODE", parts[2].trim())?)
    } else {
        None
    };

    Ok((PathBuf::from(host_path_raw), guest_path.to_string(), mode))
}

fn parse_mkdir_spec(spec: &str) -> anyhow::Result<(String, Option<u32>)> {
    let parts: Vec<&str> = spec.split(':').collect();
    if !(1..=2).contains(&parts.len()) {
        bail!(
            "invalid --mkdir spec '{}'; expected GUEST_PATH[:MODE]",
            spec
        );
    }
    let path = parts[0].trim();
    if path.is_empty() {
        bail!(
            "invalid --mkdir spec '{}'; guest path must be non-empty",
            spec
        );
    }
    let mode = if parts.len() == 2 {
        Some(parse_mode_value("--mkdir MODE", parts[1].trim())?)
    } else {
        None
    };
    Ok((path.to_string(), mode))
}

fn parse_symlink_spec(spec: &str) -> anyhow::Result<(String, String)> {
    let Some((path, target)) = spec.split_once(':') else {
        bail!(
            "invalid --symlink spec '{}'; expected GUEST_PATH:TARGET",
            spec
        );
    };
    let path = path.trim();
    let target = target.trim();
    if path.is_empty() || target.is_empty() {
        bail!(
            "invalid --symlink spec '{}'; guest path and target must be non-empty",
            spec
        );
    }
    Ok((path.to_string(), target.to_string()))
}

fn parse_delete_file_spec(spec: &str) -> anyhow::Result<String> {
    let path = spec.trim();
    if path.is_empty() {
        bail!(
            "invalid --delete-file spec '{}'; guest path must be non-empty",
            spec
        );
    }
    Ok(path.to_string())
}

fn parse_set_mode_spec(spec: &str) -> anyhow::Result<(String, u32)> {
    let Some((path, mode_raw)) = spec.split_once(':') else {
        bail!(
            "invalid --set-mode spec '{}'; expected GUEST_PATH:MODE",
            spec
        );
    };
    let path = path.trim();
    if path.is_empty() {
        bail!(
            "invalid --set-mode spec '{}'; guest path must be non-empty",
            spec
        );
    }
    let mode = parse_mode_value("--set-mode MODE", mode_raw.trim())?;
    Ok((path.to_string(), mode))
}

fn parse_set_owner_spec(spec: &str) -> anyhow::Result<(String, u32, u32)> {
    let parts: Vec<&str> = spec.split(':').collect();
    if parts.len() != 3 {
        bail!(
            "invalid --set-owner spec '{}'; expected GUEST_PATH:UID:GID",
            spec
        );
    }
    let path = parts[0].trim();
    if path.is_empty() {
        bail!(
            "invalid --set-owner spec '{}'; guest path must be non-empty",
            spec
        );
    }
    let uid = parse_u32_value("--set-owner UID", parts[1].trim())?;
    let gid = parse_u32_value("--set-owner GID", parts[2].trim())?;
    Ok((path.to_string(), uid, gid))
}

fn parse_mode_value(field: &str, value: &str) -> anyhow::Result<u32> {
    if value.is_empty() {
        bail!("{field} must not be empty");
    }
    let parsed = if let Some(stripped) = value.strip_prefix("0o") {
        u32::from_str_radix(stripped, 8)
            .with_context(|| format!("{field} must be valid octal, received '{value}'"))?
    } else if value.len() > 1 && value.starts_with('0') {
        u32::from_str_radix(value, 8)
            .with_context(|| format!("{field} must be valid octal, received '{value}'"))?
    } else if value.len() <= 4 && value.chars().all(|c| matches!(c, '0'..='7')) {
        u32::from_str_radix(value, 8)
            .with_context(|| format!("{field} must be valid octal, received '{value}'"))?
    } else {
        value
            .parse::<u32>()
            .with_context(|| format!("{field} must be valid u32, received '{value}'"))?
    };
    validate_mode(field, parsed)?;
    Ok(parsed)
}

fn parse_u32_value(field: &str, value: &str) -> anyhow::Result<u32> {
    if value.is_empty() {
        bail!("{field} must not be empty");
    }
    value
        .parse::<u32>()
        .with_context(|| format!("{field} must be valid u32, received '{value}'"))
}
