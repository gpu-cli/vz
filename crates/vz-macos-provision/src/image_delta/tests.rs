use super::*;
use anyhow::bail;
use tempfile::tempdir;

#[test]
fn roundtrip_growth_shrink_and_empty_with_progress() -> Result<()> {
    for target in [vec![3; 10000], vec![9; 5], vec![]] {
        let dir = tempdir()?;
        let base = dir.path().join("base");
        let prepared = dir.path().join("prepared");
        let patch = dir.path().join("patch");
        let output = dir.path().join("output");
        fs::write(&base, vec![4; 8192])?;
        fs::write(&prepared, &target)?;
        let expected = create(&base, &prepared, &patch, 4096, |_| Ok(()))?;
        let mut events = Vec::new();
        assert_eq!(
            apply(&base, &patch, &output, |p| {
                events.push(p);
                Ok(())
            })?,
            expected
        );
        assert_eq!(fs::read(output)?, target);
        assert_eq!(fs::read(base)?, vec![4; 8192]);
        assert!(events.iter().all(|p| p.completed <= p.total));
        assert_eq!(events.last().map(|p| p.phase), Some(Phase::VerifyingOutput));
    }
    Ok(())
}

fn fixture() -> Result<tempfile::TempDir> {
    let d = tempdir()?;
    fs::write(d.path().join("base"), vec![0; 8192])?;
    fs::write(d.path().join("prepared"), vec![7; 8192])?;
    create(
        &d.path().join("base"),
        &d.path().join("prepared"),
        &d.path().join("patch"),
        4096,
        |_| Ok(()),
    )?;
    Ok(d)
}

#[test]
fn wrong_base_and_output_digest_never_publish() -> Result<()> {
    for corrupt_base in [true, false] {
        let d = fixture()?;
        if corrupt_base {
            fs::write(d.path().join("base"), vec![1; 8192])?;
        } else {
            let mut bytes = fs::read(d.path().join("patch"))?;
            bytes[64] ^= 1; // Target digest, after magic/version/size/base digest.
            fs::write(d.path().join("patch"), bytes)?;
        }
        let before = fs::read_dir(d.path())?.count();
        assert!(
            apply(
                &d.path().join("base"),
                &d.path().join("patch"),
                &d.path().join("output"),
                |_| Ok(())
            )
            .is_err()
        );
        assert!(!d.path().join("output").exists());
        assert_eq!(fs::read_dir(d.path())?.count(), before);
    }
    Ok(())
}

#[test]
fn cancellation_in_every_phase_leaves_no_partial_image() -> Result<()> {
    for phase in [
        Phase::VerifyingBase,
        Phase::CopyingBase,
        Phase::ApplyingPatch,
        Phase::VerifyingOutput,
    ] {
        let d = fixture()?;
        assert!(
            apply(
                &d.path().join("base"),
                &d.path().join("patch"),
                &d.path().join("output"),
                |p| {
                    if p.phase == phase && p.completed > 0 {
                        bail!("cancelled");
                    }
                    Ok(())
                }
            )
            .is_err()
        );
        assert_eq!(fs::read_dir(d.path())?.count(), 3);
    }
    Ok(())
}

#[test]
fn cancellation_during_creation_discards_patch() -> Result<()> {
    let d = fixture()?;
    assert!(
        create(
            &d.path().join("base"),
            &d.path().join("prepared"),
            &d.path().join("second-patch"),
            4096,
            |p| {
                if p.completed > 0 {
                    bail!("cancelled");
                }
                Ok(())
            }
        )
        .is_err()
    );
    assert_eq!(fs::read_dir(d.path())?.count(), 3);
    Ok(())
}

#[test]
fn destination_created_during_apply_is_preserved() -> Result<()> {
    let d = fixture()?;
    let out = d.path().join("output");
    assert!(
        apply(&d.path().join("base"), &d.path().join("patch"), &out, |p| {
            if p.phase == Phase::VerifyingOutput && p.completed == p.total {
                fs::write(&out, b"neighbor")?;
            }
            Ok(())
        })
        .is_err()
    );
    assert_eq!(fs::read(out)?, b"neighbor");
    assert_eq!(fs::read_dir(d.path())?.count(), 4);
    Ok(())
}

#[test]
fn malformed_sizes_trailing_bytes_and_truncated_payload_fail_closed() -> Result<()> {
    for corruption in 0..5 {
        let d = fixture()?;
        let patch = d.path().join("patch");
        let mut b = fs::read(&patch)?;
        match corruption {
            0 => b[12..16].copy_from_slice(&u32::MAX.to_le_bytes()),
            1 => b[116..120].copy_from_slice(&u32::MAX.to_le_bytes()), // Compressed size.
            2 => b.push(0),
            3 => {
                b.truncate(b.len() - 1);
            }
            _ => b[96..104].copy_from_slice(&u64::MAX.to_le_bytes()), // Count.
        }
        fs::write(patch, b)?;
        assert!(
            apply(
                &d.path().join("base"),
                &d.path().join("patch"),
                &d.path().join("output"),
                |_| Ok(())
            )
            .is_err()
        );
        assert_eq!(fs::read_dir(d.path())?.count(), 3);
    }
    Ok(())
}

#[test]
fn reads_legacy_v1_fixture_and_does_not_copy_machine_identity() -> Result<()> {
    let d = tempdir()?;
    let base = d.path().join("base.img");
    fs::write(&base, b"abc")?;
    fs::write(base.with_extension("machineid"), b"original-machine")?;
    // Independently assemble the documented legacy unchanged-image format.
    let mut bytes = b"VZDELTA1".to_vec();
    bytes.extend(1u32.to_le_bytes());
    bytes.extend(4096u32.to_le_bytes());
    bytes.extend(3u64.to_le_bytes());
    bytes.extend(3u64.to_le_bytes());
    let digest: [u8; 32] = Sha256::digest(b"abc").into();
    bytes.extend(digest);
    bytes.extend(digest);
    bytes.extend(0u64.to_le_bytes());
    let patch = d.path().join("patch");
    fs::write(&patch, bytes)?;
    let out = d.path().join("output.img");
    apply(&base, &patch, &out, |_| Ok(()))?;
    assert_eq!(fs::read(&out)?, b"abc");
    assert!(!out.with_extension("machineid").exists());
    Ok(())
}

#[cfg(unix)]
#[test]
fn symlink_input_and_output_are_rejected() -> Result<()> {
    use std::os::unix::fs::symlink;
    let d = fixture()?;
    let base = d.path().join("base");
    let link = d.path().join("link");
    symlink(&base, &link)?;
    assert!(
        apply(
            &link,
            &d.path().join("patch"),
            &d.path().join("output"),
            |_| Ok(())
        )
        .is_err()
    );
    assert!(apply(&base, &d.path().join("patch"), &link, |_| Ok(())).is_err());
    assert_eq!(fs::read(base)?, vec![0; 8192]);
    Ok(())
}
