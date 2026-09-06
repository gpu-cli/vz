//! Package the explicitly selected local Xcode without modifying its source.
use super::{check_cancelled, hash_file};
use anyhow::{Context, Result, ensure};
use flate2::{Compression, GzBuilder};
use std::{collections::BTreeMap, fs, io::Write, path::Path, process::Command};
use vz_macos_provision::toolchain::{ArchiveIdentity, ToolchainLayout, ToolchainManifest};

pub(super) fn package(source: &Path, output: &Path) -> Result<String> {
    let developer = source.join("Contents/Developer");
    let binaries = "Toolchains/XcodeDefault.xctoolchain/usr/bin";
    let query = |args: &[&str]| -> Result<String> {
        let result = Command::new("/usr/bin/xcrun")
            .args(args)
            .env("DEVELOPER_DIR", &developer)
            .env("LC_ALL", "C")
            .output()?;
        ensure!(
            result.status.success(),
            "local Xcode query failed: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        Ok(String::from_utf8(result.stdout)?.trim().to_string())
    };
    let sdk = query(&["--sdk", "macosx", "--show-sdk-version"])?;
    ensure!(
        !sdk.is_empty() && sdk.bytes().all(|b| b.is_ascii_digit() || b == b'.'),
        "invalid SDK version"
    );
    let selected =
        format!("Platforms/MacOSX.platform/Developer/SDKs/MacOSX{sdk}.sdk/SDKSettings.json");
    let mut files = BTreeMap::new();
    for name in [
        "swift-frontend",
        "swift-driver",
        "swift-package",
        "clang",
        "ld",
    ] {
        let relative = format!("{binaries}/{name}");
        files.insert(relative.clone(), hash_file(&developer.join(relative))?.0);
    }
    files.insert(selected.clone(), hash_file(&developer.join(selected))?.0);
    let result = Command::new("/bin/sh")
        .args(["-c", "exec \"$1\" --version 2>&1", "vz-swift-version"])
        .arg(developer.join(binaries).join("swift"))
        .env("DEVELOPER_DIR", &developer)
        .env("LC_ALL", "C")
        .output()?;
    ensure!(result.status.success(), "local Swift version query failed");
    let swift_version = String::from_utf8([result.stdout, result.stderr].concat())?
        .trim()
        .to_string();
    let archive = output.join("toolchain.tar.gz");
    let gzip = GzBuilder::new()
        .mtime(0)
        .write(fs::File::create_new(&archive)?, Compression::fast());
    let mut tar = tar::Builder::new(gzip);
    tar.follow_symlinks(false);
    append(&mut tar, source, source)?;
    tar.into_inner()?.finish()?.sync_all()?;
    for (relative, expected) in &files {
        ensure!(
            hash_file(&developer.join(relative))?.0 == *expected,
            "Xcode changed during preparation"
        );
    }
    let (sha256, size_bytes) = hash_file(&archive)?;
    let manifest = ToolchainManifest {
        schema_version: 1,
        layout: ToolchainLayout::Xcode,
        swift_version,
        sdk_version: sdk,
        files,
        archive: ArchiveIdentity { sha256, size_bytes },
    };
    manifest.validate()?;
    let bytes = serde_json::to_vec_pretty(&manifest)?;
    let mut file = fs::File::create_new(output.join("toolchain.json"))?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(super::hash_bytes(&bytes))
}

fn append<W: Write>(tar: &mut tar::Builder<W>, root: &Path, path: &Path) -> Result<()> {
    check_cancelled()?;
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_dir() || metadata.is_file() || metadata.is_symlink(),
        "unsupported toolchain entry: {}",
        path.display()
    );
    let mut header = tar::Header::new_gnu();
    header.set_metadata(&metadata);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    use std::os::unix::fs::PermissionsExt;
    header.set_mode(metadata.permissions().mode() & 0o777);
    let name = Path::new("Xcode.app").join(path.strip_prefix(root)?);
    if metadata.is_symlink() {
        let link = fs::read_link(path)?;
        ensure!(
            !link.is_absolute()
                && path
                    .canonicalize()
                    .with_context(|| format!("resolve toolchain link {}", path.display()))?
                    .starts_with(root),
            "toolchain symlink escapes input: {}",
            path.display()
        );
        header.set_size(0);
        // Code signatures seal the literal link target. append_link normalizes
        // doubled separators and dot components, changing signed Xcode resources.
        use std::os::unix::ffi::OsStrExt;
        let target = link.as_os_str().as_bytes();
        if header.set_link_name_literal(target).is_err() {
            header.set_link_name_literal(b"pax-link-target")?;
            tar.append_pax_extensions([("linkpath", target)])?;
        }
        tar.append_data(&mut header, &name, std::io::empty())?;
    } else if metadata.is_dir() {
        header.set_size(0);
        tar.append_data(&mut header, &name, std::io::empty())?;
        let mut entries = fs::read_dir(path)?
            .map(|e| e.map(|e| e.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        entries.sort();
        for entry in entries {
            append(tar, root, &entry)?;
        }
    } else {
        tar.append_data(&mut header, name, fs::File::open(path)?)?;
    }
    Ok(())
}

/// Pin compiler and selected SDK inputs before deciding whether setup is reusable.
pub(super) fn source_identity(source: &Path) -> Result<BTreeMap<String, String>> {
    let root = source.join("Contents/Developer");
    let mut anchors = BTreeMap::new();
    for name in [
        "swift-frontend",
        "swift-driver",
        "swift-package",
        "clang",
        "ld",
    ] {
        let path = format!("Toolchains/XcodeDefault.xctoolchain/usr/bin/{name}");
        anchors.insert(path.clone(), hash_file(&root.join(&path))?.0);
    }
    let sdk = "Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk/SDKSettings.json";
    anchors.insert(sdk.into(), hash_file(&root.join(sdk))?.0);
    Ok(anchors)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::{ffi::OsStrExt, fs::symlink};

    #[test]
    fn archive_preserves_literal_signed_resource_links() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let source = temporary.path().join("source");
        fs::create_dir_all(source.join("Resources"))?;
        let source = source.canonicalize()?;
        fs::write(source.join("Resources/value"), b"signed resource")?;
        let long = format!("Resources/{}", "nested".repeat(25));
        fs::create_dir_all(source.join(&long))?;
        fs::write(source.join(&long).join("value"), b"long signed resource")?;
        let links = [
            ("short-link".to_string(), "Resources//./value".to_string()),
            ("long-link".to_string(), format!("{long}//./value")),
            (
                format!("both-long-{}", "x".repeat(120)),
                format!("{long}//./value"),
            ),
            (
                format!("long-name-{}", "x".repeat(120)),
                "Resources//./value".to_string(),
            ),
        ];
        for (name, target) in &links {
            symlink(target, source.join(name))?;
        }
        let archive = temporary.path().join("test.tar");
        let mut builder = tar::Builder::new(fs::File::create(&archive)?);
        append(&mut builder, &source, &source)?;
        builder.finish()?;
        drop(builder);
        let extracted = temporary.path().join("extracted");
        fs::create_dir(&extracted)?;
        // Exercise the same BSD tar extractor used in the actual macOS guest.
        let status = Command::new("/usr/bin/tar")
            .arg("-xf")
            .arg(&archive)
            .arg("-C")
            .arg(&extracted)
            .status()?;
        ensure!(status.success(), "extract archive");
        for (name, expected) in links {
            let observed = fs::read_link(extracted.join("Xcode.app").join(&name))
                .with_context(|| format!("read extracted link {name}"))?;
            // Path equality normalizes components; signatures seal literal bytes.
            assert_eq!(observed.as_os_str().as_bytes(), expected.as_bytes());
        }
        Ok(())
    }
}
