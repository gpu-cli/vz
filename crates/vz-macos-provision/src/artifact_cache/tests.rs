use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tempfile::TempDir;
use tokio::net::TcpListener;

fn cache() -> Result<(TempDir, ArtifactCache)> {
    let d = tempfile::tempdir()?;
    // macOS /var and /tmp are symlinks; canonicalize the test fixture's parent.
    let mut cache = ArtifactCache::new(d.path().canonicalize()?.join("cache"))?;
    cache.client = reqwest::Client::builder().no_proxy().build()?;
    Ok((d, cache))
}

fn pin(url: String, bytes: &[u8]) -> Artifact {
    Artifact {
        url,
        sha256: format!("{:x}", Sha256::digest(bytes)),
        size_bytes: bytes.len() as u64,
    }
}

async fn serve(bytes: Vec<u8>) -> Result<(String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/artifact", listener.local_addr()?);
    let count = Arc::new(AtomicUsize::new(0));
    let c = count.clone();
    let task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = [0; 4096];
            if stream.read(&mut buf).await.is_err() {
                continue;
            }
            c.fetch_add(1, Ordering::SeqCst);
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                bytes.len()
            );
            if stream.write_all(header.as_bytes()).await.is_err() {
                continue;
            }
            let _ = stream.write_all(&bytes).await;
        }
    });
    Ok((url, count, task))
}

#[test]
fn pin_validation_fails_before_cache_mutation() -> Result<()> {
    let valid = pin("https://example.invalid/pinned.ipsw".into(), b"payload");
    valid.validate()?;
    for url in [
        "http://example.invalid/base",
        "https://user:secret@example.invalid/base",
        "https://example.invalid/base#fragment",
    ] {
        assert!(
            Artifact {
                url: url.into(),
                ..valid.clone()
            }
            .validate()
            .is_err()
        );
    }
    assert!(
        Artifact {
            sha256: "../escape".into(),
            ..valid.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        Artifact {
            size_bytes: 0,
            ..valid
        }
        .validate()
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_downloads_are_once_and_warm_hit_works_offline() -> Result<()> {
    let (_d, cache) = cache()?;
    let bytes = vec![42; 128 * 1024];
    let (url, count, server) = serve(bytes.clone()).await?;
    let artifact = pin(url, &bytes);
    artifact.validate_url(true)?;
    let (a, b) = tokio::join!(
        cache.ensure_validated(&artifact, |_| Ok(())),
        cache.ensure_validated(&artifact, |_| Ok(()))
    );
    server.abort();
    let _ = server.await;
    let path = a?;
    assert_eq!(path, b?);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let mut phases = Vec::new();
    assert_eq!(
        path,
        cache
            .ensure_validated(&artifact, |p| {
                phases.push(p.phase);
                Ok(())
            })
            .await?
    );
    assert!(!phases.contains(&Phase::Downloading));
    assert_eq!(fs::read(path)?, bytes);
    Ok(())
}

#[tokio::test]
async fn tamper_and_cancel_do_not_publish_partial_downloads() -> Result<()> {
    for cancel in [true, false] {
        let (_d, cache) = cache()?;
        let (url, _, server) = serve(vec![3; 65536]).await?;
        let artifact = pin(url, &vec![if cancel { 3 } else { 4 }; 65536]);
        let result = cache
            .ensure_validated(&artifact, |p| {
                if cancel && p.phase == Phase::Downloading && p.completed > 0 {
                    anyhow::bail!("cancelled");
                }
                Ok(())
            })
            .await;
        server.abort();
        let _ = server.await;
        assert!(result.is_err());
        assert!(!cache.root.join(&artifact.sha256).exists());
        assert_eq!(fs::read_dir(&cache.root)?.count(), 1); // Persistent lock only.
    }
    Ok(())
}

#[tokio::test]
async fn corrupt_cache_is_rejected_without_network_or_replacement() -> Result<()> {
    let (_d, cache) = cache()?;
    let a = pin("https://example.invalid/unreachable".into(), b"right");
    let path = cache.root.join(&a.sha256);
    fs::write(&path, b"wrong")?;
    assert!(cache.ensure(&a, |_| Ok(())).await.is_err());
    assert_eq!(fs::read(path)?, b"wrong");
    Ok(())
}

#[tokio::test]
async fn invalid_length_has_no_published_artifact() -> Result<()> {
    let (_d, cache) = cache()?;
    let (url, _, server) = serve(vec![3; 100]).await?;
    let artifact = pin(url, b"short");
    let result = cache.ensure_validated(&artifact, |_| Ok(())).await;
    server.abort();
    let _ = server.await;
    assert!(result.is_err());
    assert_eq!(fs::read_dir(&cache.root)?.count(), 1);
    Ok(())
}

#[cfg(unix)]
#[test]
fn public_or_symlink_cache_is_rejected() -> Result<()> {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let d = tempfile::tempdir()?;
    let parent = d.path().canonicalize()?;
    let public = parent.join("public");
    fs::create_dir(&public)?;
    fs::set_permissions(&public, fs::Permissions::from_mode(0o755))?;
    assert!(ArtifactCache::new(public.clone()).is_err());
    let link = parent.join("link");
    symlink(&public, &link)?;
    assert!(ArtifactCache::new(link).is_err());
    Ok(())
}

#[tokio::test]
async fn dropped_download_future_discards_staging_and_releases_lock() -> Result<()> {
    let (_d, cache) = cache()?;
    let cache = Arc::new(cache);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let artifact = pin(
        format!("http://{}/artifact", listener.local_addr()?),
        b"abcdefgh",
    );
    let stalled_server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut method = [0; 4];
        stream.read_exact(&mut method).await?;
        if &method != b"GET " {
            return Err(std::io::Error::other("expected GET request"));
        }
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nabc")
            .await?;
        std::future::pending::<()>().await;
        Ok::<_, std::io::Error>(())
    });
    let received = Arc::new(tokio::sync::Notify::new());
    let signal = received.clone();
    let target = artifact.clone();
    let c = cache.clone();
    let download = tokio::spawn(async move {
        c.ensure_validated(&target, |p| {
            if p.phase == Phase::Downloading && p.completed > 0 {
                signal.notify_one();
            }
            Ok(())
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), received.notified()).await?;
    download.abort();
    let cancelled = download.await;
    stalled_server.abort();
    let _ = stalled_server.await;
    assert!(cancelled.is_err_and(|e| e.is_cancelled()));
    assert_eq!(fs::read_dir(&cache.root)?.count(), 1);
    let (url, _, server) = serve(b"abcdefgh".to_vec()).await?;
    let result = cache
        .ensure_validated(&Artifact { url, ..artifact }, |_| Ok(()))
        .await;
    server.abort();
    let _ = server.await;
    assert_eq!(fs::read(result?)?, b"abcdefgh");
    Ok(())
}
