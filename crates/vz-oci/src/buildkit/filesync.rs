use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use bollard_buildkit_proto::fsutil::types::{Packet, Stat, packet::PacketType};
use bollard_buildkit_proto::moby::filesync::v1::file_sync_server::FileSync;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

/// Errors returned by local FileSync provider operations.
#[derive(Debug, thiserror::Error)]
pub enum FileSyncError {
    #[error("context directory does not exist: {path}")]
    ContextMissing { path: PathBuf },

    #[error("context path is not a directory: {path}")]
    ContextNotDirectory { path: PathBuf },

    #[error("path is outside build context: {path}")]
    PathOutsideContext { path: PathBuf },

    #[error("path is ignored by .dockerignore: {path}")]
    PathIgnored { path: PathBuf },

    #[error("invalid .dockerignore: {0}")]
    Dockerignore(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
struct ContextEntry {
    relative_path: PathBuf,
    metadata: std::fs::Metadata,
}

/// Local filesystem-backed BuildKit context provider.
#[derive(Debug, Clone)]
pub struct LocalFileSync {
    context_dir: PathBuf,
    dockerignore: Arc<Gitignore>,
}

impl LocalFileSync {
    /// Create provider rooted at `context_dir` with `.dockerignore` filtering.
    pub fn new(context_dir: impl Into<PathBuf>) -> Result<Self, FileSyncError> {
        let context_dir = context_dir.into();
        if !context_dir.exists() {
            return Err(FileSyncError::ContextMissing { path: context_dir });
        }
        if !context_dir.is_dir() {
            return Err(FileSyncError::ContextNotDirectory { path: context_dir });
        }

        let canonical_context = std::fs::canonicalize(&context_dir)?;
        let dockerignore = build_dockerignore(&canonical_context)?;
        Ok(Self {
            context_dir: canonical_context,
            dockerignore: Arc::new(dockerignore),
        })
    }

    /// Root directory for this provider.
    pub fn context_dir(&self) -> &Path {
        &self.context_dir
    }

    /// List non-ignored context entries recursively.
    fn list_entries(&self) -> Result<Vec<ContextEntry>, FileSyncError> {
        let mut entries = Vec::new();
        let mut queue = VecDeque::from([PathBuf::new()]);

        while let Some(relative_dir) = queue.pop_front() {
            let full_dir = self.context_dir.join(&relative_dir);
            for dir_entry in std::fs::read_dir(full_dir)? {
                let dir_entry = dir_entry?;
                let file_name = dir_entry.file_name();
                let relative_path = if relative_dir.as_os_str().is_empty() {
                    PathBuf::from(&file_name)
                } else {
                    relative_dir.join(&file_name)
                };

                let metadata = dir_entry.metadata()?;
                if self.path_ignored(&relative_path, metadata.is_dir()) {
                    continue;
                }

                if metadata.is_dir() {
                    queue.push_back(relative_path.clone());
                }
                entries.push(ContextEntry {
                    relative_path,
                    metadata,
                });
            }
        }

        Ok(entries)
    }

    /// Resolve a relative path inside context and ensure it is not ignored.
    fn resolve_path(&self, relative_path: &Path) -> Result<PathBuf, FileSyncError> {
        if relative_path.is_absolute() {
            return Err(FileSyncError::PathOutsideContext {
                path: relative_path.to_path_buf(),
            });
        }

        let joined = self.context_dir.join(relative_path);
        let canonical = std::fs::canonicalize(&joined)?;
        if !canonical.starts_with(&self.context_dir) {
            return Err(FileSyncError::PathOutsideContext {
                path: relative_path.to_path_buf(),
            });
        }

        let metadata = std::fs::metadata(&canonical)?;
        if self.path_ignored(relative_path, metadata.is_dir()) {
            return Err(FileSyncError::PathIgnored {
                path: relative_path.to_path_buf(),
            });
        }

        Ok(canonical)
    }

    fn path_ignored(&self, relative_path: &Path, is_dir: bool) -> bool {
        self.dockerignore.matched(relative_path, is_dir).is_ignore()
    }
}

fn build_dockerignore(context_dir: &Path) -> Result<Gitignore, FileSyncError> {
    let mut builder = GitignoreBuilder::new(context_dir);
    let dockerignore_path = context_dir.join(".dockerignore");
    if dockerignore_path.exists() {
        if let Some(err) = builder.add(dockerignore_path) {
            return Err(FileSyncError::Dockerignore(err.to_string()));
        }
    }
    builder
        .build()
        .map_err(|err| FileSyncError::Dockerignore(err.to_string()))
}

fn stat_mode(metadata: &std::fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut mode = metadata.permissions().mode();
        if metadata.is_dir() {
            mode |= 0o040000;
        } else if metadata.is_file() {
            mode |= 0o100000;
        }
        mode
    }

    #[cfg(not(unix))]
    {
        if metadata.is_dir() {
            0o040755
        } else {
            0o100644
        }
    }
}

fn stat_mod_time_secs(metadata: &std::fs::Metadata) -> i64 {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        .unwrap_or_default()
}

fn to_status(error: FileSyncError) -> Status {
    match error {
        FileSyncError::PathOutsideContext { .. } | FileSyncError::PathIgnored { .. } => {
            Status::permission_denied(error.to_string())
        }
        FileSyncError::ContextMissing { .. }
        | FileSyncError::ContextNotDirectory { .. }
        | FileSyncError::Dockerignore(_) => Status::failed_precondition(error.to_string()),
        FileSyncError::Io(io) => Status::internal(io.to_string()),
    }
}

/// BuildKit FileSync gRPC service for context transfer.
#[derive(Debug, Clone)]
pub struct FileSyncService {
    provider: LocalFileSync,
}

impl FileSyncService {
    pub fn new(provider: LocalFileSync) -> Self {
        Self { provider }
    }
}

#[tonic::async_trait]
impl FileSync for FileSyncService {
    type DiffCopyStream = ReceiverStream<Result<Packet, Status>>;
    type TarStreamStream = ReceiverStream<Result<Packet, Status>>;

    async fn diff_copy(
        &self,
        request: Request<tonic::Streaming<Packet>>,
    ) -> Result<Response<Self::DiffCopyStream>, Status> {
        let mut inbound = request.into_inner();
        let provider = self.provider.clone();
        let (tx, rx) = mpsc::channel::<Result<Packet, Status>>(128);

        tokio::spawn(async move {
            if let Err(error) = send_context_stats(&provider, &tx).await {
                let _ = tx.send(Err(error)).await;
                return;
            }

            loop {
                let maybe_packet = match inbound.message().await {
                    Ok(message) => message,
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                };

                let Some(packet) = maybe_packet else {
                    return;
                };

                let packet_type =
                    PacketType::try_from(packet.r#type).unwrap_or(PacketType::PacketStat);
                if packet_type != PacketType::PacketReq {
                    continue;
                }
                let Some(requested_stat) = packet.stat else {
                    continue;
                };

                let relative = PathBuf::from(requested_stat.path);
                let full_path = match provider.resolve_path(&relative) {
                    Ok(path) => path,
                    Err(error) => {
                        let _ = tx.send(Err(to_status(error))).await;
                        continue;
                    }
                };

                if let Err(error) = send_file_data(full_path, packet.id, &tx).await {
                    let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                    return;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn tar_stream(
        &self,
        request: Request<tonic::Streaming<Packet>>,
    ) -> Result<Response<Self::TarStreamStream>, Status> {
        self.diff_copy(request).await
    }
}

async fn send_context_stats(
    provider: &LocalFileSync,
    tx: &mpsc::Sender<Result<Packet, Status>>,
) -> Result<(), Status> {
    let entries = provider.list_entries().map_err(to_status)?;
    for entry in entries {
        let stat_packet = Packet {
            r#type: PacketType::PacketStat as i32,
            stat: Some(Stat {
                path: entry.relative_path.to_string_lossy().into_owned(),
                mode: stat_mode(&entry.metadata),
                uid: 0,
                gid: 0,
                size: i64::try_from(entry.metadata.len()).unwrap_or_default(),
                mod_time: stat_mod_time_secs(&entry.metadata),
                linkname: String::new(),
                devmajor: 0,
                devminor: 0,
                xattrs: Default::default(),
            }),
            id: 0,
            data: Vec::new(),
        };
        tx.send(Ok(stat_packet))
            .await
            .map_err(|_| Status::cancelled("filesync receiver closed"))?;
    }
    Ok(())
}

async fn send_file_data(
    file_path: PathBuf,
    packet_id: u32,
    tx: &mpsc::Sender<Result<Packet, Status>>,
) -> Result<(), std::io::Error> {
    let mut file = tokio::fs::File::open(file_path).await?;
    let mut buffer = vec![0_u8; 1024 * 1024];

    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        tx.send(Ok(Packet {
            r#type: PacketType::PacketData as i32,
            stat: None,
            id: packet_id,
            data: buffer[..read].to_vec(),
        }))
        .await
        .map_err(|_| std::io::Error::other("filesync receiver closed"))?;
    }

    tx.send(Ok(Packet {
        r#type: PacketType::PacketFin as i32,
        stat: None,
        id: packet_id,
        data: Vec::new(),
    }))
    .await
    .map_err(|_| std::io::Error::other("filesync receiver closed"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::io::Write;
    use std::path::Path;

    use tempfile::tempdir;

    use super::LocalFileSync;

    #[test]
    fn list_entries_respects_dockerignore_rules() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        std::fs::write(root.join("keep.txt"), "a").unwrap();
        std::fs::write(root.join("drop.log"), "b").unwrap();
        std::fs::create_dir_all(root.join("build")).unwrap();
        std::fs::write(root.join("build").join("keep.log"), "c").unwrap();

        let mut dockerignore = std::fs::File::create(root.join(".dockerignore")).unwrap();
        writeln!(dockerignore, "*.log").unwrap();
        writeln!(dockerignore, "!build/keep.log").unwrap();

        let provider = LocalFileSync::new(root).unwrap();
        let entries = provider.list_entries().unwrap();
        let paths: Vec<String> = entries
            .iter()
            .map(|entry| entry.relative_path.to_string_lossy().into_owned())
            .collect();

        assert!(paths.contains(&"keep.txt".to_string()));
        assert!(paths.contains(&"build".to_string()));
        assert!(paths.contains(&"build/keep.log".to_string()));
        assert!(!paths.contains(&"drop.log".to_string()));
    }

    #[test]
    fn resolve_path_rejects_escaping_context() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        std::fs::write(root.join("keep.txt"), "a").unwrap();
        let provider = LocalFileSync::new(root).unwrap();

        let result = provider.resolve_path(Path::new("../keep.txt"));
        assert!(result.is_err());
    }
}
