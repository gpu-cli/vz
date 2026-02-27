use super::super::*;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::time::UNIX_EPOCH;

const DEFAULT_READ_LIMIT_BYTES: u64 = 1024 * 1024;
const MAX_READ_LIMIT_BYTES: u64 = 8 * 1024 * 1024;
const DEFAULT_LIST_LIMIT: usize = 256;
const MAX_LIST_LIMIT: usize = 5000;

#[derive(Clone)]
pub(in crate::grpc) struct FileServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl FileServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

fn sandbox_fs_root(daemon: &RuntimeDaemon, sandbox_id: &str) -> PathBuf {
    daemon
        .runtime_data_dir()
        .join("sandboxes")
        .join(sandbox_id)
        .join("fs")
}

fn validation_status(request_id: &str, message: impl Into<String>) -> Status {
    status_from_machine_error(MachineError::new(
        MachineErrorCode::ValidationError,
        message.into(),
        Some(request_id.to_string()),
        BTreeMap::new(),
    ))
}

fn io_status(request_id: &str, context: &str, error: &std::io::Error) -> Status {
    let code = match error.kind() {
        std::io::ErrorKind::NotFound => MachineErrorCode::NotFound,
        std::io::ErrorKind::AlreadyExists => MachineErrorCode::StateConflict,
        std::io::ErrorKind::InvalidInput => MachineErrorCode::ValidationError,
        std::io::ErrorKind::PermissionDenied => MachineErrorCode::BackendUnavailable,
        _ => MachineErrorCode::InternalError,
    };
    status_from_machine_error(MachineError::new(
        code,
        format!("{context}: {error}"),
        Some(request_id.to_string()),
        BTreeMap::new(),
    ))
}

fn ensure_sandbox_exists(
    daemon: &RuntimeDaemon,
    sandbox_id: &str,
    request_id: &str,
) -> Result<(), Status> {
    let sandbox_exists = daemon
        .with_state_store(|store| store.load_sandbox(sandbox_id))
        .map_err(|error| status_from_stack_error(error, request_id))?
        .is_some();
    if !sandbox_exists {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::NotFound,
            format!("sandbox not found: {sandbox_id}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }
    Ok(())
}

fn resolve_scoped_path(
    root: &Path,
    raw_path: &str,
    request_id: &str,
    field_name: &str,
    allow_empty_as_root: bool,
) -> Result<PathBuf, Status> {
    let trimmed = raw_path.trim();
    if trimmed.is_empty() {
        if allow_empty_as_root {
            return Ok(root.to_path_buf());
        }
        return Err(validation_status(
            request_id,
            format!("{field_name} cannot be empty"),
        ));
    }

    let candidate = Path::new(trimmed);
    if candidate.is_absolute() {
        return Err(validation_status(
            request_id,
            format!("{field_name} must be a relative path"),
        ));
    }

    let mut scoped = root.to_path_buf();
    for component in candidate.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(segment) => scoped.push(segment),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(validation_status(
                    request_id,
                    format!("{field_name} cannot traverse outside sandbox scope"),
                ));
            }
        }
    }
    Ok(scoped)
}

fn modified_unix_secs(metadata: &std::fs::Metadata) -> u64 {
    metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn entry_rel_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn list_entries(
    root: &Path,
    start_path: &Path,
    recursive: bool,
    limit: usize,
) -> Result<Vec<runtime_v2::FileEntry>, std::io::Error> {
    let mut entries = Vec::new();
    let mut queue = vec![start_path.to_path_buf()];
    while let Some(dir) = queue.pop() {
        let iter = std::fs::read_dir(&dir)?;
        for item in iter {
            let item = item?;
            let path = item.path();
            let metadata = item.metadata()?;
            let is_dir = metadata.is_dir();
            entries.push(runtime_v2::FileEntry {
                path: entry_rel_path(root, &path),
                is_dir,
                size: if is_dir { 0 } else { metadata.len() },
                modified_at: modified_unix_secs(&metadata),
            });
            if entries.len() >= limit {
                return Ok(entries);
            }
            if recursive && is_dir {
                queue.push(path);
            }
        }
    }
    Ok(entries)
}

fn copy_path_recursive(src: &Path, dst: &Path, overwrite: bool) -> Result<(), std::io::Error> {
    let metadata = std::fs::metadata(src)?;
    if metadata.is_dir() {
        if dst.exists() {
            if !overwrite {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("destination already exists: {}", dst.display()),
                ));
            }
            if dst.is_file() {
                std::fs::remove_file(dst)?;
            }
        }
        std::fs::create_dir_all(dst)?;
        for item in std::fs::read_dir(src)? {
            let item = item?;
            let child_src = item.path();
            let child_dst = dst.join(item.file_name());
            copy_path_recursive(&child_src, &child_dst, overwrite)?;
        }
        return Ok(());
    }

    if dst.exists() {
        if !overwrite {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("destination already exists: {}", dst.display()),
            ));
        }
        if dst.is_dir() {
            std::fs::remove_dir_all(dst)?;
        } else {
            std::fs::remove_file(dst)?;
        }
    }
    if let Some(parent) = dst.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let _ = std::fs::copy(src, dst)?;
    Ok(())
}

#[tonic::async_trait]
impl runtime_v2::file_service_server::FileService for FileServiceImpl {
    async fn read_file(
        &self,
        request: Request<runtime_v2::ReadFileRequest>,
    ) -> Result<Response<runtime_v2::ReadFileResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let sandbox_id = request.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(validation_status(&request_id, "sandbox_id cannot be empty"));
        }
        ensure_sandbox_exists(self.daemon.as_ref(), &sandbox_id, &request_id)?;

        let root = sandbox_fs_root(self.daemon.as_ref(), &sandbox_id);
        let file_path = resolve_scoped_path(&root, &request.path, &request_id, "path", false)?;
        let mut file = std::fs::File::open(&file_path)
            .map_err(|error| io_status(&request_id, "failed to open file", &error))?;
        file.seek(SeekFrom::Start(request.offset))
            .map_err(|error| io_status(&request_id, "failed to seek file", &error))?;

        let limit = if request.limit == 0 {
            DEFAULT_READ_LIMIT_BYTES
        } else {
            request.limit.min(MAX_READ_LIMIT_BYTES)
        };
        let mut data = Vec::new();
        let mut reader = file.take(limit.saturating_add(1));
        reader
            .read_to_end(&mut data)
            .map_err(|error| io_status(&request_id, "failed to read file", &error))?;
        let truncated = u64::try_from(data.len())
            .map(|len| len > limit)
            .unwrap_or(true);
        if truncated {
            data.truncate(limit as usize);
        }

        Ok(Response::new(runtime_v2::ReadFileResponse {
            request_id,
            data,
            truncated,
        }))
    }

    async fn write_file(
        &self,
        request: Request<runtime_v2::WriteFileRequest>,
    ) -> Result<Response<runtime_v2::WriteFileResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateVolume,
            &metadata,
            &request_id,
        )?;

        let sandbox_id = request.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(validation_status(&request_id, "sandbox_id cannot be empty"));
        }
        ensure_sandbox_exists(self.daemon.as_ref(), &sandbox_id, &request_id)?;
        let root = sandbox_fs_root(self.daemon.as_ref(), &sandbox_id);
        let file_path = resolve_scoped_path(&root, &request.path, &request_id, "path", false)?;

        if request.create_parents
            && let Some(parent) = file_path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                io_status(&request_id, "failed to create parent directory", &error)
            })?;
        }

        let mut open = OpenOptions::new();
        open.write(true).create(true);
        if request.append {
            open.append(true);
        } else {
            open.truncate(true);
        }
        let mut file = open
            .open(&file_path)
            .map_err(|error| io_status(&request_id, "failed to open file for write", &error))?;
        file.write_all(&request.data)
            .map_err(|error| io_status(&request_id, "failed to write file", &error))?;
        file.flush()
            .map_err(|error| io_status(&request_id, "failed to flush file", &error))?;

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "write_file".to_string(),
                        entity_id: format!("{sandbox_id}:{}", request.path),
                        entity_type: "file".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_file_mutation_metadata(
                            "file_written",
                            &sandbox_id,
                            request.path.as_str(),
                            None,
                        )?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(runtime_v2::WriteFileResponse {
            request_id,
            bytes_written: request.data.len() as u64,
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn list_files(
        &self,
        request: Request<runtime_v2::ListFilesRequest>,
    ) -> Result<Response<runtime_v2::ListFilesResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let sandbox_id = request.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(validation_status(&request_id, "sandbox_id cannot be empty"));
        }
        ensure_sandbox_exists(self.daemon.as_ref(), &sandbox_id, &request_id)?;

        let root = sandbox_fs_root(self.daemon.as_ref(), &sandbox_id);
        let scoped = resolve_scoped_path(&root, &request.path, &request_id, "path", true)?;
        let metadata = std::fs::metadata(&scoped)
            .map_err(|error| io_status(&request_id, "failed to stat target path", &error))?;
        let limit = if request.limit == 0 {
            DEFAULT_LIST_LIMIT
        } else {
            (request.limit as usize).clamp(1, MAX_LIST_LIMIT)
        };

        let entries = if metadata.is_file() {
            vec![runtime_v2::FileEntry {
                path: entry_rel_path(&root, &scoped),
                is_dir: false,
                size: metadata.len(),
                modified_at: modified_unix_secs(&metadata),
            }]
        } else {
            list_entries(&root, &scoped, request.recursive, limit)
                .map_err(|error| io_status(&request_id, "failed to list files", &error))?
        };

        Ok(Response::new(runtime_v2::ListFilesResponse {
            request_id,
            entries,
        }))
    }

    async fn make_dir(
        &self,
        request: Request<runtime_v2::MakeDirRequest>,
    ) -> Result<Response<runtime_v2::FileMutationResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateVolume,
            &metadata,
            &request_id,
        )?;

        let sandbox_id = request.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(validation_status(&request_id, "sandbox_id cannot be empty"));
        }
        ensure_sandbox_exists(self.daemon.as_ref(), &sandbox_id, &request_id)?;
        let root = sandbox_fs_root(self.daemon.as_ref(), &sandbox_id);
        let dir_path = resolve_scoped_path(&root, &request.path, &request_id, "path", false)?;
        if request.parents {
            std::fs::create_dir_all(&dir_path)
                .map_err(|error| io_status(&request_id, "failed to create directory", &error))?;
        } else {
            std::fs::create_dir(&dir_path)
                .map_err(|error| io_status(&request_id, "failed to create directory", &error))?;
        }

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "mkdir".to_string(),
                        entity_id: format!("{sandbox_id}:{}", request.path),
                        entity_type: "file".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_file_mutation_metadata(
                            "file_dir_created",
                            &sandbox_id,
                            request.path.as_str(),
                            None,
                        )?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(runtime_v2::FileMutationResponse {
            request_id,
            path: request.path,
            status: "success".to_string(),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn remove_path(
        &self,
        request: Request<runtime_v2::RemovePathRequest>,
    ) -> Result<Response<runtime_v2::FileMutationResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::DetachVolume,
            &metadata,
            &request_id,
        )?;

        let sandbox_id = request.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(validation_status(&request_id, "sandbox_id cannot be empty"));
        }
        ensure_sandbox_exists(self.daemon.as_ref(), &sandbox_id, &request_id)?;
        let root = sandbox_fs_root(self.daemon.as_ref(), &sandbox_id);
        let target_path = resolve_scoped_path(&root, &request.path, &request_id, "path", false)?;

        let metadata = std::fs::metadata(&target_path)
            .map_err(|error| io_status(&request_id, "failed to stat path", &error))?;
        if metadata.is_dir() {
            if request.recursive {
                std::fs::remove_dir_all(&target_path).map_err(|error| {
                    io_status(&request_id, "failed to remove directory", &error)
                })?;
            } else {
                std::fs::remove_dir(&target_path).map_err(|error| {
                    io_status(&request_id, "failed to remove directory", &error)
                })?;
            }
        } else {
            std::fs::remove_file(&target_path)
                .map_err(|error| io_status(&request_id, "failed to remove file", &error))?;
        }

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "remove_path".to_string(),
                        entity_id: format!("{sandbox_id}:{}", request.path),
                        entity_type: "file".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_file_mutation_metadata(
                            "file_removed",
                            &sandbox_id,
                            request.path.as_str(),
                            None,
                        )?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(runtime_v2::FileMutationResponse {
            request_id,
            path: request.path,
            status: "success".to_string(),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn move_path(
        &self,
        request: Request<runtime_v2::MovePathRequest>,
    ) -> Result<Response<runtime_v2::FileMutationResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateVolume,
            &metadata,
            &request_id,
        )?;

        let sandbox_id = request.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(validation_status(&request_id, "sandbox_id cannot be empty"));
        }
        ensure_sandbox_exists(self.daemon.as_ref(), &sandbox_id, &request_id)?;
        let root = sandbox_fs_root(self.daemon.as_ref(), &sandbox_id);
        let src_path =
            resolve_scoped_path(&root, &request.src_path, &request_id, "src_path", false)?;
        let dst_path =
            resolve_scoped_path(&root, &request.dst_path, &request_id, "dst_path", false)?;

        if dst_path.exists() {
            if !request.overwrite {
                return Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::StateConflict,
                    format!("destination already exists: {}", request.dst_path),
                    Some(request_id),
                    BTreeMap::new(),
                )));
            }
            if dst_path.is_dir() {
                std::fs::remove_dir_all(&dst_path).map_err(|error| {
                    io_status(
                        &request_id,
                        "failed to replace destination directory",
                        &error,
                    )
                })?;
            } else {
                std::fs::remove_file(&dst_path).map_err(|error| {
                    io_status(&request_id, "failed to replace destination file", &error)
                })?;
            }
        }
        if let Some(parent) = dst_path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                io_status(&request_id, "failed to create destination parent", &error)
            })?;
        }
        std::fs::rename(&src_path, &dst_path)
            .map_err(|error| io_status(&request_id, "failed to move path", &error))?;

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "move_path".to_string(),
                        entity_id: format!("{sandbox_id}:{}", request.src_path),
                        entity_type: "file".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_file_mutation_metadata(
                            "file_moved",
                            &sandbox_id,
                            request.src_path.as_str(),
                            Some(request.dst_path.as_str()),
                        )?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(runtime_v2::FileMutationResponse {
            request_id,
            path: request.dst_path,
            status: "success".to_string(),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn copy_path(
        &self,
        request: Request<runtime_v2::CopyPathRequest>,
    ) -> Result<Response<runtime_v2::FileMutationResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateVolume,
            &metadata,
            &request_id,
        )?;

        let sandbox_id = request.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(validation_status(&request_id, "sandbox_id cannot be empty"));
        }
        ensure_sandbox_exists(self.daemon.as_ref(), &sandbox_id, &request_id)?;
        let root = sandbox_fs_root(self.daemon.as_ref(), &sandbox_id);
        let src_path =
            resolve_scoped_path(&root, &request.src_path, &request_id, "src_path", false)?;
        let dst_path =
            resolve_scoped_path(&root, &request.dst_path, &request_id, "dst_path", false)?;

        copy_path_recursive(&src_path, &dst_path, request.overwrite)
            .map_err(|error| io_status(&request_id, "failed to copy path", &error))?;

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "copy_path".to_string(),
                        entity_id: format!("{sandbox_id}:{}", request.src_path),
                        entity_type: "file".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_file_mutation_metadata(
                            "file_copied",
                            &sandbox_id,
                            request.src_path.as_str(),
                            Some(request.dst_path.as_str()),
                        )?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(runtime_v2::FileMutationResponse {
            request_id,
            path: request.dst_path,
            status: "success".to_string(),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn chmod_path(
        &self,
        request: Request<runtime_v2::ChmodPathRequest>,
    ) -> Result<Response<runtime_v2::FileMutationResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateVolume,
            &metadata,
            &request_id,
        )?;

        let sandbox_id = request.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(validation_status(&request_id, "sandbox_id cannot be empty"));
        }
        ensure_sandbox_exists(self.daemon.as_ref(), &sandbox_id, &request_id)?;
        let root = sandbox_fs_root(self.daemon.as_ref(), &sandbox_id);
        let target_path = resolve_scoped_path(&root, &request.path, &request_id, "path", false)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&target_path, std::fs::Permissions::from_mode(request.mode))
                .map_err(|error| io_status(&request_id, "failed to chmod path", &error))?;
        }
        #[cfg(not(unix))]
        {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::UnsupportedOperation,
                "chmod is only supported on unix hosts".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "chmod_path".to_string(),
                        entity_id: format!("{sandbox_id}:{}", request.path),
                        entity_type: "file".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_file_mutation_metadata(
                            "file_chmod",
                            &sandbox_id,
                            request.path.as_str(),
                            None,
                        )?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(runtime_v2::FileMutationResponse {
            request_id,
            path: request.path,
            status: "success".to_string(),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn chown_path(
        &self,
        request: Request<runtime_v2::ChownPathRequest>,
    ) -> Result<Response<runtime_v2::FileMutationResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateVolume,
            &metadata,
            &request_id,
        )?;

        let sandbox_id = request.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(validation_status(&request_id, "sandbox_id cannot be empty"));
        }
        ensure_sandbox_exists(self.daemon.as_ref(), &sandbox_id, &request_id)?;
        let root = sandbox_fs_root(self.daemon.as_ref(), &sandbox_id);
        let target_path = resolve_scoped_path(&root, &request.path, &request_id, "path", false)?;

        #[cfg(unix)]
        {
            let output = Command::new("chown")
                .arg(format!("{}:{}", request.uid, request.gid))
                .arg(&target_path)
                .output()
                .map_err(|error| io_status(&request_id, "failed to execute chown", &error))?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                return Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::UnsupportedOperation,
                    if stderr.is_empty() {
                        "chown operation failed".to_string()
                    } else {
                        format!("chown operation failed: {stderr}")
                    },
                    Some(request_id),
                    BTreeMap::new(),
                )));
            }
        }
        #[cfg(not(unix))]
        {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::UnsupportedOperation,
                "chown is only supported on unix hosts".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "chown_path".to_string(),
                        entity_id: format!("{sandbox_id}:{}", request.path),
                        entity_type: "file".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_file_mutation_metadata(
                            "file_chown",
                            &sandbox_id,
                            request.path.as_str(),
                            None,
                        )?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(runtime_v2::FileMutationResponse {
            request_id,
            path: request.path,
            status: "success".to_string(),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }
}
