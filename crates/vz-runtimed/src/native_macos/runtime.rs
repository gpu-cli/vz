//! Native VM ownership. Every execution lease retains an exact boot reader.
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{Mutex, OwnedRwLockReadGuard, RwLock};
use vz::{DiskConfig, MacPlatformConfig, NetworkConfig, Vm, VmConfigBuilder, VmState};
use vz_linux::grpc_client::GrpcAgentClient;
use vz_oci_macos::MacosOciError as Error;
use vz_runtime_contract::{StackRuntimeIdentity, StackRuntimeShutdownRequest};

pub(crate) fn error(value: impl ToString) -> Error {
    Error::InvalidConfig(value.to_string())
}

pub struct NativeMacosRuntime {
    directory: PathBuf,
    cpus: u8,
    memory_mb: u64,
    lifecycle: Arc<RwLock<()>>,
    live: Mutex<Option<Arc<NativeBoot>>>,
}

struct NativeBoot {
    vm: Arc<Vm>,
    identity: StackRuntimeIdentity,
}

pub struct NativeMacosLease {
    boot: Arc<NativeBoot>,
    _guard: OwnedRwLockReadGuard<()>,
}

impl NativeMacosRuntime {
    pub fn new(directory: PathBuf, cpus: u8, memory_mb: u64) -> Self {
        Self {
            directory,
            cpus,
            memory_mb,
            lifecycle: Arc::new(RwLock::new(())),
            live: Mutex::new(None),
        }
    }

    pub async fn boot(&self, name: &str) -> Result<NativeMacosLease, Error> {
        let guard = Arc::clone(&self.lifecycle).read_owned().await;
        let mut live = self.live.lock().await;
        if let Some(boot) = live.as_ref() {
            if boot.identity.stack_id != name
                || *boot.vm.state_stream().borrow() != VmState::Running
            {
                return Err(error("native boot is retained but not safely reusable"));
            }
            return Ok(NativeMacosLease {
                boot: Arc::clone(boot),
                _guard: guard,
            });
        }
        let config = VmConfigBuilder::new()
            .boot_macos()
            .cpus(self.cpus.into())
            .memory_mb(self.memory_mb)
            .disk(DiskConfig {
                id: "system".into(),
                path: self.directory.join("disk.img"),
                read_only: false,
            })
            .mac_platform(MacPlatformConfig {
                hardware_model_path: self.directory.join("hardware-model"),
                auxiliary_storage_path: self.directory.join("auxiliary-storage"),
                machine_identifier_path: self.directory.join("machine-identifier"),
            })
            .network(NetworkConfig::None)
            .enable_vsock()
            .build()
            .map_err(error)?;
        let boot = Arc::new(NativeBoot {
            vm: Arc::new(Vm::create(config).await.map_err(error)?),
            identity: StackRuntimeIdentity::new(name).map_err(error)?,
        });
        // Retain before dispatch: even a failed start is not absence evidence.
        *live = Some(Arc::clone(&boot));
        boot.vm.start().await.map_err(error)?;
        Ok(NativeMacosLease {
            boot,
            _guard: guard,
        })
    }

    pub async fn stop_exact(&self, request: &StackRuntimeShutdownRequest) -> Result<(), Error> {
        let _guard = Arc::clone(&self.lifecycle).write_owned().await;
        let mut live = self.live.lock().await;
        let boot = live
            .as_ref()
            .ok_or_else(|| error("native VM absence has no positive stop proof"))?;
        if request.expected != boot.identity {
            return Err(error("native Stop identity mismatch"));
        }
        let mut state = boot.vm.state_stream();
        if *state.borrow() != VmState::Stopped {
            // The guest owns graceful shutdown. Transport loss during shutdown is
            // expected; only the framework's positive Stopped state closes it.
            let shutdown = async {
                let mut client = GrpcAgentClient::connect_default(Arc::clone(&boot.vm)).await?;
                client
                    .exec_stream(
                        "/sbin/shutdown".into(),
                        vec!["-h".into(), "now".into()],
                        Default::default(),
                    )
                    .await?
                    .collect()
                    .await;
                Ok::<_, vz_linux::LinuxError>(())
            };
            let _ = tokio::time::timeout(Duration::from_secs(10), shutdown).await;
            tokio::time::timeout(Duration::from_secs(60), async {
                loop {
                    if *state.borrow_and_update() == VmState::Stopped {
                        return Ok::<(), Error>(());
                    }
                    state.changed().await.map_err(error)?;
                }
            })
            .await
            .map_err(|_| error("native graceful Stop timed out; original VM retained"))??;
        }
        *live = None;
        Ok(())
    }
}

impl NativeMacosLease {
    pub fn identity(&self) -> &StackRuntimeIdentity {
        &self.boot.identity
    }
    pub async fn client(&self) -> Result<GrpcAgentClient, Error> {
        if *self.boot.vm.state_stream().borrow() != VmState::Running {
            return Err(error("exact native VM is not running"));
        }
        Ok(GrpcAgentClient::connect_default(Arc::clone(&self.boot.vm)).await?)
    }
}
