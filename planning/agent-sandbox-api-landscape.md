# Agent Sandbox API Landscape: Concrete Interface Analysis

Research date: 2026-03-01

This document catalogs the exact API surfaces that AI coding agents talk to when interacting with sandboxed execution environments. The goal is to understand what interface VZ needs to expose to be a credible sandbox backend.

---

## 1. E2B (e2b.dev)

**Architecture**: Firecracker microVMs. ~150ms cold start. Each sandbox is a full Linux VM with its own filesystem, network, and process space. Templates built from Dockerfiles, converted to microVM snapshots.

### 1.1 SDK Surface (TypeScript)

```typescript
import { Sandbox } from 'e2b'

// === LIFECYCLE ===
const sandbox = await Sandbox.create()                           // Default template
const sandbox = await Sandbox.create('my-template')              // Custom template
const sandbox = await Sandbox.create({
  timeoutMs: 300_000,                                            // 5 min default
  envs: { FOO: 'bar' },
  metadata: { purpose: 'test' },
})
const sandbox = await Sandbox.connect(sandboxId)                 // Reconnect by ID
await sandbox.kill()                                             // Destroy
await sandbox.setTimeout(600_000)                                // Extend lifetime
const running = await sandbox.isRunning()
const list = await Sandbox.list()                                // List all sandboxes

// === PERSISTENCE (beta) ===
const sandbox = await Sandbox.betaCreate({ autoPause: true, timeoutMs: 600_000 })
await sandbox.betaPause()                                        // Pause (saves memory + fs)
const resumed = await Sandbox.connect(sandboxId)                 // Auto-resumes if paused
await sandbox.createSnapshot()                                   // Point-in-time snapshot

// === COMMANDS ===
const result = await sandbox.commands.run('echo hello')          // Sync execution
// result: { stdout, stderr, exitCode, ... }

const handle = await sandbox.commands.run('long-task', {
  background: true,
  cwd: '/home/user/project',
  envs: { NODE_ENV: 'production' },
  timeoutMs: 60_000,
  user: 'root',
  onStdout: (data: string) => console.log(data),                // Streaming stdout
  onStderr: (data: string) => console.error(data),              // Streaming stderr
})
await handle.wait()                                              // Wait for completion

await sandbox.commands.sendStdin(pid, 'input\n')                // Send stdin
await sandbox.commands.kill(pid)                                 // Kill process
const procs = await sandbox.commands.list()                     // List running processes
// ProcessInfo: { pid, cmd, args, cwd, envs, tag }

const handle = await sandbox.commands.connect(pid)               // Attach to running process

// === FILESYSTEM ===
const text = await sandbox.files.read('/path', { format: 'text' })
const bytes = await sandbox.files.read('/path', { format: 'bytes' })     // Uint8Array
const blob = await sandbox.files.read('/path', { format: 'blob' })
const stream = await sandbox.files.read('/path', { format: 'stream' })   // ReadableStream

await sandbox.files.write('/path/file.txt', 'content')
await sandbox.files.write('/path/file.bin', arrayBuffer)
await sandbox.files.write('/path/file', readableStream)
// Returns: EntryInfo { name, type, path }

const entries = await sandbox.files.list('/dir')                 // EntryInfo[]
const exists = await sandbox.files.exists('/path')
await sandbox.files.makeDir('/path/to/dir')
await sandbox.files.remove('/path')
await sandbox.files.rename('/old', '/new')

const watcher = await sandbox.files.watchDir('/dir', (event) => {
  // event: { type, path, ... }
})

// === NETWORKING ===
const host = sandbox.getHost(3000)                               // "https://3000-xxx.e2b.app"
const url = sandbox.downloadUrl('/path/to/file')                 // Direct download URL
const url = sandbox.uploadUrl('/dest/path')                      // Direct upload URL

// Network control at creation:
const sandbox = await Sandbox.create({
  network: {
    denyOut: [ALL_TRAFFIC],                                      // Block all outbound
    allowOut: ['1.1.1.1', '8.8.8.0/24'],                        // IP/CIDR allowlist
  }
})
// Domain-based filtering (HTTP/TLS only):
const sandbox = await Sandbox.create({
  network: {
    denyOut: [ALL_TRAFFIC],
    allowOut: ['*.github.com', 'registry.npmjs.org'],            // Wildcard domains
  }
})
// Public URL access control:
const sandbox = await Sandbox.create({ allowPublicTraffic: false })
// Requires header: e2b-traffic-access-token: sandbox.trafficAccessToken

// === PTY ===
sandbox.pty                                                      // Pseudo-terminal access
```

### 1.2 E2B MCP Server (15 tools)

```
SANDBOX LIFECYCLE (5):
  e2b_create_sandbox        { timeoutMs?, template? }
  e2b_kill_sandbox          { sandboxId }
  e2b_reconnect_sandbox     { sandboxId }
  e2b_extend_sandbox_timeout { sandboxId, timeoutMs }
  e2b_list_sandboxes        {}

CODE EXECUTION (3):
  e2b_execute_code          { sandboxId, code, language: "python"|"js"|"r"|"java"|"bash" }
  e2b_create_code_context   { sandboxId }          // Stateful REPL context
  e2b_execute_in_context    { sandboxId, contextId, code }  // Preserves variables

FILE OPERATIONS (4):
  e2b_read_file             { sandboxId, path, encoding? }
  e2b_write_file            { sandboxId, path, content }
  e2b_list_directory        { sandboxId, path }
  e2b_delete_file           { sandboxId, path }

FILE TRANSFER (3):
  e2b_upload_file           { sandboxId, localPath, remotePath }
  e2b_download_file         { sandboxId, remotePath }
  (1 more transfer tool)

All responses: dual format (JSON + Markdown)
```

### 1.3 Key Design Choices

- **Firecracker microVMs**: VM-level isolation, not containers
- **~150ms cold start**: From pre-built microVM snapshot
- **Pause/resume preserves memory + filesystem**: ~4s per GiB RAM
- **gRPC for hot-path** (commands, files, PTY); REST for lifecycle
- **Templates from Dockerfiles**: `e2b template build` converts Dockerfile to microVM snapshot
- **Default 5 min timeout**: Auto-kill, extendable
- **Full internet access by default**: Opt-in deny lists

---

## 2. Daytona (daytona.io)

**Architecture**: Secure sandboxes with sub-90ms creation. Python, TypeScript, Ruby, Go SDKs. Focus on AI code execution with stateful interpreters.

### 2.1 SDK Surface (TypeScript)

```typescript
import { Daytona } from '@daytonaio/sdk'

const daytona = new Daytona()

// === LIFECYCLE ===
const sandbox = await daytona.create({
  language: 'python' | 'typescript' | 'javascript',
  name: 'my-sandbox',                                           // Optional, reusable after deletion
  snapshot: 'snapshot-id',                                       // Pre-built environment
  image: { name: 'ubuntu:22.04' },                              // Custom OCI image
  ephemeral: false,                                              // Auto-destroy on stop?
  labels: { team: 'ml' },
  // Resources:
  cpu: 1,                                                        // max: 4
  memory: 1,                                                     // GB, max: 8
  disk: 3,                                                       // GB, max: 10
  gpu: 0,
  // Lifecycle:
  autoStopInterval: 15,                                          // minutes, 0 = indefinite
  autoArchiveInterval: 10080,                                    // 7 days default
  autoDeleteInterval: -1,                                        // disabled
  // Networking:
  networkBlockAll: true,                                         // Block all egress
  networkAllowList: '208.80.154.232/32,10.0.0.0/8',            // Up to 5 CIDRs
  public: false,
  // Volumes:
  volumes: [{ name: 'data', mountPath: '/data' }],
})

await sandbox.start()                                            // Resume stopped/archived
await sandbox.stop()                                             // Halt, preserve filesystem
await sandbox.archive()                                          // Move to object storage
await sandbox.delete()                                           // Permanent destroy
await sandbox.resize({ cpu: 2, memory: 4 })                     // Scale resources
const info = await sandbox.getInfo()

// === COMMAND EXECUTION ===
const result = await sandbox.process.executeCommand('echo hello')
// result: { result: string, exitCode: number }

// Stateless code execution:
const result = await sandbox.process.codeRun('print("hello")', {
  env: { KEY: 'val' },
  timeout: 30000,
})
// result: { result: string, exitCode: number }

// Stateful code interpreter (preserves variables across calls):
const ctx = await sandbox.codeInterpreter.createContext()
await sandbox.codeInterpreter.runCode('x = 42', { context: ctx })
await sandbox.codeInterpreter.runCode('print(x)', {
  context: ctx,
  onStdout: (line) => console.log(line),
})
await sandbox.codeInterpreter.deleteContext(ctx)

// Sessions (long-running interactive):
await sandbox.process.createSession('dev')
const result = await sandbox.process.executeSessionCommand('dev', {
  command: 'npm run dev',
  runAsync: true,
})
await sandbox.process.sendSessionCommandInput('dev', cmdId, 'y\n')
await sandbox.process.getSessionCommandLogs('dev', cmdId,
  (stdout) => console.log(stdout),
  (stderr) => console.error(stderr),
)
await sandbox.process.deleteSession('dev')

// === FILESYSTEM ===
const files = await sandbox.fs.listFiles('/path')
// FileInfo: { name, isDir, size, modTime }

const details = await sandbox.fs.getFileDetails('/path/file.txt')
// { size, modTime, mode, isDir, permissions, owner, group }

await sandbox.fs.createFolder('/path/to/dir', '0755')
await sandbox.fs.uploadFile(buffer, '/dest/path')
await sandbox.fs.uploadFiles([{ source: buffer, destination: '/path' }])
const content = await sandbox.fs.downloadFile('/path')            // Buffer
const results = await sandbox.fs.downloadFiles([{ source: '/path' }])
await sandbox.fs.deleteFile('/path')
await sandbox.fs.setFilePermissions('/path', { mode: '0644' })
await sandbox.fs.moveFiles('/src', '/dest')

// Search and replace across files:
const matches = await sandbox.fs.findFiles({ path: '/src', pattern: 'TODO' })
// Match: { file, line, content }
await sandbox.fs.replaceInFiles(['/src/app.ts'], 'old_func', 'new_func')
```

### 2.2 Daytona MCP Server (12 tools)

```
SANDBOX MANAGEMENT:
  create_sandbox     { id?, target: "us", image?, auto_stop_interval: "15",
                       auto_archive_interval: "10080", auto_delete_interval: "-1",
                       volumes?, network_block_all?, network_allow_list?,
                       public?, cpu?, gpu?, memory?, disk? }
  destroy_sandbox    { sandbox_id }

FILE OPERATIONS:
  upload_file        { file_path, content, encoding?, overwrite? }
  download_file      { file_path }
  create_folder      { folder_path, mode? }
  get_file_info      { file_path }
  list_files         { path? }
  move_file          { source_path, dest_path }
  delete_file        { file_path }

COMMAND EXECUTION:
  execute_command    { command }            // Returns stdout, stderr, exit_code

GIT OPERATIONS:
  git_clone          { url, path?, branch?, commit_id?, username?, password? }

PREVIEW:
  preview_link       { port, description?, check_server? }
```

### 2.3 Key Design Choices

- **Sub-90ms sandbox creation**
- **Stateful code interpreter**: Preserves variables across calls (like Jupyter kernel)
- **Sessions**: Long-running interactive processes with stdin/stdout streaming
- **3-state lifecycle**: Running -> Stopped -> Archived (object storage)
- **CIDR-based network control**: Up to 5 allowlisted CIDRs, or block-all
- **Whitelisted essential services**: NPM, PyPI, GitHub, Docker Hub always accessible (tier-dependent)
- **Resource limits**: CPU 1-4, Memory 1-8GB, Disk 3-10GB, GPU support
- **Search/replace in filesystem**: Built into SDK (agent-friendly)
- **Git clone as first-class operation**

---

## 3. Fly.io Sprites (sprites.dev)

**Architecture**: Firecracker VMs with object-storage-backed persistent filesystems (JuiceFS-inspired). 100GB storage per sprite. ~1-2s cold start, ~100-500ms warm wake. Stateful by design -- processes survive hibernation via Services.

### 3.1 REST API Surface

```
Base URL: https://api.sprites.dev
Auth: Authorization: Bearer $SPRITES_TOKEN
SDKs: Go, TypeScript, Python (coming), Elixir (coming)

=== SPRITES (LIFECYCLE) ===
PUT    /v1/sprites/{name}                    Create/update sprite
GET    /v1/sprites                           List sprites
GET    /v1/sprites/{name}                    Get sprite details
DELETE /v1/sprites/{name}                    Destroy sprite

=== EXEC (COMMAND EXECUTION) ===
WSS    /v1/sprites/{name}/exec              WebSocket exec (primary)
  Query params:
    cmd* (string, repeatable)               Command + args
    tty (bool)                              Terminal mode
    stdin (bool)                            Enable stdin
    cols, rows (int)                        Terminal dimensions
    max_run_after_disconnect (duration)     TTY: indefinite, non-TTY: 10s
    env (string, repeatable)                KEY=VALUE pairs

  Binary protocol (non-TTY multiplexing):
    0x00: stdin  (client -> server)
    0x01: stdout (server -> client)
    0x02: stderr (server -> client)
    0x03: exit code
    0x04: stdin EOF
  PTY mode: raw bytes, no prefix

  JSON messages:
    SessionInfoMessage: { session_id, cmd, created_at, cols, rows, owned, tty }
    ResizeMessage: { cols, rows }
    PortNotificationMessage: { port, pid, ... }

POST   /v1/sprites/{name}/exec              Non-WebSocket exec (fallback)
  Body: { "command": "echo hello" }
  Response: { stdout, stderr }

GET    /v1/sprites/{name}/exec              List sessions
WSS    /v1/sprites/{name}/exec/{session_id} Attach to existing session
                                             (sends scrollback buffer on connect)
POST   /v1/sprites/{name}/exec/{session_id}/kill  Kill session
  Body: { signal?, timeout? }
  Response: streaming NDJSON progress

=== CHECKPOINTS ===
POST   /v1/sprites/{name}/checkpoint                  Create checkpoint
  Body: { comment?: string }
  Response: streaming NDJSON { type: "info"|"error"|"complete", data, time }

GET    /v1/sprites/{name}/checkpoints                 List checkpoints
  Response: [{ id: "v7", create_time, source_id?, comment?, health? }]

GET    /v1/sprites/{name}/checkpoints/{id}            Get checkpoint
POST   /v1/sprites/{name}/checkpoints/{id}/restore    Restore checkpoint
  Response: streaming NDJSON progress

=== FILESYSTEM ===
GET    /v1/sprites/{name}/fs/read       Read file (raw bytes)
  Query: path, workingDir

PUT    /v1/sprites/{name}/fs/write      Write file (raw bytes in body)
  Query: path, workingDir, mode? (octal), mkdir? (bool)

GET    /v1/sprites/{name}/fs/list       List directory
  Query: path, workingDir

DELETE /v1/sprites/{name}/fs/delete     Delete file/dir
  Body: { path, workingDir, recursive, asRoot }

POST   /v1/sprites/{name}/fs/rename     Move/rename
  Body: { source, dest, workingDir, asRoot }

POST   /v1/sprites/{name}/fs/copy       Copy
  Body: { source, dest, workingDir, recursive, preserveAttrs, asRoot }

POST   /v1/sprites/{name}/fs/chmod      Change permissions
  Body: { path, workingDir, mode, recursive, asRoot }

POST   /v1/sprites/{name}/fs/chown      Change ownership
  Body: { path, workingDir, uid, gid, recursive, asRoot }

WSS    /v1/sprites/{name}/fs/watch      Watch filesystem changes
  Messages: { type, paths[], recursive, workingDir, path, event, timestamp, size, isDir }

=== SERVICES (BACKGROUND PROCESSES) ===
POST   /v1/sprites/{name}/services      Create service
GET    /v1/sprites/{name}/services      List services
GET    /v1/sprites/{name}/services/{id} Get service
GET    /v1/sprites/{name}/services/{id}/logs  Get logs
POST   /v1/sprites/{name}/services/{id}/start Start
POST   /v1/sprites/{name}/services/{id}/stop  Stop

=== NETWORK POLICY ===
GET    /v1/sprites/{name}/policy/network      Get network rules
POST   /v1/sprites/{name}/policy/network      Set network rules
  Body: { rules: [
    { action: "allow"|"deny", domain: "github.com" },
    { action: "allow", domain: "*.npmjs.org" },
    { action: "deny", domain: "*" },
    { include: "preset-bundle-name" }       // Preset rule bundles
  ]}
  Changes apply immediately; existing connections to blocked domains terminated.

=== PRIVILEGES POLICY ===
GET    /v1/sprites/{name}/policy/privileges
POST   /v1/sprites/{name}/policy/privileges
DELETE /v1/sprites/{name}/policy/privileges

=== PROXY (TCP TUNNEL) ===
WSS    /v1/sprites/{name}/proxy/{port}        TCP tunnel to sprite port
```

### 3.2 TypeScript SDK

```typescript
import { SpritesClient } from '@fly/sprites'

const client = new SpritesClient(process.env.SPRITES_TOKEN!)
const sprite = client.sprite('my-sprite')

const { stdout } = await sprite.exec('echo hello')
await sprite.restoreCheckpoint(checkpointId)     // Returns async stream
```

### 3.3 Go SDK

```go
client := sprites.New("token")
sprite := client.Sprite("my-sprite")
cmd := sprite.Command("echo", "hello", "world")  // mirrors exec.Command API
output, err := cmd.Output()
```

### 3.4 Key Design Choices

- **Stateful by default**: 100GB persistent storage, processes survive hibernation via Services
- **Checkpoint = metadata shuffle (~300ms)**: Object-storage-backed FS means checkpoints don't copy data
- **WebSocket-first exec**: Binary protocol multiplexing stdin/stdout/stderr, session persistence
- **POST exec fallback**: For environments without WebSocket
- **DNS-based network filtering**: Domain wildcards, preset bundles, immediate enforcement
- **Activity-based hibernation**: Active -> Warm (idle) -> Cold (extended), auto-wake on request
- **Services**: Named background processes that auto-restart and survive hibernation
- **Rich filesystem API**: chmod, chown, copy with preserveAttrs, recursive operations
- **Streaming NDJSON responses**: For checkpoints and long operations

---

## 4. Anthropic Claude Code Sandbox

**Architecture**: OS-level sandboxing using native primitives. NOT a remote VM -- runs locally on the developer's machine with restricted filesystem and network access.

### 4.1 Interface Model

Claude Code does NOT use a "create sandbox" API. The sandbox is the developer's machine with restrictions:

```
Agent has 2 tools that touch the sandbox:
  1. Bash(command)     -> { stdout, stderr, exitCode }
  2. Edit(file, ...)   -> file modification result

The sandbox wraps these tools transparently:
  - macOS: Seatbelt profiles via sandbox_init()
  - Linux: bubblewrap (bwrap) + Landlock + seccomp
```

### 4.2 Sandbox Configuration

```
Filesystem isolation:
  - Read:  Current working directory + system dirs (/usr, /lib, etc.)
  - Write: Current working directory only
  - Blocked: ~/.ssh, ~/.aws, system files, etc.

Network isolation:
  - All traffic routed through Unix domain socket -> external proxy
  - Proxy enforces domain allowlist
  - New domains require user confirmation
  - Configurable allow/deny per domain

Permission model:
  - Pre-defined boundaries (dirs, domains) reduce prompts by 84%
  - Agent notified immediately on boundary violation
  - User can allow/deny in real-time
```

### 4.3 Cloud Variant (Claude Code on Web)

```
Each session runs in an isolated sandbox container:
  - Full access to its own server
  - Git credentials and signing keys kept OUTSIDE the sandbox
  - Credential injection only for specific operations
  - Complete filesystem isolation between sessions
```

### 4.4 Key Design Choices

- **Local execution, not remote VMs**: Sandbox is the developer's own machine
- **OS-level primitives**: Seatbelt (macOS), bubblewrap+Landlock+seccomp (Linux)
- **Two tools only**: Bash and Edit -- that's the entire interface
- **Proxy-based network control**: Unix socket -> domain-filtering proxy
- **No explicit lifecycle**: No create/destroy -- sandbox exists for duration of session
- **No checkpoint/restore**: Agent works directly on the real filesystem
- **Permission prompts as escape hatch**: When sandbox blocks something, user decides

---

## 5. OpenAI Codex CLI Sandbox

**Architecture**: OS-level sandboxing with per-platform native security mechanisms. Re-execution pattern via `codex-arg0` crate applies sandbox before command starts.

### 5.1 Interface Model

Similar to Claude Code -- the agent's tools are sandboxed transparently:

```
Agent tools:
  - Shell execution (sandboxed via OS primitives)
  - File read/write (restricted by sandbox mode)

Sandbox applied via process re-execution:
  codex-arg0 crate -> detect platform -> apply sandbox -> exec command
```

### 5.2 Sandbox Modes

```
1. read-only:
   - Files readable
   - No edits, no commands, no network without approval

2. workspace-write (default):
   - Read: system dirs + workspace
   - Write: workspace dir + /tmp only
   - Network: disabled by default
   - Protected paths: .git, .agents, .codex (read-only recursively)

3. danger-full-access:
   - Unrestricted (not recommended)
```

### 5.3 Platform Implementations

```
macOS:
  - Seatbelt profiles via sandbox-exec / sandbox_init()
  - Profile compiled at runtime matching selected mode
  - Platform-specific policies appended for tool compatibility

Linux:
  - Landlock v5.13+ for filesystem access control
  - seccomp for syscall filtering
  - Alternative: bwrap with proxy-only bridge (fails closed if no loopback)
  - Read: /usr, /lib, system dirs
  - Write: workspace dir only in workspace-write mode

Windows:
  - WSL: uses Linux implementation
  - Native: restricted security tokens + job objects (codex-windows-sandbox crate)
```

### 5.4 Codex Cloud

```
Two-phase runtime:
  1. Setup phase: network access ON, install dependencies
  2. Agent phase: network access OFF by default
     - Secrets removed before agent phase starts
     - Enable internet via: network_access = true in [sandbox_workspace_write]

Environment control:
  - shell_environment_policy: clean/trimmed/override
  - Prevents secret leakage via env vars
```

### 5.5 Key Design Choices

- **Local execution + OS primitives**: Same approach as Claude Code
- **Three explicit modes**: read-only, workspace-write, full-access
- **No remote VM**: Agent runs on developer's machine
- **Two-phase cloud model**: Setup (online) -> Agent (offline by default)
- **Protected paths**: .git always read-only even in write mode
- **Process re-execution pattern**: Sandbox constraints applied before command starts
- **No lifecycle management**: No create/destroy/checkpoint

---

## 6. MCP Standard for Sandboxes

There is **no standard MCP tool schema for sandboxes**. The MCP spec (November 2025) defines the protocol for tool registration/calling but not specific tool interfaces. Each provider defines their own tool names and schemas.

### 6.1 Common MCP Tool Patterns Across Providers

```
Despite no standard, a clear pattern emerges:

LIFECYCLE:
  create_sandbox / e2b_create_sandbox    { template?, timeout?, resources? }
  destroy_sandbox / e2b_kill_sandbox     { sandboxId }

EXECUTION:
  execute_command / execute_code          { sandboxId, command|code, language? }
  -> { stdout, stderr, exitCode }

FILES:
  read_file / download_file              { sandboxId, path }
  write_file / upload_file               { sandboxId, path, content }
  list_files / list_directory            { sandboxId, path }
  delete_file                            { sandboxId, path }

GIT:
  git_clone                              { url, branch?, path? }

PREVIEW:
  get_url / preview_link                 { sandboxId, port }
```

### 6.2 MCP November 2025 Spec: Tasks Primitive

The newest MCP spec adds **Tasks** for async long-running operations:
```
- Create a task, return a handle
- Publish progress updates
- Deliver results when complete
- Tool calling in sampling requests (server-side agent loops)
- Parallel tool calls for concurrent execution
```

This maps well to: sandbox creation, long-running builds, streaming command output.

---

## 7. Synthesis: The Standard Sandbox Interface

### 7.1 What Every Agent Expects

Based on all providers, agents expect these core capabilities:

| Capability | E2B | Daytona | Sprites | Claude Code | Codex |
|-----------|-----|---------|---------|-------------|-------|
| Create sandbox | `Sandbox.create()` | `daytona.create()` | `PUT /sprites/{name}` | implicit (session) | implicit (session) |
| Execute command | `commands.run()` | `process.executeCommand()` | `WSS exec` | `Bash()` tool | shell tool |
| Streaming output | `onStdout/onStderr` | `onStdout` callback | WebSocket binary | terminal | terminal |
| Read file | `files.read()` | `fs.downloadFile()` | `GET fs/read` | `Read()` tool | file read tool |
| Write file | `files.write()` | `fs.uploadFile()` | `PUT fs/write` | `Edit()` tool | file write tool |
| List dir | `files.list()` | `fs.listFiles()` | `GET fs/list` | `ls` via Bash | `ls` via shell |
| Kill process | `commands.kill(pid)` | session management | `kill` session | Ctrl-C | Ctrl-C |
| Network control | deny/allow lists | block_all + CIDR allow | DNS domain rules | proxy + domain list | mode-based |
| Checkpoint | `betaPause()` / snapshot | archive (fs only) | checkpoint/restore | none | none |
| Destroy | `sandbox.kill()` | `sandbox.delete()` | `DELETE sprite` | session end | session end |

### 7.2 The Minimum Viable Sandbox API for VZ

For VZ to serve as a sandbox backend for coding agents, it needs:

```
TIER 1 - MUST HAVE (every agent needs these):
  1. Create/acquire sandbox          -> sandbox_id
  2. Execute command (sync)          -> { stdout, stderr, exit_code }
  3. Execute command (streaming)     -> stream of stdout/stderr chunks
  4. Read file                       -> bytes | string
  5. Write file                      -> ok
  6. List directory                  -> entries[]
  7. Destroy sandbox                 -> ok

TIER 2 - EXPECTED (most agent frameworks use these):
  8. File delete / rename / mkdir
  9. Process listing and kill
  10. Environment variable injection
  11. Network egress control (block-all + allowlist)
  12. Timeout / auto-destroy
  13. Port forwarding / URL exposure
  14. Send stdin to running process

TIER 3 - DIFFERENTIATING (advanced features):
  15. Checkpoint / restore (full memory + fs)
  16. Snapshot / template creation
  17. File watching (WebSocket)
  18. Stateful code interpreter (REPL context)
  19. Sessions (long-running, reconnectable)
  20. Background services (survive hibernation)
  21. Resource limits (CPU, memory, disk)
  22. Filesystem search
  23. Privilege policies
```

### 7.3 Output Format Consensus

All providers converge on this command result shape:

```typescript
interface CommandResult {
  stdout: string       // Captured standard output
  stderr: string       // Captured standard error
  exitCode: number     // Process exit code
}

// Streaming variant:
interface StreamChunk {
  type: 'stdout' | 'stderr' | 'exit'
  data: string | number
}
```

### 7.4 Transport Patterns

```
SDK calls (E2B, Daytona):
  - gRPC for hot-path (commands, files)
  - REST for lifecycle

REST API (Sprites):
  - REST for lifecycle + files
  - WebSocket for exec + file watching
  - Binary protocol for stdin/stdout/stderr multiplexing

MCP (all):
  - JSON-RPC over stdio or SSE
  - Tool calls with JSON parameters
  - Results as JSON or text content blocks

Local (Claude Code, Codex):
  - Direct process execution with OS-level sandboxing
  - No network API -- everything is local
```

### 7.5 What VZ Should Expose

Given VZ's architecture (Virtualization.framework VMs with vsock + VirtioFS):

```
gRPC service (for SDK integration):
  rpc CreateSandbox(CreateReq) returns (SandboxInfo)
  rpc DestroySandbox(DestroyReq) returns (Empty)
  rpc Exec(ExecReq) returns (stream ExecChunk)        // Streaming
  rpc ExecSync(ExecReq) returns (ExecResult)           // One-shot
  rpc ReadFile(FileReq) returns (stream Bytes)
  rpc WriteFile(stream WriteChunk) returns (FileInfo)
  rpc ListDir(DirReq) returns (DirEntries)
  rpc DeleteFile(FileReq) returns (Empty)
  rpc MkDir(DirReq) returns (Empty)
  rpc Kill(KillReq) returns (Empty)
  rpc ListProcesses(Empty) returns (ProcessList)
  rpc SendStdin(StdinReq) returns (Empty)
  rpc Checkpoint(CheckpointReq) returns (CheckpointInfo)
  rpc Restore(RestoreReq) returns (stream ProgressEvent)
  rpc SetNetworkPolicy(PolicyReq) returns (Empty)
  rpc GetHost(PortReq) returns (HostInfo)

MCP server (for agent integration):
  vz_create_sandbox    { template?, timeout?, resources? }
  vz_destroy_sandbox   { sandboxId }
  vz_exec              { sandboxId, command, cwd?, envs?, timeout? }
  vz_read_file         { sandboxId, path }
  vz_write_file        { sandboxId, path, content }
  vz_list_dir          { sandboxId, path }
  vz_delete_file       { sandboxId, path }
  vz_checkpoint        { sandboxId, comment? }
  vz_restore           { sandboxId, checkpointId }
  vz_set_network       { sandboxId, blockAll?, allowDomains? }
  vz_preview_url       { sandboxId, port }
```

### 7.6 VZ's Unique Advantages

Based on this landscape analysis, VZ has potential differentiators:

1. **macOS-native VMs**: No provider offers macOS sandbox VMs (all are Linux)
2. **Hardware-backed save/restore**: Virtualization.framework's save state is hardware-encrypted
3. **VirtioFS for filesystem**: Near-native file I/O performance vs. network-based fs
4. **vsock for communication**: Zero-config host-guest channel, no network overhead
5. **Dual backend**: macOS VMs locally + Linux containers on Linux hosts
6. **Local execution**: Like Claude Code/Codex but with actual VM isolation

---

## Sources

- [E2B Documentation](https://e2b.dev/docs)
- [E2B SDK Reference - Sandbox](https://e2b.dev/docs/sdk-reference/js-sdk/v1.0.1/sandbox)
- [E2B SDK Reference - Filesystem](https://e2b.dev/docs/sdk-reference/js-sdk/v1.0.1/filesystem)
- [E2B SDK Reference - Commands](https://e2b.dev/docs/sdk-reference/js-sdk/v1.4.0/commands)
- [E2B Sandbox Persistence](https://e2b.dev/docs/sandbox/persistence)
- [E2B Internet Access](https://e2b.dev/docs/sandbox/internet-access)
- [E2B MCP Server (GitHub)](https://github.com/e2b-dev/mcp-server)
- [Daytona Documentation](https://www.daytona.io/docs/en/)
- [Daytona Process & Code Execution](https://www.daytona.io/docs/en/process-code-execution/)
- [Daytona File System Operations](https://www.daytona.io/docs/en/file-system-operations/)
- [Daytona Sandboxes](https://www.daytona.io/docs/en/sandboxes/)
- [Daytona Network Limits](https://www.daytona.io/docs/en/network-limits/)
- [Daytona MCP Server](https://www.daytona.io/docs/en/mcp/)
- [Sprites.dev](https://sprites.dev/)
- [Sprites API Reference](https://docs.sprites.dev/api/v001-rc30/)
- [Sprites Filesystem API](https://docs.sprites.dev/api/v001-rc30/filesystem/)
- [Sprites Design & Implementation (Fly Blog)](https://fly.io/blog/design-and-implementation/)
- [Sprites Exec API](https://sprites.dev/api/sprites/exec)
- [Sprites Checkpoint API](https://sprites.dev/api/sprites/checkpoints)
- [Sprites Network Policy](https://sprites.dev/api/sprites/policies)
- [Working with Sprites](https://docs.sprites.dev/working-with-sprites/)
- [Claude Code Sandboxing (Anthropic Engineering)](https://www.anthropic.com/engineering/claude-code-sandboxing)
- [Claude Code Sandboxing Docs](https://code.claude.com/docs/en/sandboxing)
- [Codex Security](https://developers.openai.com/codex/security)
- [Codex CLI Reference](https://developers.openai.com/codex/cli/reference/)
- [MCP Specification 2025-11-25](https://modelcontextprotocol.io/specification/2025-11-25)
- [Simon Willison on Sprites](https://simonwillison.net/2026/Jan/9/sprites-dev/)
