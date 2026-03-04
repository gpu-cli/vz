use super::helpers::split_exec_command;
use super::*;

#[derive(Debug, Deserialize)]
pub(super) struct ApiErrorPayload {
    pub(super) code: String,
    pub(super) message: String,
    pub(super) request_id: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiErrorEnvelope {
    pub(super) error: ApiErrorPayload,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiStackServiceStatus {
    pub(super) service_name: String,
    pub(super) phase: String,
    pub(super) ready: bool,
    pub(super) container_id: String,
    pub(super) last_error: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiApplyStackPayload {
    pub(super) stack_name: String,
    pub(super) changed_actions: u32,
    pub(super) converged: bool,
    pub(super) services_ready: u32,
    pub(super) services_failed: u32,
    pub(super) services: Vec<ApiStackServiceStatus>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiApplyStackResponse {
    pub(super) stack: ApiApplyStackPayload,
}

#[derive(Debug, Serialize)]
pub(super) struct ApiApplyStackRequest {
    pub(super) stack_name: String,
    pub(super) compose_yaml: String,
    pub(super) compose_dir: String,
    pub(super) dry_run: bool,
    pub(super) detach: bool,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiTeardownStackPayload {
    pub(super) stack_name: String,
    pub(super) changed_actions: u32,
    pub(super) removed_volumes: u32,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiTeardownStackResponse {
    pub(super) stack: ApiTeardownStackPayload,
}

#[derive(Debug, Serialize)]
pub(super) struct ApiTeardownStackRequest {
    pub(super) stack_name: String,
    pub(super) dry_run: bool,
    pub(super) remove_volumes: bool,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiStackStatusResponse {
    pub(super) stack_name: String,
    pub(super) services: Vec<ApiStackServiceStatus>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiEventRecord {
    pub(super) id: i64,
    pub(super) stack_name: String,
    pub(super) created_at: String,
    pub(super) event: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiStackEventsResponse {
    pub(super) events: Vec<ApiEventRecord>,
    pub(super) next_cursor: i64,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiStackServiceLog {
    pub(super) service_name: String,
    pub(super) output: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiStackLogsResponse {
    pub(super) logs: Vec<ApiStackServiceLog>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub(super) struct ApiStackServiceActionPayload {
    pub(super) stack_name: String,
    pub(super) service: ApiStackServiceStatus,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiStackServiceActionResponse {
    pub(super) action: ApiStackServiceActionPayload,
}

#[derive(Debug, Serialize)]
pub(super) struct ApiStackRunContainerRequest {
    pub(super) stack_name: String,
    pub(super) service_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) run_service_name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub(super) struct ApiStackRunContainerPayload {
    pub(super) stack_name: String,
    pub(super) service_name: String,
    pub(super) run_service_name: String,
    pub(super) container_id: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiStackRunContainerResponse {
    pub(super) run_container: ApiStackRunContainerPayload,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiSandboxPayload {
    pub(super) sandbox_id: String,
    pub(super) state: String,
    pub(super) labels: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiSandboxListResponse {
    pub(super) sandboxes: Vec<ApiSandboxPayload>,
}

#[derive(Debug, Serialize)]
pub(super) struct ApiCreateExecutionRequest {
    pub(super) container_id: String,
    pub(super) cmd: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) args: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) env_override: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) pty_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) timeout_secs: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiExecutionPayload {
    pub(super) execution_id: String,
    pub(super) state: String,
    pub(super) exit_code: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiExecutionResponse {
    pub(super) execution: ApiExecutionPayload,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiExecutionOutputEvent {
    pub(super) event: String,
    #[serde(default)]
    pub(super) data_base64: Option<String>,
    #[serde(default)]
    pub(super) exit_code: Option<i32>,
    #[serde(default)]
    pub(super) error: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiExecutionStreamErrorBody {
    pub(super) code: String,
    pub(super) message: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiExecutionStreamError {
    pub(super) request_id: String,
    pub(super) error: ApiExecutionStreamErrorBody,
}

pub(super) fn stack_service_status_from_api(
    payload: ApiStackServiceStatus,
) -> runtime_v2::StackServiceStatus {
    runtime_v2::StackServiceStatus {
        service_name: payload.service_name,
        phase: payload.phase,
        ready: payload.ready,
        container_id: payload.container_id,
        last_error: payload.last_error,
    }
}

fn runtime_api_url(path: &str) -> anyhow::Result<String> {
    let base = runtime_api_base_url()?;
    Ok(format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    ))
}

async fn api_error_response(response: reqwest::Response, context: &str) -> anyhow::Error {
    let status = response.status();
    let body = response.bytes().await.unwrap_or_default();
    if let Ok(error) = serde_json::from_slice::<ApiErrorEnvelope>(&body) {
        return anyhow!(
            "{context}: api error {} {} (request_id={})",
            error.error.code,
            error.error.message,
            error.error.request_id
        );
    }
    let snippet = String::from_utf8_lossy(&body);
    anyhow!("{context}: api status {status} body={snippet}")
}

pub(super) async fn api_apply_stack(
    request: ApiApplyStackRequest,
) -> anyhow::Result<ApiApplyStackPayload> {
    let url = runtime_api_url("/v1/stacks/apply")?;
    let response = reqwest::Client::new()
        .post(url)
        .json(&request)
        .send()
        .await
        .context("failed to call api apply stack")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to apply stack via api").await);
    }
    let payload: ApiApplyStackResponse = response
        .json()
        .await
        .context("failed to decode api apply stack response")?;
    Ok(payload.stack)
}

pub(super) async fn api_teardown_stack(
    request: ApiTeardownStackRequest,
) -> anyhow::Result<ApiTeardownStackPayload> {
    let url = runtime_api_url("/v1/stacks/teardown")?;
    let response = reqwest::Client::new()
        .post(url)
        .json(&request)
        .send()
        .await
        .context("failed to call api teardown stack")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to teardown stack via api").await);
    }
    let payload: ApiTeardownStackResponse = response
        .json()
        .await
        .context("failed to decode api teardown stack response")?;
    Ok(payload.stack)
}

pub(super) async fn api_get_stack_status(
    stack_name: &str,
) -> anyhow::Result<ApiStackStatusResponse> {
    let url = runtime_api_url(&format!("/v1/stacks/{stack_name}/status"))?;
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .context("failed to call api stack status")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to get stack status via api").await);
    }
    response
        .json()
        .await
        .context("failed to decode api stack status response")
}

pub(super) async fn api_list_stack_events(
    stack_name: &str,
    after: i64,
    limit: u32,
) -> anyhow::Result<ApiStackEventsResponse> {
    let url = runtime_api_url(&format!(
        "/v1/stacks/{stack_name}/events?after={after}&limit={limit}"
    ))?;
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .context("failed to call api stack events")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to list stack events via api").await);
    }
    response
        .json()
        .await
        .context("failed to decode api stack events response")
}

pub(super) async fn api_get_stack_logs(
    stack_name: &str,
    service: &str,
    tail: u32,
) -> anyhow::Result<ApiStackLogsResponse> {
    let url = if service.trim().is_empty() {
        runtime_api_url(&format!("/v1/stacks/{stack_name}/logs?tail={tail}"))?
    } else {
        runtime_api_url(&format!(
            "/v1/stacks/{stack_name}/logs?service={service}&tail={tail}"
        ))?
    };
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .context("failed to call api stack logs")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to get stack logs via api").await);
    }
    response
        .json()
        .await
        .context("failed to decode api stack logs response")
}

pub(super) async fn api_stack_service_action(
    stack_name: &str,
    service_name: &str,
    action: &str,
) -> anyhow::Result<ApiStackServiceActionPayload> {
    let url = runtime_api_url(&format!(
        "/v1/stacks/{stack_name}/services/{service_name}/{action}"
    ))?;
    let response = reqwest::Client::new()
        .post(url)
        .send()
        .await
        .context("failed to call api stack service action")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed stack service action via api").await);
    }
    let payload: ApiStackServiceActionResponse = response
        .json()
        .await
        .context("failed to decode api stack service action response")?;
    Ok(payload.action)
}

pub(super) async fn api_create_stack_run_container(
    request: ApiStackRunContainerRequest,
) -> anyhow::Result<ApiStackRunContainerPayload> {
    let url = runtime_api_url("/v1/stacks/run-container/create")?;
    let response = reqwest::Client::new()
        .post(url)
        .json(&request)
        .send()
        .await
        .context("failed to call api create stack run container")?;
    if !response.status().is_success() {
        return Err(
            api_error_response(response, "failed to create stack run container via api").await,
        );
    }
    let payload: ApiStackRunContainerResponse = response
        .json()
        .await
        .context("failed to decode api create stack run container response")?;
    Ok(payload.run_container)
}

pub(super) async fn api_remove_stack_run_container(
    request: ApiStackRunContainerRequest,
) -> anyhow::Result<()> {
    let url = runtime_api_url("/v1/stacks/run-container/remove")?;
    let response = reqwest::Client::new()
        .post(url)
        .json(&request)
        .send()
        .await
        .context("failed to call api remove stack run container")?;
    if !response.status().is_success() {
        return Err(
            api_error_response(response, "failed to remove stack run container via api").await,
        );
    }
    Ok(())
}

pub(super) async fn api_list_sandboxes() -> anyhow::Result<Vec<ApiSandboxPayload>> {
    let url = runtime_api_url("/v1/sandboxes")?;
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .context("failed to call api list sandboxes")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to list sandboxes via api").await);
    }
    let payload: ApiSandboxListResponse = response
        .json()
        .await
        .context("failed to decode api list sandboxes response")?;
    Ok(payload.sandboxes)
}

async fn api_create_execution(
    request: ApiCreateExecutionRequest,
) -> anyhow::Result<ApiExecutionPayload> {
    let url = runtime_api_url("/v1/executions")?;
    let response = reqwest::Client::new()
        .post(url)
        .json(&request)
        .send()
        .await
        .context("failed to call api create execution")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to create execution via api").await);
    }
    let payload: ApiExecutionResponse = response
        .json()
        .await
        .context("failed to decode api create execution response")?;
    Ok(payload.execution)
}

async fn api_get_execution(execution_id: &str) -> anyhow::Result<Option<ApiExecutionPayload>> {
    let url = runtime_api_url(&format!("/v1/executions/{execution_id}"))?;
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .context("failed to call api get execution")?;
    if response.status() == HttpStatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to get execution via api").await);
    }
    let payload: ApiExecutionResponse = response
        .json()
        .await
        .context("failed to decode api get execution response")?;
    Ok(Some(payload.execution))
}

async fn api_stream_exec_output(execution_id: &str) -> anyhow::Result<reqwest::Response> {
    let url = runtime_api_url(&format!("/v1/executions/{execution_id}/stream"))?;
    let response = reqwest::Client::new()
        .get(url)
        .header(reqwest::header::ACCEPT, "text/event-stream")
        .send()
        .await
        .context("failed to call api execution output stream")?;
    if !response.status().is_success() {
        return Err(
            api_error_response(response, "failed to stream execution output via api").await,
        );
    }
    Ok(response)
}

fn handle_api_exec_stream_event(
    execution_id: &str,
    payload_json: &str,
    stdout: &mut std::io::StdoutLock<'_>,
    stderr: &mut std::io::StderrLock<'_>,
) -> anyhow::Result<Option<i32>> {
    if let Ok(event) = serde_json::from_str::<ApiExecutionOutputEvent>(payload_json) {
        match event.event.as_str() {
            "stdout" => {
                if let Some(encoded) = event.data_base64 {
                    let chunk = BASE64_STANDARD.decode(encoded).with_context(|| {
                        format!(
                            "failed to decode stdout chunk from api stream for `{execution_id}`"
                        )
                    })?;
                    if !chunk.is_empty() {
                        stdout
                            .write_all(&chunk)
                            .context("failed writing execution stdout")?;
                        stdout.flush().context("failed flushing execution stdout")?;
                    }
                }
                return Ok(None);
            }
            "stderr" => {
                if let Some(encoded) = event.data_base64 {
                    let chunk = BASE64_STANDARD.decode(encoded).with_context(|| {
                        format!(
                            "failed to decode stderr chunk from api stream for `{execution_id}`"
                        )
                    })?;
                    if !chunk.is_empty() {
                        stderr
                            .write_all(&chunk)
                            .context("failed writing execution stderr")?;
                        stderr.flush().context("failed flushing execution stderr")?;
                    }
                }
                return Ok(None);
            }
            "exit_code" => return Ok(event.exit_code),
            "error" => {
                let message = event
                    .error
                    .unwrap_or_else(|| "unknown execution stream error".to_string());
                bail!("execution `{execution_id}` reported error: {message}");
            }
            _ => return Ok(None),
        }
    }

    if let Ok(error) = serde_json::from_str::<ApiExecutionStreamError>(payload_json) {
        bail!(
            "execution stream failed: {} {} (request_id={})",
            error.error.code,
            error.error.message,
            error.request_id
        );
    }

    bail!("received unrecognized execution stream payload: {payload_json}");
}

pub(super) async fn execute_stack_container_command_api(
    container_id: String,
    command: &[String],
) -> anyhow::Result<()> {
    let (cmd, cmd_args) = split_exec_command(command)?;
    let execution = api_create_execution(ApiCreateExecutionRequest {
        container_id,
        cmd,
        args: Some(cmd_args),
        env_override: Some(HashMap::new()),
        pty_mode: Some("inherit".to_string()),
        timeout_secs: None,
    })
    .await?;
    let execution_id = execution.execution_id;
    let mut stream = api_stream_exec_output(&execution_id).await?;
    let mut pending = Vec::<u8>::new();
    let mut event_data = String::new();
    let mut terminal_exit_code = execution.exit_code;

    let stdout_handle = std::io::stdout();
    let stderr_handle = std::io::stderr();
    let mut stdout = stdout_handle.lock();
    let mut stderr = stderr_handle.lock();

    while let Some(chunk) = stream
        .chunk()
        .await
        .with_context(|| format!("failed reading output stream for `{execution_id}`"))?
    {
        pending.extend_from_slice(&chunk);
        while let Some(line_end) = pending.iter().position(|byte| *byte == b'\n') {
            let mut line = pending.drain(..=line_end).collect::<Vec<u8>>();
            if line.last() == Some(&b'\n') {
                let _ = line.pop();
            }
            if line.last() == Some(&b'\r') {
                let _ = line.pop();
            }
            let line = String::from_utf8(line)
                .with_context(|| format!("received non UTF-8 stream line for `{execution_id}`"))?;
            if line.is_empty() {
                if !event_data.is_empty() {
                    if let Some(code) = handle_api_exec_stream_event(
                        &execution_id,
                        &event_data,
                        &mut stdout,
                        &mut stderr,
                    )? {
                        terminal_exit_code = Some(code);
                    }
                    event_data.clear();
                }
                continue;
            }
            if line.starts_with(':') {
                continue;
            }
            if let Some(data_line) = line.strip_prefix("data:") {
                if !event_data.is_empty() {
                    event_data.push('\n');
                }
                event_data.push_str(data_line.trim_start());
            }
        }
    }

    if terminal_exit_code.is_none()
        && !event_data.is_empty()
        && let Some(code) =
            handle_api_exec_stream_event(&execution_id, &event_data, &mut stdout, &mut stderr)?
    {
        terminal_exit_code = Some(code);
    }

    if terminal_exit_code.is_none()
        && let Some(execution) = api_get_execution(&execution_id).await?
    {
        if execution.state.eq_ignore_ascii_case("failed") {
            bail!("execution `{execution_id}` ended in failed state");
        }
        terminal_exit_code = execution.exit_code;
    }

    if terminal_exit_code.unwrap_or(0) != 0 {
        bail!(
            "stack command exited with status {}",
            terminal_exit_code.unwrap_or(1)
        );
    }

    Ok(())
}

pub(super) async fn execute_stack_container_command_daemon(
    client: &mut vz_runtimed_client::DaemonClient,
    container_id: String,
    command: &[String],
) -> anyhow::Result<()> {
    let (cmd, cmd_args) = split_exec_command(command)?;
    let execution_response = client
        .create_execution(runtime_v2::CreateExecutionRequest {
            metadata: None,
            container_id,
            cmd,
            args: cmd_args,
            env_override: HashMap::new(),
            timeout_secs: 0,
            pty_mode: runtime_v2::create_execution_request::PtyMode::Inherit as i32,
        })
        .await
        .context("failed to create execution")?;
    let execution = execution_response
        .execution
        .ok_or_else(|| anyhow::anyhow!("daemon returned missing execution payload"))?;
    let execution_id = execution.execution_id.clone();

    let mut stream = client
        .stream_exec_output(runtime_v2::StreamExecOutputRequest {
            execution_id: execution_id.clone(),
            metadata: None,
        })
        .await
        .with_context(|| format!("failed to stream output for execution `{execution_id}`"))?;

    let mut terminal_exit_code: Option<i32> = None;
    while let Some(event) = stream
        .message()
        .await
        .with_context(|| format!("failed reading output stream for `{execution_id}`"))?
    {
        match event.payload {
            Some(runtime_v2::exec_output_event::Payload::Stdout(chunk)) => {
                if !chunk.is_empty() {
                    let mut stdout = std::io::stdout().lock();
                    stdout
                        .write_all(&chunk)
                        .context("failed writing execution stdout")?;
                    stdout.flush().context("failed flushing execution stdout")?;
                }
            }
            Some(runtime_v2::exec_output_event::Payload::Stderr(chunk)) => {
                if !chunk.is_empty() {
                    let mut stderr = std::io::stderr().lock();
                    stderr
                        .write_all(&chunk)
                        .context("failed writing execution stderr")?;
                    stderr.flush().context("failed flushing execution stderr")?;
                }
            }
            Some(runtime_v2::exec_output_event::Payload::ExitCode(code)) => {
                terminal_exit_code = Some(code);
                break;
            }
            Some(runtime_v2::exec_output_event::Payload::Error(message)) => {
                bail!("execution `{execution_id}` reported error: {message}");
            }
            None => {}
        }
    }

    let exit_code = match terminal_exit_code {
        Some(code) => code,
        None => {
            let execution = client
                .get_execution(runtime_v2::GetExecutionRequest {
                    execution_id: execution_id.clone(),
                    metadata: None,
                })
                .await
                .with_context(|| {
                    format!("failed to load terminal execution state for `{execution_id}`")
                })?
                .execution
                .ok_or_else(|| anyhow::anyhow!("daemon returned missing execution payload"))?;
            if execution.state.eq_ignore_ascii_case("failed") {
                bail!("execution `{execution_id}` ended in failed state");
            }
            execution.exit_code
        }
    };

    if exit_code != 0 {
        bail!("stack command exited with status {exit_code}");
    }

    Ok(())
}
