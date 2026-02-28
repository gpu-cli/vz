use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bollard_buildkit_proto::google::protobuf::Timestamp;
use bollard_buildkit_proto::moby::buildkit::v1::{StatusResponse, Vertex};

/// Build log stream source emitted by BuildKit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildLogStream {
    Stdout,
    Stderr,
}

/// Typed build progress events for CLI/UI consumers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildProgress {
    StepStarted {
        id: String,
        name: String,
    },
    StepCached {
        id: String,
        name: String,
    },
    StepCompleted {
        id: String,
        name: String,
        duration: Option<Duration>,
    },
    StepFailed {
        id: String,
        name: String,
        error: String,
    },
    Log {
        id: String,
        stream: BuildLogStream,
        data: Vec<u8>,
    },
    Warning {
        id: String,
        message: String,
        detail: Option<String>,
        url: Option<String>,
    },
}

/// Stateful mapper from BuildKit status updates to typed progress events.
#[derive(Debug, Default)]
pub struct BuildProgressMapper {
    started: HashSet<String>,
    cached: HashSet<String>,
    completed: HashSet<String>,
    failed: HashSet<String>,
    warnings: HashSet<String>,
}

impl BuildProgressMapper {
    /// Map one BuildKit `StatusResponse` to zero or more typed events.
    pub fn map_status(&mut self, status: StatusResponse) -> Vec<BuildProgress> {
        let mut events = Vec::new();

        for vertex in status.vertexes {
            let id = vertex_id(&vertex);
            let name = vertex_name(&vertex, &id);

            if vertex.cached && self.cached.insert(id.clone()) {
                events.push(BuildProgress::StepCached {
                    id: id.clone(),
                    name: name.clone(),
                });
            }
            if vertex.started.is_some() && self.started.insert(id.clone()) {
                events.push(BuildProgress::StepStarted {
                    id: id.clone(),
                    name: name.clone(),
                });
            }
            if !vertex.error.is_empty() && self.failed.insert(id.clone()) {
                events.push(BuildProgress::StepFailed {
                    id: id.clone(),
                    name: name.clone(),
                    error: vertex.error,
                });
            }
            if vertex.completed.is_some() && self.completed.insert(id.clone()) {
                events.push(BuildProgress::StepCompleted {
                    id,
                    name,
                    duration: duration_between(vertex.started.as_ref(), vertex.completed.as_ref()),
                });
            }
        }

        for log in status.logs {
            let id = if log.vertex.is_empty() {
                "unknown".to_string()
            } else {
                log.vertex
            };
            events.push(BuildProgress::Log {
                id,
                stream: map_stream(log.stream),
                data: log.msg,
            });
        }

        for warning in status.warnings {
            let id = if warning.vertex.is_empty() {
                "unknown".to_string()
            } else {
                warning.vertex
            };
            let message = String::from_utf8_lossy(&warning.short).trim().to_string();
            let warning_key = format!("{id}:{message}");
            if !self.warnings.insert(warning_key) {
                continue;
            }

            let detail = if warning.detail.is_empty() {
                None
            } else {
                Some(
                    warning
                        .detail
                        .iter()
                        .map(|entry| String::from_utf8_lossy(entry).trim().to_string())
                        .collect::<Vec<_>>()
                        .join("\n"),
                )
            };
            let url = (!warning.url.is_empty()).then_some(warning.url);
            events.push(BuildProgress::Warning {
                id,
                message,
                detail,
                url,
            });
        }

        events
    }
}

fn vertex_id(vertex: &Vertex) -> String {
    if vertex.digest.is_empty() {
        vertex.name.clone()
    } else {
        vertex.digest.clone()
    }
}

fn vertex_name(vertex: &Vertex, fallback: &str) -> String {
    if vertex.name.is_empty() {
        fallback.to_string()
    } else {
        vertex.name.clone()
    }
}

fn map_stream(stream: i64) -> BuildLogStream {
    match stream {
        2 => BuildLogStream::Stderr,
        _ => BuildLogStream::Stdout,
    }
}

fn duration_between(start: Option<&Timestamp>, end: Option<&Timestamp>) -> Option<Duration> {
    let start = start.and_then(timestamp_to_system_time)?;
    let end = end.and_then(timestamp_to_system_time)?;
    end.duration_since(start).ok()
}

fn timestamp_to_system_time(timestamp: &Timestamp) -> Option<SystemTime> {
    if timestamp.seconds < 0 {
        return None;
    }
    if !(0..1_000_000_000).contains(&timestamp.nanos) {
        return None;
    }

    UNIX_EPOCH.checked_add(Duration::new(
        u64::try_from(timestamp.seconds).ok()?,
        u32::try_from(timestamp.nanos).ok()?,
    ))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use bollard_buildkit_proto::google::protobuf::Timestamp;
    use bollard_buildkit_proto::moby::buildkit::v1::{
        StatusResponse, Vertex, VertexLog, VertexWarning,
    };

    use super::{BuildLogStream, BuildProgress, BuildProgressMapper};

    fn timestamp(seconds: i64) -> Option<Timestamp> {
        Some(Timestamp { seconds, nanos: 0 })
    }

    #[test]
    fn maps_vertex_lifecycle_events_once() {
        let mut mapper = BuildProgressMapper::default();
        let response = StatusResponse {
            vertexes: vec![
                Vertex {
                    digest: "sha256:abc".to_string(),
                    name: "RUN echo hello".to_string(),
                    started: timestamp(10),
                    completed: timestamp(13),
                    ..Vertex::default()
                },
                Vertex {
                    digest: "sha256:cached".to_string(),
                    name: "COPY . .".to_string(),
                    cached: true,
                    ..Vertex::default()
                },
            ],
            ..StatusResponse::default()
        };

        let first = mapper.map_status(response.clone());
        let second = mapper.map_status(response);

        assert!(first.contains(&BuildProgress::StepStarted {
            id: "sha256:abc".to_string(),
            name: "RUN echo hello".to_string(),
        }));
        assert!(first.contains(&BuildProgress::StepCompleted {
            id: "sha256:abc".to_string(),
            name: "RUN echo hello".to_string(),
            duration: Some(std::time::Duration::from_secs(3)),
        }));
        assert!(first.contains(&BuildProgress::StepCached {
            id: "sha256:cached".to_string(),
            name: "COPY . .".to_string(),
        }));
        assert!(second.is_empty());
    }

    #[test]
    fn maps_logs_and_warnings() {
        let mut mapper = BuildProgressMapper::default();
        let response = StatusResponse {
            logs: vec![VertexLog {
                vertex: "sha256:abc".to_string(),
                stream: 2,
                msg: b"stderr line".to_vec(),
                ..VertexLog::default()
            }],
            warnings: vec![VertexWarning {
                vertex: "sha256:abc".to_string(),
                short: b"deprecated".to_vec(),
                detail: vec![b"use newer syntax".to_vec()],
                url: "https://example.com/warn".to_string(),
                ..VertexWarning::default()
            }],
            ..StatusResponse::default()
        };

        let events = mapper.map_status(response);
        assert!(events.contains(&BuildProgress::Log {
            id: "sha256:abc".to_string(),
            stream: BuildLogStream::Stderr,
            data: b"stderr line".to_vec(),
        }));
        assert!(events.contains(&BuildProgress::Warning {
            id: "sha256:abc".to_string(),
            message: "deprecated".to_string(),
            detail: Some("use newer syntax".to_string()),
            url: Some("https://example.com/warn".to_string()),
        }));
    }
}
