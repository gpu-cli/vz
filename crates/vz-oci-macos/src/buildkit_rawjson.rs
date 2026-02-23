use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

/// BuildKit `--progress=rawjson` solve status envelope.
///
/// BuildKit emits one JSON object per line. Each object corresponds to the Go
/// `client.SolveStatus` type and may contain any mix of vertices, statuses,
/// logs, and warnings.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildkitSolveStatus {
    #[serde(default)]
    pub vertexes: Vec<BuildkitVertex>,
    #[serde(default)]
    pub statuses: Vec<BuildkitVertexStatus>,
    #[serde(default)]
    pub logs: Vec<BuildkitVertexLog>,
    #[serde(default)]
    pub warnings: Vec<BuildkitVertexWarning>,
}

/// Build graph node metadata from BuildKit rawjson.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildkitVertex {
    #[serde(default)]
    pub digest: String,
    #[serde(default)]
    pub inputs: Vec<String>,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub started: Option<String>,
    #[serde(default)]
    pub completed: Option<String>,
    #[serde(default)]
    pub cached: bool,
    #[serde(default)]
    pub error: String,
    #[serde(default, rename = "progressGroup")]
    pub progress_group: Option<BuildkitProgressGroup>,
}

/// Progress group metadata attached to a vertex.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildkitProgressGroup {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub weak: bool,
}

/// In-flight metric update for a vertex (e.g. download progress).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildkitVertexStatus {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub vertex: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub total: i64,
    #[serde(default)]
    pub current: i64,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub started: Option<String>,
    #[serde(default)]
    pub completed: Option<String>,
}

/// Log chunk emitted by a vertex.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildkitVertexLog {
    #[serde(default)]
    pub vertex: String,
    #[serde(default)]
    pub stream: i64,
    #[serde(default, deserialize_with = "deserialize_optional_bytes")]
    pub data: Vec<u8>,
    #[serde(default)]
    pub timestamp: Option<String>,
}

/// Build warning metadata (lint, best practices, etc.).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildkitVertexWarning {
    #[serde(default)]
    pub vertex: String,
    #[serde(default)]
    pub level: i64,
    #[serde(default, deserialize_with = "deserialize_optional_bytes")]
    pub short: Vec<u8>,
    #[serde(default, deserialize_with = "deserialize_vec_of_optional_bytes")]
    pub detail: Vec<Vec<u8>>,
    #[serde(default)]
    pub url: String,
    #[serde(default, rename = "sourceInfo")]
    pub source_info: Option<BuildkitSourceInfo>,
    #[serde(default, rename = "range")]
    pub ranges: Vec<BuildkitRange>,
}

/// Source file metadata linked to warnings.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildkitSourceInfo {
    #[serde(default)]
    pub filename: String,
    #[serde(default, deserialize_with = "deserialize_optional_bytes")]
    pub data: Vec<u8>,
    #[serde(default)]
    pub definition: Option<serde_json::Value>,
    #[serde(default)]
    pub language: String,
}

/// Source code range in warning metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildkitRange {
    #[serde(default)]
    pub start: Option<BuildkitPosition>,
    #[serde(default)]
    pub end: Option<BuildkitPosition>,
}

/// Source position (0-based character offset in line).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildkitPosition {
    #[serde(default)]
    pub line: i32,
    #[serde(default)]
    pub character: i32,
}

/// Per-line parse error while decoding BuildKit rawjson stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildkitRawJsonLineError {
    pub line: Vec<u8>,
    pub error: String,
}

/// Decode result for one line from BuildKit rawjson output.
pub type BuildkitRawJsonDecodeResult = Result<BuildkitSolveStatus, BuildkitRawJsonLineError>;

/// Parse a single BuildKit rawjson line into a solve status object.
pub fn parse_solve_status_line(line: &[u8]) -> Result<BuildkitSolveStatus, serde_json::Error> {
    serde_json::from_slice::<BuildkitSolveStatus>(line)
}

/// Incremental decoder for BuildKit rawjson stream chunks.
#[derive(Debug, Clone, Default)]
pub struct BuildkitRawJsonStreamDecoder {
    buffered: Vec<u8>,
}

impl BuildkitRawJsonStreamDecoder {
    /// Feed a new output chunk and return all completed line decode results.
    pub fn push_chunk(&mut self, chunk: &[u8]) -> Vec<BuildkitRawJsonDecodeResult> {
        self.buffered.extend_from_slice(chunk);
        self.drain_complete_lines(false)
    }

    /// Flush any trailing partial line when the stream ends.
    pub fn finish(&mut self) -> Vec<BuildkitRawJsonDecodeResult> {
        self.drain_complete_lines(true)
    }

    fn drain_complete_lines(&mut self, include_partial: bool) -> Vec<BuildkitRawJsonDecodeResult> {
        let mut decoded = Vec::new();

        while let Some(newline_idx) = self.buffered.iter().position(|byte| *byte == b'\n') {
            let mut line = self.buffered.drain(..=newline_idx).collect::<Vec<u8>>();
            line.pop(); // newline
            trim_trailing_cr(&mut line);
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            decoded.push(decode_status_line(line));
        }

        if include_partial && !self.buffered.is_empty() {
            let mut line = std::mem::take(&mut self.buffered);
            trim_trailing_cr(&mut line);
            if !line.iter().all(u8::is_ascii_whitespace) {
                decoded.push(decode_status_line(line));
            }
        }

        decoded
    }
}

fn trim_trailing_cr(line: &mut Vec<u8>) {
    while line.last() == Some(&b'\r') {
        line.pop();
    }
}

fn decode_status_line(line: Vec<u8>) -> BuildkitRawJsonDecodeResult {
    match parse_solve_status_line(&line) {
        Ok(status) => Ok(status),
        Err(error) => Err(BuildkitRawJsonLineError {
            line,
            error: error.to_string(),
        }),
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BytesRepr {
    Base64(String),
    Raw(Vec<u8>),
}

fn deserialize_optional_bytes<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<BytesRepr>::deserialize(deserializer)?;
    match value {
        None => Ok(Vec::new()),
        Some(BytesRepr::Raw(bytes)) => Ok(bytes),
        Some(BytesRepr::Base64(encoded)) => STANDARD
            .decode(encoded.as_bytes())
            .map_err(|error| D::Error::custom(format!("invalid base64 bytes: {error}"))),
    }
}

fn deserialize_vec_of_optional_bytes<'de, D>(deserializer: D) -> Result<Vec<Vec<u8>>, D::Error>
where
    D: Deserializer<'de>,
{
    let values = Option::<Vec<BytesRepr>>::deserialize(deserializer)?;
    let Some(values) = values else {
        return Ok(Vec::new());
    };

    let mut out = Vec::with_capacity(values.len());
    for value in values {
        match value {
            BytesRepr::Raw(bytes) => out.push(bytes),
            BytesRepr::Base64(encoded) => {
                let decoded = STANDARD
                    .decode(encoded.as_bytes())
                    .map_err(|error| D::Error::custom(format!("invalid base64 bytes: {error}")))?;
                out.push(decoded);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn parse_line_decodes_all_known_rawjson_types() {
        let raw = r#"{
  "vertexes": [
    {
      "digest": "sha256:abc",
      "inputs": ["sha256:def"],
      "name": "[1/2] FROM docker.io/library/alpine:3.20",
      "cached": true,
      "started": "2026-02-23T13:00:00Z",
      "completed": "2026-02-23T13:00:01Z",
      "progressGroup": {"id":"pull","name":"pulling layers","weak":false}
    }
  ],
  "statuses": [
    {
      "id": "status-1",
      "vertex": "sha256:abc",
      "name": "extracting",
      "current": 42,
      "total": 100,
      "timestamp": "2026-02-23T13:00:00Z",
      "started": "2026-02-23T13:00:00Z"
    }
  ],
  "logs": [
    {
      "vertex": "sha256:abc",
      "stream": 1,
      "data": "aGVsbG8K",
      "timestamp": "2026-02-23T13:00:00Z"
    }
  ],
  "warnings": [
    {
      "vertex": "sha256:abc",
      "level": 1,
      "short": "d2FybmluZw==",
      "detail": ["ZGV0YWlsLTE=", "ZGV0YWlsLTI="],
      "url": "https://example.com/warn",
      "sourceInfo": {
        "filename": "Dockerfile",
        "data": "RlJPTSBhbHBpbmU6My4yMAo=",
        "language": "dockerfile"
      },
      "range": [
        {
          "start": {"line": 1, "character": 0},
          "end": {"line": 1, "character": 8}
        }
      ]
    }
  ]
}"#;

        let parsed = parse_solve_status_line(raw.as_bytes()).unwrap();
        assert_eq!(parsed.vertexes.len(), 1);
        assert_eq!(parsed.statuses.len(), 1);
        assert_eq!(parsed.logs.len(), 1);
        assert_eq!(parsed.warnings.len(), 1);

        assert_eq!(parsed.logs[0].data, b"hello\n");
        assert_eq!(parsed.warnings[0].short, b"warning");
        assert_eq!(parsed.warnings[0].detail[0], b"detail-1");
        assert_eq!(
            parsed.warnings[0]
                .source_info
                .as_ref()
                .unwrap()
                .filename
                .as_str(),
            "Dockerfile"
        );
        assert_eq!(
            parsed.warnings[0]
                .source_info
                .as_ref()
                .unwrap()
                .data
                .as_slice(),
            b"FROM alpine:3.20\n"
        );
    }

    #[test]
    fn stream_decoder_handles_split_chunks_and_flushes_tail() {
        let line1 = r#"{"vertexes":[{"digest":"sha256:1","name":"step"}]}"#;
        let line2 = r#"{"logs":[{"stream":1,"data":"YmxvY2stMg=="}]}"#;
        let payload = format!("{line1}\n{line2}");

        let mut decoder = BuildkitRawJsonStreamDecoder::default();
        let first = decoder.push_chunk(&payload.as_bytes()[..line1.len() + 1]);
        assert_eq!(first.len(), 1);
        assert!(first[0].is_ok());

        let second = decoder.push_chunk(&payload.as_bytes()[line1.len() + 1..line1.len() + 11]);
        assert!(second.is_empty());

        let tail = decoder.push_chunk(&payload.as_bytes()[line1.len() + 11..]);
        assert!(tail.is_empty());

        let flushed = decoder.finish();
        assert_eq!(flushed.len(), 1);
        let parsed = flushed.into_iter().next().unwrap().unwrap();
        assert_eq!(parsed.logs[0].data, b"block-2");
    }

    #[test]
    fn stream_decoder_reports_invalid_lines() {
        let mut decoder = BuildkitRawJsonStreamDecoder::default();
        let events = decoder.push_chunk(b"not-json\n");
        assert_eq!(events.len(), 1);

        let error = events.into_iter().next().unwrap().unwrap_err();
        assert_eq!(error.line, b"not-json");
        assert!(error.error.contains("expected ident"));
    }
}
