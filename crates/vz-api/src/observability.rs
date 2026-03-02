use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use axum::http::StatusCode;

const LATENCY_BUCKETS_SECONDS: [f64; 11] =
    [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

#[derive(Debug, Default)]
pub(crate) struct ApiObservability {
    inner: Mutex<ApiObservabilityInner>,
}

#[derive(Debug, Default)]
struct ApiObservabilityInner {
    request_counts: BTreeMap<RequestCountKey, u64>,
    request_durations: BTreeMap<DurationKey, Histogram>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RequestCountKey {
    method: String,
    route: String,
    status_class: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DurationKey {
    method: String,
    route: String,
}

#[derive(Debug, Clone)]
struct Histogram {
    buckets: [u64; LATENCY_BUCKETS_SECONDS.len() + 1],
    count: u64,
    sum_seconds: f64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: [0; LATENCY_BUCKETS_SECONDS.len() + 1],
            count: 0,
            sum_seconds: 0.0,
        }
    }
}

impl Histogram {
    fn observe(&mut self, elapsed: Duration) {
        let value_seconds = elapsed.as_secs_f64();
        let mut bucket_index = LATENCY_BUCKETS_SECONDS.len();
        for (index, upper_bound) in LATENCY_BUCKETS_SECONDS.iter().enumerate() {
            if value_seconds <= *upper_bound {
                bucket_index = index;
                break;
            }
        }

        self.buckets[bucket_index] += 1;
        self.count += 1;
        self.sum_seconds += value_seconds;
    }
}

impl ApiObservability {
    pub(crate) fn record_http_request(
        &self,
        method: &str,
        route: &str,
        status: StatusCode,
        elapsed: Duration,
    ) {
        let status_class = status_class(status);
        let mut guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        let request_key = RequestCountKey {
            method: method.to_string(),
            route: route.to_string(),
            status_class,
        };
        *guard.request_counts.entry(request_key).or_insert(0) += 1;

        let duration_key = DurationKey {
            method: method.to_string(),
            route: route.to_string(),
        };
        guard
            .request_durations
            .entry(duration_key)
            .or_default()
            .observe(elapsed);
    }

    pub(crate) fn render_prometheus(&self) -> String {
        let guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        let mut output = String::new();
        output.push_str(
            "# HELP vz_api_http_requests_total Total HTTP requests handled by vz-api.\n",
        );
        output.push_str("# TYPE vz_api_http_requests_total counter\n");
        for (key, count) in &guard.request_counts {
            output.push_str(&format!(
                "vz_api_http_requests_total{{method=\"{}\",route=\"{}\",status_class=\"{}\"}} {count}\n",
                escape_label_value(&key.method),
                escape_label_value(&key.route),
                key.status_class,
            ));
        }

        output.push_str(
            "# HELP vz_api_http_request_duration_seconds HTTP request latency in seconds.\n",
        );
        output.push_str("# TYPE vz_api_http_request_duration_seconds histogram\n");
        for (key, histogram) in &guard.request_durations {
            let method = escape_label_value(&key.method);
            let route = escape_label_value(&key.route);
            let mut cumulative = 0_u64;
            for (index, upper_bound) in LATENCY_BUCKETS_SECONDS.iter().enumerate() {
                cumulative += histogram.buckets[index];
                output.push_str(&format!(
                    "vz_api_http_request_duration_seconds_bucket{{method=\"{method}\",route=\"{route}\",le=\"{}\"}} {cumulative}\n",
                    format_bucket(*upper_bound),
                ));
            }
            cumulative += histogram.buckets[LATENCY_BUCKETS_SECONDS.len()];
            output.push_str(&format!(
                "vz_api_http_request_duration_seconds_bucket{{method=\"{method}\",route=\"{route}\",le=\"+Inf\"}} {cumulative}\n",
            ));
            output.push_str(&format!(
                "vz_api_http_request_duration_seconds_sum{{method=\"{method}\",route=\"{route}\"}} {}\n",
                histogram.sum_seconds
            ));
            output.push_str(&format!(
                "vz_api_http_request_duration_seconds_count{{method=\"{method}\",route=\"{route}\"}} {}\n",
                histogram.count
            ));
        }

        output
    }
}

pub(crate) fn normalize_http_path_for_metrics(path: &str) -> String {
    if path.is_empty() {
        return "/unknown".to_string();
    }

    let mut normalized_segments = Vec::new();
    let mut previous = "";
    for segment in path.trim_start_matches('/').split('/') {
        if segment.is_empty() {
            continue;
        }
        if let Some(placeholder) = metric_placeholder_for_segment(previous, segment) {
            normalized_segments.push(placeholder.to_string());
        } else {
            normalized_segments.push(segment.to_string());
        }
        previous = segment;
    }

    if normalized_segments.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", normalized_segments.join("/"))
    }
}

fn metric_placeholder_for_segment(previous: &str, segment: &str) -> Option<&'static str> {
    if is_stable_metrics_segment(segment) {
        return None;
    }

    match previous {
        "sandboxes" => Some("{sandbox_id}"),
        "leases" => Some("{lease_id}"),
        "executions" => Some("{execution_id}"),
        "checkpoints" => Some("{checkpoint_id}"),
        "containers" => Some("{container_id}"),
        "images" => Some("{image_ref}"),
        "builds" => Some("{build_id}"),
        "receipts" => Some("{receipt_id}"),
        "events" => Some("{stack_name}"),
        "stacks" => Some("{stack_name}"),
        "services" => Some("{service_name}"),
        _ if looks_dynamic(segment) => Some("{id}"),
        _ => None,
    }
}

fn is_stable_metrics_segment(segment: &str) -> bool {
    matches!(
        segment,
        "openapi.json"
            | "metrics"
            | "v1"
            | "capabilities"
            | "stacks"
            | "apply"
            | "teardown"
            | "status"
            | "events"
            | "logs"
            | "services"
            | "stop"
            | "start"
            | "restart"
            | "run-container"
            | "create"
            | "remove"
            | "stream"
            | "ws"
            | "sandboxes"
            | "shell"
            | "open"
            | "close"
            | "leases"
            | "heartbeat"
            | "executions"
            | "resize"
            | "stdin"
            | "signal"
            | "checkpoints"
            | "restore"
            | "fork"
            | "children"
            | "containers"
            | "images"
            | "pull"
            | "prune"
            | "builds"
            | "receipts"
            | "files"
            | "read"
            | "write"
            | "list"
            | "mkdir"
            | "move"
            | "copy"
            | "chmod"
            | "chown"
    )
}

fn looks_dynamic(segment: &str) -> bool {
    if segment.len() < 4 {
        return false;
    }

    let has_digit = segment.chars().any(|character| character.is_ascii_digit());
    let has_dash = segment.contains('-');
    let has_colon = segment.contains(':');
    has_digit || has_dash || has_colon
}

fn status_class(status: StatusCode) -> &'static str {
    match status.as_u16() / 100 {
        1 => "1xx",
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        5 => "5xx",
        _ => "unknown",
    }
}

fn format_bucket(value: f64) -> String {
    if (value - value.trunc()).abs() < f64::EPSILON {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn escape_label_value(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\");
    escaped.replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_http_path_for_metrics_collapses_dynamic_segments() {
        assert_eq!(
            normalize_http_path_for_metrics("/v1/sandboxes/sbx-123/shell/open"),
            "/v1/sandboxes/{sandbox_id}/shell/open"
        );
        assert_eq!(
            normalize_http_path_for_metrics("/v1/stacks/my-stack/services/web/restart"),
            "/v1/stacks/{stack_name}/services/{service_name}/restart"
        );
        assert_eq!(
            normalize_http_path_for_metrics("/v1/events/runtime-stack/stream"),
            "/v1/events/{stack_name}/stream"
        );
    }

    #[test]
    fn render_prometheus_includes_counts_and_histograms() {
        let observability = ApiObservability::default();
        observability.record_http_request(
            "GET",
            "/v1/capabilities",
            StatusCode::OK,
            Duration::from_millis(8),
        );

        let output = observability.render_prometheus();
        assert!(output.contains("vz_api_http_requests_total"));
        assert!(
            output.contains(
                "vz_api_http_requests_total{method=\"GET\",route=\"/v1/capabilities\",status_class=\"2xx\"} 1"
            )
        );
        assert!(output.contains(
            "vz_api_http_request_duration_seconds_count{method=\"GET\",route=\"/v1/capabilities\"} 1"
        ));
    }
}
