use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tonic::codegen::http;
use tower::{Layer, Service};

const LATENCY_BUCKETS_SECONDS: [f64; 11] =
    [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

#[derive(Debug, Default)]
pub(in crate::grpc) struct GrpcObservability {
    inner: Mutex<GrpcObservabilityInner>,
}

#[derive(Debug, Default)]
struct GrpcObservabilityInner {
    request_counts: BTreeMap<RequestCountKey, u64>,
    request_durations: BTreeMap<String, Histogram>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RequestCountKey {
    rpc_method: String,
    grpc_status: String,
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

impl GrpcObservability {
    pub(in crate::grpc) fn record_rpc(
        &self,
        rpc_method: &str,
        grpc_status: &str,
        elapsed: Duration,
    ) {
        let mut guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        let count_key = RequestCountKey {
            rpc_method: rpc_method.to_string(),
            grpc_status: grpc_status.to_string(),
        };
        *guard.request_counts.entry(count_key).or_insert(0) += 1;
        guard
            .request_durations
            .entry(rpc_method.to_string())
            .or_default()
            .observe(elapsed);
    }

    pub(in crate::grpc) fn render_prometheus(&self) -> String {
        let guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        let mut output = String::new();
        output.push_str(
            "# HELP vz_runtimed_grpc_requests_total Total gRPC requests handled by vz-runtimed.\n",
        );
        output.push_str("# TYPE vz_runtimed_grpc_requests_total counter\n");
        for (key, count) in &guard.request_counts {
            output.push_str(&format!(
                "vz_runtimed_grpc_requests_total{{rpc_method=\"{}\",grpc_status=\"{}\"}} {count}\n",
                escape_label_value(&key.rpc_method),
                escape_label_value(&key.grpc_status),
            ));
        }

        output.push_str(
            "# HELP vz_runtimed_grpc_request_duration_seconds gRPC request latency in seconds.\n",
        );
        output.push_str("# TYPE vz_runtimed_grpc_request_duration_seconds histogram\n");
        for (rpc_method, histogram) in &guard.request_durations {
            let rpc_method = escape_label_value(rpc_method);
            let mut cumulative = 0_u64;
            for (index, upper_bound) in LATENCY_BUCKETS_SECONDS.iter().enumerate() {
                cumulative += histogram.buckets[index];
                output.push_str(&format!(
                    "vz_runtimed_grpc_request_duration_seconds_bucket{{rpc_method=\"{rpc_method}\",le=\"{}\"}} {cumulative}\n",
                    format_bucket(*upper_bound),
                ));
            }
            cumulative += histogram.buckets[LATENCY_BUCKETS_SECONDS.len()];
            output.push_str(&format!(
                "vz_runtimed_grpc_request_duration_seconds_bucket{{rpc_method=\"{rpc_method}\",le=\"+Inf\"}} {cumulative}\n",
            ));
            output.push_str(&format!(
                "vz_runtimed_grpc_request_duration_seconds_sum{{rpc_method=\"{rpc_method}\"}} {}\n",
                histogram.sum_seconds
            ));
            output.push_str(&format!(
                "vz_runtimed_grpc_request_duration_seconds_count{{rpc_method=\"{rpc_method}\"}} {}\n",
                histogram.count
            ));
        }

        output
    }
}

#[derive(Clone)]
pub(in crate::grpc) struct GrpcMetricsLayer {
    observability: Arc<GrpcObservability>,
}

impl GrpcMetricsLayer {
    pub(in crate::grpc) fn new(observability: Arc<GrpcObservability>) -> Self {
        Self { observability }
    }
}

impl<S> Layer<S> for GrpcMetricsLayer {
    type Service = GrpcMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcMetricsService {
            inner,
            observability: self.observability.clone(),
        }
    }
}

#[derive(Clone)]
pub(in crate::grpc) struct GrpcMetricsService<S> {
    inner: S,
    observability: Arc<GrpcObservability>,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for GrpcMetricsService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
        let rpc_method = normalize_rpc_method(request.uri().path());
        let started_at = Instant::now();
        let observability = self.observability.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let result = inner.call(request).await;
            let grpc_status = match &result {
                Ok(response) => grpc_status_from_response(response),
                Err(_) => "transport_error".to_string(),
            };
            observability.record_rpc(&rpc_method, &grpc_status, started_at.elapsed());
            result
        })
    }
}

fn normalize_rpc_method(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        "/unknown".to_string()
    } else {
        trimmed.to_string()
    }
}

fn grpc_status_from_response<Body>(response: &http::Response<Body>) -> String {
    if let Some(value) = response
        .headers()
        .get("grpc-status")
        .and_then(|value| value.to_str().ok())
    {
        return value.to_string();
    }

    if response.status().is_success() {
        "0".to_string()
    } else {
        format!("http_{}", response.status().as_u16())
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
    fn render_prometheus_includes_request_totals_and_histograms() {
        let observability = GrpcObservability::default();
        observability.record_rpc(
            "/vz.runtime.v2.SandboxService/CreateSandbox",
            "0",
            Duration::from_millis(9),
        );

        let output = observability.render_prometheus();
        assert!(output.contains("vz_runtimed_grpc_requests_total"));
        assert!(
            output.contains(
                "vz_runtimed_grpc_requests_total{rpc_method=\"/vz.runtime.v2.SandboxService/CreateSandbox\",grpc_status=\"0\"} 1"
            )
        );
        assert!(output.contains(
            "vz_runtimed_grpc_request_duration_seconds_count{rpc_method=\"/vz.runtime.v2.SandboxService/CreateSandbox\"} 1"
        ));
    }

    #[test]
    fn normalize_rpc_method_defaults_when_path_empty() {
        assert_eq!(normalize_rpc_method(""), "/unknown");
        assert_eq!(normalize_rpc_method("   "), "/unknown");
        assert_eq!(
            normalize_rpc_method("/vz.runtime.v2.EventService/ListEvents"),
            "/vz.runtime.v2.EventService/ListEvents"
        );
    }
}
