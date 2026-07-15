use std::{sync::Arc, time::Instant};

use prometheus_client::{
    encoding::{text::encode, EncodeLabelSet},
    metrics::{
        counter::Counter,
        family::{Family, MetricConstructor},
        gauge::Gauge,
        histogram::Histogram,
        info::Info,
    },
    registry::Registry,
};

use crate::error::ApiError;

pub(crate) const INGEST_STATES: [&str; 8] = [
    "queued",
    "parsing",
    "parsed",
    "fragmenting",
    "indexing",
    "completed",
    "failed",
    "other",
];
pub(crate) const OPERATION_STATUSES: [&str; 6] = [
    "pending",
    "primary_committed",
    "effects_submitted",
    "partially_failed",
    "completed",
    "failed",
];

const HTTP_DURATION_BUCKETS_SECONDS: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct HttpRequestLabels {
    method: String,
    route: String,
    status_class: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct HttpRouteLabels {
    method: String,
    route: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StateLabels {
    state: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StatusLabels {
    status: String,
}

#[derive(Clone, Copy, Debug)]
struct HttpDurationHistogram;

impl MetricConstructor<Histogram> for HttpDurationHistogram {
    fn new_metric(&self) -> Histogram {
        Histogram::new(HTTP_DURATION_BUCKETS_SECONDS)
    }
}

#[derive(Clone)]
pub(crate) struct Metrics {
    registry: Arc<Registry>,
    http_requests: Family<HttpRequestLabels, Counter>,
    http_request_duration: Family<HttpRouteLabels, Histogram, HttpDurationHistogram>,
    http_in_flight: Gauge,
    ingest_queue_depth: Gauge,
    ingest_accepting: Gauge,
    ingest_tasks: Family<StateLabels, Gauge>,
    operations: Family<StatusLabels, Gauge>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct IngestRuntimeMetrics {
    pub(crate) queue_depth: usize,
    pub(crate) accepting: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct StoreMetricsSnapshot {
    pub(crate) ingest_tasks: [usize; INGEST_STATES.len()],
    pub(crate) operations: [usize; OPERATION_STATUSES.len()],
}

pub(crate) struct HttpRequestObservation {
    metrics: Metrics,
    method: String,
    route: String,
    status_class: Option<String>,
    started_at: Instant,
}

impl Metrics {
    pub(crate) fn new() -> Self {
        let http_requests = Family::default();
        let http_request_duration = Family::new_with_constructor(HttpDurationHistogram);
        let http_in_flight = Gauge::default();
        let ingest_queue_depth = Gauge::default();
        let ingest_accepting = Gauge::default();
        let ingest_tasks = Family::default();
        let operations = Family::default();

        let build_info = Info::new(vec![
            ("version".to_string(), env!("CARGO_PKG_VERSION").to_string()),
            ("git_revision".to_string(), build_revision().to_string()),
        ]);
        let mut registry = Registry::default();
        registry.register(
            "nowledge_build",
            "Build identity; values remain constant for the process lifetime",
            build_info,
        );
        registry.register(
            "nowledge_http_requests",
            "Completed HTTP requests grouped only by bounded protocol labels",
            http_requests.clone(),
        );
        registry.register(
            "nowledge_http_request_duration_seconds",
            "HTTP request duration through response-body completion or cancellation",
            http_request_duration.clone(),
        );
        registry.register(
            "nowledge_http_in_flight",
            "HTTP requests whose response bodies have not completed or been cancelled",
            http_in_flight.clone(),
        );
        registry.register(
            "nowledge_ingest_queue_depth",
            "Ingest jobs admitted but not yet running",
            ingest_queue_depth.clone(),
        );
        registry.register(
            "nowledge_ingest_accepting",
            "Whether the ingest dispatcher currently accepts new jobs",
            ingest_accepting.clone(),
        );
        registry.register(
            "nowledge_ingest_tasks",
            "Current tenant ingest task records grouped by bounded state",
            ingest_tasks.clone(),
        );
        registry.register(
            "nowledge_operations",
            "Current tenant durable mutation operations grouped by bounded status",
            operations.clone(),
        );

        Self {
            registry: Arc::new(registry),
            http_requests,
            http_request_duration,
            http_in_flight,
            ingest_queue_depth,
            ingest_accepting,
            ingest_tasks,
            operations,
        }
    }

    pub(crate) fn begin_http_request(&self, method: &str, route: &str) -> HttpRequestObservation {
        self.http_in_flight.inc();
        HttpRequestObservation {
            metrics: self.clone(),
            method: bounded_method(method).to_string(),
            route: route.to_string(),
            status_class: None,
            started_at: Instant::now(),
        }
    }

    pub(crate) fn render(
        &self,
        runtime: IngestRuntimeMetrics,
        store: &StoreMetricsSnapshot,
    ) -> Result<String, ApiError> {
        self.ingest_queue_depth
            .set(metric_value(runtime.queue_depth));
        self.ingest_accepting.set(i64::from(runtime.accepting));
        for (index, state) in INGEST_STATES.iter().enumerate() {
            self.ingest_tasks
                .get_or_create(&StateLabels {
                    state: (*state).to_string(),
                })
                .set(metric_value(store.ingest_tasks[index]));
        }
        for (index, status) in OPERATION_STATUSES.iter().enumerate() {
            self.operations
                .get_or_create(&StatusLabels {
                    status: (*status).to_string(),
                })
                .set(metric_value(store.operations[index]));
        }

        let mut body = String::new();
        encode(&mut body, &self.registry)
            .map_err(|_| ApiError::Internal("failed to encode operational metrics".to_string()))?;
        Ok(body)
    }
}

impl HttpRequestObservation {
    pub(crate) fn complete(mut self, status: u16) -> Self {
        self.status_class = Some(status_class(status).to_string());
        self
    }
}

impl Drop for HttpRequestObservation {
    fn drop(&mut self) {
        self.metrics.http_in_flight.dec();
        let Some(status_class) = self.status_class.take() else {
            return;
        };
        self.metrics
            .http_requests
            .get_or_create(&HttpRequestLabels {
                method: self.method.clone(),
                route: self.route.clone(),
                status_class,
            })
            .inc();
        self.metrics
            .http_request_duration
            .get_or_create(&HttpRouteLabels {
                method: self.method.clone(),
                route: self.route.clone(),
            })
            .observe(self.started_at.elapsed().as_secs_f64());
    }
}

fn bounded_method(method: &str) -> &'static str {
    match method {
        "GET" => "GET",
        "POST" => "POST",
        "PUT" => "PUT",
        "PATCH" => "PATCH",
        "DELETE" => "DELETE",
        "HEAD" => "HEAD",
        "OPTIONS" => "OPTIONS",
        "CONNECT" => "CONNECT",
        "TRACE" => "TRACE",
        _ => "OTHER",
    }
}

fn status_class(status: u16) -> &'static str {
    match status {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    }
}

fn metric_value(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn build_revision() -> &'static str {
    option_env!("NOWLEDGE_GIT_REVISION")
        .filter(|revision| {
            (7..=64).contains(&revision.len())
                && revision.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
        .unwrap_or("unknown")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attacker_controlled_methods_and_statuses_are_bounded() {
        assert_eq!(bounded_method("GET"), "GET");
        assert_eq!(bounded_method("X-CUSTOM-METHOD"), "OTHER");
        assert_eq!(status_class(200), "2xx");
        assert_eq!(status_class(799), "other");
        assert!(
            build_revision() == "unknown"
                || build_revision().bytes().all(|b| b.is_ascii_hexdigit())
        );
    }

    #[test]
    fn rendering_emits_fixed_runtime_series_and_openmetrics_eof() {
        let metrics = Metrics::new();
        let body = metrics
            .render(
                IngestRuntimeMetrics {
                    queue_depth: 3,
                    accepting: true,
                },
                &StoreMetricsSnapshot::default(),
            )
            .unwrap();
        assert!(body.contains("nowledge_build_info"));
        assert!(body.contains("nowledge_ingest_queue_depth 3"));
        assert!(body.contains("nowledge_ingest_accepting 1"));
        assert!(body.ends_with("# EOF\n"));
    }
}
