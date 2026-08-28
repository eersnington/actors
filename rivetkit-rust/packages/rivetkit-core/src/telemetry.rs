//! Actor invocation telemetry for native RivetKit runtimes.
//!
//! Core emits actor spans on the `rivetkit::telemetry` target. Normal logging
//! layers must disable this target, while an OpenTelemetry layer enables it only
//! when export is configured. Only the NAPI runtime installs that layer today;
//! the Rust `rivetkit` crate does not, which is a known parity gap.

use std::{future::Future, time::Duration};

use opentelemetry::KeyValue;
use opentelemetry::trace::{TraceContextExt as _, TracerProvider as _};
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use parking_lot::Mutex;
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

/// Trace context attached to one actor invocation and carried across foreign-runtime calls.
#[derive(Clone, Debug)]
pub struct ActorOperationTelemetry {
	parent: opentelemetry::Context,
	ray_id: String,
	actor_id: String,
	actor_name: String,
	actor_key: String,
}

impl ActorOperationTelemetry {
	pub(crate) fn new(
		parent: tracing::Span,
		ray_id: String,
		actor_id: String,
		actor_name: String,
		actor_key: String,
	) -> Self {
		let parent_context = parent.context();
		let parent_span_context = parent_context.span().span_context().clone();
		Self {
			parent: opentelemetry::Context::new()
				.with_remote_span_context(parent_span_context),
			ray_id,
			actor_id,
			actor_name,
			actor_key,
		}
	}

	/// Records one safe, automatic runtime operation as a child of the invocation.
	pub async fn trace<T, E>(
		&self,
		system: &'static str,
		operation: &'static str,
		future: impl Future<Output = Result<T, E>>,
	) -> Result<T, E> {
		let span_name = format!("rivet.{system}.{operation}");
		let span = tracing::info_span!(
			target: "rivetkit::telemetry",
			parent: None,
			"rivet.runtime.operation",
			otel.name = %span_name,
			otel.kind = "internal",
			rivet.operation.system = system,
			rivet.operation.name = operation,
			rivet.actor.id = %self.actor_id,
			rivet.actor.name = %self.actor_name,
			rivet.actor.key = %self.actor_key,
			rivet.ray.id = %self.ray_id,
			otel.status_code = tracing::field::Empty,
		);
		span.set_parent(self.parent.clone());
		let result = future.instrument(span.clone()).await;
		span.record(
			"otel.status_code",
			if result.is_ok() { "OK" } else { "ERROR" },
		);
		result
	}
}

static PROVIDER: Mutex<Option<SdkTracerProvider>> = Mutex::new(None);

/// Initializes the process-wide actor trace provider from standard OpenTelemetry configuration.
pub fn initialize_if_configured() -> Result<Option<SdkTracer>, opentelemetry::trace::TraceError> {
	if !export_is_configured(|name| std::env::var_os(name)) {
		return Ok(None);
	}

	let mut provider = PROVIDER.lock();
	if let Some(provider) = provider.as_ref() {
		return Ok(Some(provider.tracer("rivetkit")));
	}

	let exporter = SpanExporter::builder().with_http().build()?;
	let resource = Resource::builder()
		.with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
		.build();
	let initialized = SdkTracerProvider::builder()
		.with_resource(resource)
		.with_batch_exporter(exporter)
		.build();
	let tracer = initialized.tracer("rivetkit");
	*provider = Some(initialized);

	Ok(Some(tracer))
}

fn export_is_configured(get: impl Fn(&str) -> Option<std::ffi::OsString>) -> bool {
	if get("OTEL_SDK_DISABLED")
		.and_then(|value| value.into_string().ok())
		.is_some_and(|value| value.eq_ignore_ascii_case("true"))
	{
		return false;
	}
	["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT"]
		.into_iter()
		.any(|name| get(name).is_some_and(|value| !value.is_empty()))
}

/// Flushes the shared process provider without shutting it down for other registries.
pub async fn flush_best_effort(timeout: Duration) {
	let provider = PROVIDER.lock().clone();
	let Some(provider) = provider else {
		return;
	};
	flush_provider(provider, timeout).await;
}

async fn flush_provider(provider: SdkTracerProvider, timeout: Duration) {
	let flush = tokio::task::spawn_blocking(move || provider.force_flush());
	match tokio::time::timeout(timeout, flush).await {
		Ok(Ok(Ok(()))) => {}
		Ok(Ok(Err(error))) => {
			tracing::warn!(?error, "failed to flush OpenTelemetry trace provider");
		}
		Ok(Err(error)) => {
			tracing::warn!(?error, "OpenTelemetry trace provider flush task failed");
		}
		Err(_) => {
			tracing::warn!("OpenTelemetry trace provider flush timed out");
		}
	}
}
