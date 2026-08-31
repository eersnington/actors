use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};

static PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();
static PROVIDER_INIT: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

pub(crate) fn initialize_if_configured() -> Result<Option<SdkTracer>, opentelemetry::trace::TraceError>
{
	if !export_is_configured() {
		return Ok(None);
	}
	if let Some(provider) = PROVIDER.get() {
		return Ok(Some(provider.tracer("rivetkit")));
	}
	let _init = PROVIDER_INIT.lock();
	if let Some(provider) = PROVIDER.get() {
		return Ok(Some(provider.tracer("rivetkit")));
	}

	let exporter = SpanExporter::builder().with_http().build()?;
	let resource = Resource::builder()
		.with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
		.build();
	// The SDK reads standard OTEL_TRACES_SAMPLER configuration here.
	let provider = SdkTracerProvider::builder()
		.with_resource(resource)
		.with_batch_exporter(exporter)
		.build();
	let tracer = provider.tracer("rivetkit");
	debug_assert!(PROVIDER.set(provider).is_ok());
	Ok(Some(tracer))
}

fn export_is_configured() -> bool {
	if std::env::var("OTEL_SDK_DISABLED")
		.is_ok_and(|value| value.eq_ignore_ascii_case("true"))
		|| std::env::var("OTEL_TRACES_EXPORTER")
			.is_ok_and(|value| value.eq_ignore_ascii_case("none"))
	{
		return false;
	}

	["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT"]
		.into_iter()
		.any(|name| std::env::var_os(name).is_some_and(|value| !value.is_empty()))
}

pub(crate) async fn flush_best_effort(timeout: Duration) {
	let Some(provider) = PROVIDER.get().cloned() else {
		return;
	};
	let flush = tokio::task::spawn_blocking(move || provider.force_flush());
	match tokio::time::timeout(timeout, flush).await {
		Ok(Ok(Ok(()))) => {}
		Ok(Ok(Err(error))) => tracing::warn!(?error, "failed to flush OpenTelemetry traces"),
		Ok(Err(error)) => tracing::warn!(?error, "OpenTelemetry flush task failed"),
		Err(_) => tracing::warn!("OpenTelemetry trace flush timed out"),
	}
}
