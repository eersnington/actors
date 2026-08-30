//! Automatic telemetry context for one actor invocation.

use std::future::Future;
use std::str::FromStr as _;
use std::sync::Arc;

use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt as _, TraceFlags, TraceId, TraceState};
use parking_lot::Mutex;
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

/// Propagation fields for work started by the active actor invocation.
#[derive(Clone, Debug)]
pub struct ActorInvocationTraceContext {
	pub ray_id: String,
	pub trace_id: String,
	pub span_id: String,
	pub traceparent: String,
	pub tracestate: Option<String>,
}

/// Trace correlation persisted with work that may run after its creator exits.
#[derive(Clone, Debug)]
pub struct DurableTraceContext {
	pub ray_id: String,
	pub traceparent: String,
	pub tracestate: Option<String>,
}

impl From<ActorInvocationTraceContext> for DurableTraceContext {
	fn from(value: ActorInvocationTraceContext) -> Self {
		Self {
			ray_id: value.ray_id,
			traceparent: value.traceparent,
			tracestate: value.tracestate,
		}
	}
}

/// Telemetry shared by the automatic spans created during one actor invocation.
///
/// The context crosses foreign-runtime boundaries explicitly. It does not rely
/// on task-local state surviving an N-API callback.
#[derive(Clone, Debug)]
pub struct ActorInvocationTelemetry {
	span: Arc<Mutex<Option<tracing::Span>>>,
	ray_id: String,
	actor_id: String,
	actor_name: String,
	actor_key: String,
}

impl ActorInvocationTelemetry {
	pub(crate) fn start(
		actor_id: String,
		actor_name: String,
		actor_key: String,
		action_name: &str,
		invocation_type: &'static str,
		ray_id: Option<String>,
		traceparent: Option<&str>,
		tracestate: Option<&str>,
	) -> Option<Self> {
		Self::start_with_link(
			actor_id,
			actor_name,
			actor_key,
			action_name,
			invocation_type,
			ray_id,
			traceparent,
			tracestate,
			None,
		)
	}

	pub(crate) fn start_with_link(
		actor_id: String,
		actor_name: String,
		actor_key: String,
		action_name: &str,
		invocation_type: &'static str,
		ray_id: Option<String>,
		traceparent: Option<&str>,
		tracestate: Option<&str>,
		link: Option<&DurableTraceContext>,
	) -> Option<Self> {
		if !tracing::enabled!(target: "rivetkit::telemetry", tracing::Level::INFO) {
			return None;
		}
		let ray_id = ray_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
		let span_name = format!("{actor_name}.{action_name}");
		let span = tracing::info_span!(
			target: "rivetkit::telemetry",
			parent: None,
			"rivet.actor.invoke",
			otel.name = %span_name,
			otel.kind = "server",
			rivet.invocation.type = invocation_type,
			rivet.actor.id = %actor_id,
			rivet.actor.name = %actor_name,
			rivet.actor.key = %actor_key,
			rivet.action.name = %action_name,
			rivet.ray.id = %ray_id,
			otel.status_code = tracing::field::Empty,
			error.type = tracing::field::Empty,
		);
		set_remote_parent(&span, traceparent, tracestate);
		if let Some(link) = link.and_then(parse_durable_link) {
			span.add_link(link);
		}
		Some(Self {
			span: Arc::new(Mutex::new(Some(span))),
			ray_id,
			actor_id,
			actor_name,
			actor_key,
		})
	}

	pub(crate) fn finish(&self, status: &'static str, error_type: Option<String>) {
		let Some(span) = self.span.lock().take() else {
			return;
		};
		span.record("otel.status_code", status);
		if let Some(error_type) = error_type.as_deref() {
			span.record("error.type", error_type);
		}
	}

	/// Returns the active invocation context for outbound calls and correlated logs.
	pub fn trace_context(&self) -> Option<ActorInvocationTraceContext> {
		let span = self.span.lock().as_ref()?.clone();
		let context = span.context();
		let context_span = context.span();
		let span_context = context_span.span_context();
		if !span_context.is_valid() {
			return None;
		}
		let tracestate = span_context.trace_state().header();

		Some(ActorInvocationTraceContext {
			ray_id: self.ray_id.clone(),
			trace_id: span_context.trace_id().to_string(),
			span_id: span_context.span_id().to_string(),
			traceparent: format!(
				"00-{}-{}-{:02x}",
				span_context.trace_id(),
				span_context.span_id(),
				span_context.trace_flags().to_u8(),
			),
			tracestate: (!tracestate.is_empty()).then_some(tracestate),
		})
	}

	/// Records one safe runtime operation beneath this invocation.
	pub async fn trace<T, E>(
		&self,
		system: &'static str,
		operation: &'static str,
		future: impl Future<Output = Result<T, E>>,
	) -> Result<T, E> {
		let Some(parent) = self.span.lock().as_ref().cloned() else {
			// The invocation has already replied. A retained foreign-runtime context
			// must not attach later work to a completed action.
			return future.await;
		};
		// This short-lived clone deliberately keeps the parent open until an
		// in-flight child operation finishes.
		let span_name = format!("rivet.{system}.{operation}");
		let span = tracing::info_span!(
			target: "rivetkit::telemetry",
			parent: &parent,
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
		let result = future.instrument(span.clone()).await;
		span.record(
			"otel.status_code",
			if result.is_ok() { "OK" } else { "ERROR" },
		);
		result
	}
}

fn parse_durable_link(link: &DurableTraceContext) -> Option<SpanContext> {
	parse_span_context(&link.traceparent, link.tracestate.as_deref(), true)
}

pub(crate) fn set_remote_parent(
	span: &tracing::Span,
	traceparent: Option<&str>,
	tracestate: Option<&str>,
) {
	let Some(traceparent) = traceparent else {
		return;
	};
	let Some(parent) = parse_span_context(traceparent, tracestate, true) else {
		return;
	};
	let _ = span.set_parent(opentelemetry::Context::new().with_remote_span_context(parent));
}

fn parse_span_context(
	traceparent: &str,
	tracestate: Option<&str>,
	is_remote: bool,
) -> Option<SpanContext> {
	// Accept only W3C version 00 until a later version's additional fields can
	// be interpreted without guessing at parent or sampling semantics.
	let mut parts = traceparent.split('-');
	let (Some("00"), Some(trace_id), Some(span_id), Some(flags), None) = (
		parts.next(),
		parts.next(),
		parts.next(),
		parts.next(),
		parts.next(),
	) else {
		return None;
	};
	let (Ok(trace_id), Ok(span_id), Ok(flags)) = (
		TraceId::from_hex(trace_id),
		SpanId::from_hex(span_id),
		u8::from_str_radix(flags, 16),
	) else {
		return None;
	};
	if trace_id == TraceId::INVALID || span_id == SpanId::INVALID {
		return None;
	}
	let trace_state = tracestate
		.and_then(|value| TraceState::from_str(value).ok())
		.unwrap_or_default();
	Some(SpanContext::new(
		trace_id,
		span_id,
		TraceFlags::new(flags),
		is_remote,
		trace_state,
	))
}
