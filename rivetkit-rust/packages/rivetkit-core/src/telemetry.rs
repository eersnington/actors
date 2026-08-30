//! Automatic telemetry context for one actor invocation.

use std::future::Future;
use std::sync::Arc;

use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator as _};
use opentelemetry::trace::{SpanContext, TraceContextExt as _};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use parking_lot::Mutex;
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::{ActorContext, Request, format_actor_key};

/// Propagation fields for work started by the active actor invocation.
#[derive(Clone, Debug)]
pub struct ActorInvocationTraceContext {
	pub ray_id: String,
	pub span: Option<ActorInvocationSpanContext>,
}

#[derive(Clone, Debug)]
pub struct ActorInvocationSpanContext {
	pub trace_id: String,
	pub span_id: String,
	pub trace_flags: u8,
	pub traceparent: String,
	pub tracestate: Option<String>,
}

/// Trace correlation persisted with work that may run after its creator exits.
#[derive(Clone, Debug)]
pub struct DurableTraceContext {
	pub ray_id: String,
	pub traceparent: Option<String>,
	pub tracestate: Option<String>,
}

/// Telemetry for one bounded lifecycle or connection callback.
#[derive(Debug)]
pub struct ActorCallbackTelemetry {
	span: Option<tracing::Span>,
	ray_id: String,
}

impl ActorCallbackTelemetry {
	pub fn start(
		ctx: &ActorContext,
		system: &'static str,
		operation: &'static str,
		parent: Option<&Self>,
		request: Option<&Request>,
	) -> Option<Self> {
		Self::start_with_connection(ctx, system, operation, parent, request, None)
	}

	pub fn start_connection(
		ctx: &ActorContext,
		system: &'static str,
		operation: &'static str,
		request: Option<&Request>,
		connection_id: &str,
	) -> Option<Self> {
		Self::start_with_connection(
			ctx,
			system,
			operation,
			None,
			request,
			Some(connection_id),
		)
	}

	fn start_with_connection(
		ctx: &ActorContext,
		system: &'static str,
		operation: &'static str,
		parent: Option<&Self>,
		request: Option<&Request>,
		connection_id: Option<&str>,
	) -> Option<Self> {
		if !tracing::enabled!(target: "rivetkit::telemetry", tracing::Level::INFO) {
			return None;
		}

		let request_ray_id = request.and_then(|request| invocation_ray_id(request.headers()));
		let ray_id = parent
			.map(|parent| parent.ray_id.clone())
			.or(request_ray_id)
			.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
		let span = match parent.and_then(|parent| parent.span.as_ref()) {
			Some(parent_span) => tracing::info_span!(
				target: "rivetkit::telemetry",
				parent: parent_span,
				"rivet.actor.callback",
				otel.name = %format!("rivet.actor.{system}.{operation}"),
				otel.kind = "internal",
				rivet.callback.name = operation,
				rivet.actor.id = %ctx.actor_id(),
				rivet.actor.name = %ctx.name(),
				rivet.actor.key = %format_actor_key(ctx.key()),
				rivet.ray.id = %ray_id,
				rivet.connection.id = tracing::field::Empty,
				otel.status_code = tracing::field::Empty,
				error.type = tracing::field::Empty,
			),
			None => tracing::info_span!(
				target: "rivetkit::telemetry",
				parent: None,
				"rivet.actor.callback",
				otel.name = %format!("rivet.actor.{system}.{operation}"),
				otel.kind = "internal",
				rivet.callback.name = operation,
				rivet.actor.id = %ctx.actor_id(),
				rivet.actor.name = %ctx.name(),
				rivet.actor.key = %format_actor_key(ctx.key()),
				rivet.ray.id = %ray_id,
				rivet.connection.id = tracing::field::Empty,
				otel.status_code = tracing::field::Empty,
				error.type = tracing::field::Empty,
			),
		};
		if let Some(connection_id) = connection_id {
			span.record("rivet.connection.id", connection_id);
		}

		if parent.is_none() {
			let traceparent = request.and_then(|request| header(request, "traceparent"));
			let tracestate = request.and_then(|request| header(request, "tracestate"));
			set_remote_parent(&span, traceparent, tracestate);
		}

		Some(Self {
			span: Some(span),
			ray_id,
		})
	}

	pub async fn trace<T>(
		ctx: &ActorContext,
		system: &'static str,
		operation: &'static str,
		parent: Option<&Self>,
		request: Option<&Request>,
		future: impl Future<Output = anyhow::Result<T>>,
	) -> anyhow::Result<T> {
		Self::trace_started(
			Self::start(ctx, system, operation, parent, request),
			future,
		)
		.await
	}

	pub async fn trace_started<T>(
		telemetry: Option<Self>,
		future: impl Future<Output = anyhow::Result<T>>,
	) -> anyhow::Result<T> {
		let Some(mut telemetry) = telemetry else {
			return future.await;
		};
		let span = telemetry.span.as_ref().cloned().expect("callback span");
		let result = future.instrument(span).await;
		telemetry.finish(&result);
		result
	}

	pub fn finish<T>(&mut self, result: &anyhow::Result<T>) {
		let Some(span) = self.span.take() else {
			return;
		};
		span.record(
			"otel.status_code",
			if result.is_ok() { "OK" } else { "ERROR" },
		);
		if let Err(error) = result {
			let structured = rivet_error::RivetError::extract(error);
			span.record(
				"error.type",
				format!("{}.{}", structured.group(), structured.code()),
			);
		}
	}
}

fn header<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
	request.headers().get(name)?.to_str().ok()
}

pub(crate) fn invocation_ray_id(headers: &http::HeaderMap) -> Option<String> {
	["x-rivetkit-ray-id", "x-rivet-ray-id"]
		.into_iter()
		.filter_map(|name| headers.get(name)?.to_str().ok())
		.find(|value| {
			!value.is_empty()
				&& value.len() <= 128
				&& value
					.bytes()
					.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
		})
		.map(str::to_owned)
}

impl From<ActorInvocationTraceContext> for DurableTraceContext {
	fn from(value: ActorInvocationTraceContext) -> Self {
		match value.span {
			Some(span) => Self {
				ray_id: value.ray_id,
				traceparent: Some(span.traceparent),
				tracestate: span.tracestate,
			},
			None => Self {
				ray_id: value.ray_id,
				traceparent: None,
				tracestate: None,
			},
		}
	}
}

/// Telemetry shared by the automatic spans created during one actor invocation.
///
/// The context crosses foreign-runtime boundaries explicitly. It does not rely
/// on task-local state surviving an N-API callback.
#[derive(Clone, Debug)]
pub struct ActorInvocationTelemetry {
	span: Arc<Mutex<InvocationSpan>>,
	ray_id: String,
	actor_id: String,
	actor_name: String,
	actor_key: String,
}

#[derive(Debug)]
enum InvocationSpan {
	Disabled,
	Active(tracing::Span),
	Finished,
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
	) -> Self {
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
	) -> Self {
		let ray_id = ray_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
		if !tracing::enabled!(target: "rivetkit::telemetry", tracing::Level::INFO) {
			return Self {
				span: Arc::new(Mutex::new(InvocationSpan::Disabled)),
				ray_id,
				actor_id,
				actor_name,
				actor_key,
			};
		}
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
		Self {
			span: Arc::new(Mutex::new(InvocationSpan::Active(span))),
			ray_id,
			actor_id,
			actor_name,
			actor_key,
		}
	}

	pub(crate) fn finish(&self, status: &'static str, error_type: Option<String>) {
		let span = match std::mem::replace(&mut *self.span.lock(), InvocationSpan::Finished) {
			InvocationSpan::Active(span) => span,
			InvocationSpan::Disabled | InvocationSpan::Finished => return,
		};
		span.record("otel.status_code", status);
		if let Some(error_type) = error_type.as_deref() {
			span.record("error.type", error_type);
		}
	}

	/// Returns the active invocation context for outbound calls and correlated logs.
	pub fn trace_context(&self) -> Option<ActorInvocationTraceContext> {
		let span = match &*self.span.lock() {
			InvocationSpan::Disabled => {
				return Some(ActorInvocationTraceContext {
					ray_id: self.ray_id.clone(),
					span: None,
				});
			}
			InvocationSpan::Active(span) => span.clone(),
			InvocationSpan::Finished => return None,
		};
		let context = span.context();
		let context_span = context.span();
		let span_context = context_span.span_context();
		if !span_context.is_valid() {
			return Some(ActorInvocationTraceContext {
				ray_id: self.ray_id.clone(),
				span: None,
			});
		}
		let tracestate = span_context.trace_state().header();
		let mut carrier = TraceHeaders::default();
		TraceContextPropagator::new().inject_context(&context, &mut carrier);

		Some(ActorInvocationTraceContext {
			ray_id: self.ray_id.clone(),
			span: carrier.traceparent.map(|traceparent| ActorInvocationSpanContext {
				trace_id: span_context.trace_id().to_string(),
				span_id: span_context.span_id().to_string(),
				trace_flags: span_context.trace_flags().to_u8(),
				traceparent,
				tracestate: carrier
					.tracestate
					.or_else(|| (!tracestate.is_empty()).then_some(tracestate)),
			}),
		})
	}

	/// Records one safe runtime operation beneath this invocation.
	pub async fn trace<T>(
		&self,
		system: &'static str,
		operation: &'static str,
		future: impl Future<Output = anyhow::Result<T>>,
	) -> anyhow::Result<T> {
		let parent = {
			let span = self.span.lock();
			match &*span {
				InvocationSpan::Active(parent) => Some(parent.clone()),
				InvocationSpan::Disabled | InvocationSpan::Finished => None,
			}
		};
		let Some(parent) = parent else {
			// Disabled telemetry and foreign-runtime work retained after the reply
			// both run without attaching to an invocation span.
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
			error.type = tracing::field::Empty,
		);
		let result = future.instrument(span.clone()).await;
		span.record(
			"otel.status_code",
			if result.is_ok() { "OK" } else { "ERROR" },
		);
		if let Err(error) = &result {
			let structured = rivet_error::RivetError::extract(error);
			span.record(
				"error.type",
				format!("{}.{}", structured.group(), structured.code()),
			);
		}
		result
	}
}

fn parse_durable_link(link: &DurableTraceContext) -> Option<SpanContext> {
	parse_span_context(link.traceparent.as_deref()?, link.tracestate.as_deref())
}

pub(crate) fn set_remote_parent(
	span: &tracing::Span,
	traceparent: Option<&str>,
	tracestate: Option<&str>,
) {
	let Some(traceparent) = traceparent else {
		return;
	};
	let Some(parent) = parse_span_context(traceparent, tracestate) else {
		return;
	};
	let _ = span.set_parent(opentelemetry::Context::new().with_remote_span_context(parent));
}

fn parse_span_context(
	traceparent: &str,
	tracestate: Option<&str>,
) -> Option<SpanContext> {
	let carrier = TraceHeaders {
		traceparent: Some(traceparent.to_owned()),
		tracestate: tracestate.map(str::to_owned),
	};
	let context = TraceContextPropagator::new().extract(&carrier);
	let span_context = context.span().span_context().clone();
	span_context.is_valid().then_some(span_context)
}

#[derive(Default)]
struct TraceHeaders {
	traceparent: Option<String>,
	tracestate: Option<String>,
}

impl Extractor for TraceHeaders {
	fn get(&self, key: &str) -> Option<&str> {
		match key {
			"traceparent" => self.traceparent.as_deref(),
			"tracestate" => self.tracestate.as_deref(),
			_ => None,
		}
	}

	fn keys(&self) -> Vec<&str> {
		["traceparent", "tracestate"]
			.into_iter()
			.filter(|key| self.get(key).is_some())
			.collect()
	}
}

impl Injector for TraceHeaders {
	fn set(&mut self, key: &str, value: String) {
		match key {
			"traceparent" => self.traceparent = Some(value),
			"tracestate" => self.tracestate = Some(value),
			_ => {}
		}
	}
}
