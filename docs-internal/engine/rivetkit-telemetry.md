# RivetKit telemetry architecture

RivetKit records runtime work automatically when OpenTelemetry export is configured. `rivetkit-core` owns the span and metric policy. NAPI and TypeScript only move trace context across the foreign-runtime boundary.

RivetKit instruments work that it owns. Application instrumentation remains under the application's OpenTelemetry provider.

## Direct actor actions

```text
Pegboard gateway
  └── stamps trusted x-rivet-ray-id
      └── rivetkit-core starts <actor>.<action>
          ├── rivet.kv.*
          ├── rivet.sqlite.*
          ├── actor-to-actor calls
          ├── correlated Pino logs
          └── JavaScript OTel context for application instrumentation
```

The gateway creates a request ray ID after it has accepted the request into the trusted engine boundary. RivetKit accepts `x-rivet-ray-id` and the local-development `x-rivetkit-ray-id` alias. A missing valid ray ID is created at actor ingress.

An action span is named `<actor>.<action>` and carries:

- `rivet.ray.id`
- `rivet.actor.id`
- `rivet.actor.name`
- `rivet.actor.key`
- `rivet.action.name`
- `rivet.invocation.type`

Incoming W3C `traceparent` and `tracestate` become the action span's remote parent. Invalid context is ignored. Errors set the span status and record the bounded Rivet error identity as `error.type`, for example `actor.action_not_found`. Arguments, results, state, and raw error messages are not recorded.

## Runtime operation spans

KV and SQLite operations are children of the action that performed them:

```text
counter.increment
  ├── rivet.kv.get
  ├── rivet.kv.put
  └── rivet.sqlite.execute
```

The operation name and status are recorded. KV keys and values, SQL text, bindings, and returned rows are not recorded.

The telemetry context belongs to the invocation-specific KV and SQLite handles. After an action replies, retained handles cannot attach new work to the completed action. A storage operation already in flight may keep the action span open until that operation finishes.

## Immediate actor-to-actor calls

```text
Actor A action
  └── traceparent + tracestate + ray ID
      └── Actor B action
```

RivetKit adds propagation fields to the individual outbound call. They are not stored on a reusable client handle. Actor B therefore remains in the same trace and ray without allowing one concurrent call to inherit another call's context.

Pino remains the application logger. Logs created inside an actor invocation include the active `trace_id`, `span_id`, and `ray_id`, so a log can be found from its trace and a trace can be found from its log. RivetKit does not convert Pino records into OpenTelemetry logs.

## Durable schedules

A schedule can fire after the action that created it has finished. Keeping the original span open would produce a misleading, hours-long trace. RivetKit persists a neutral carrier with the schedule instead:

```text
creator action       trace A, ray 123
  └── persisted traceparent + tracestate + ray ID

scheduled action     trace B, ray 123
  └── span link to the creator in trace A
```

The scheduled action is a new trace with `rivet.invocation.type=scheduled`. The shared ray finds the complete chain; the span link identifies the work that created this schedule. If no durable carrier exists, the schedule gets a fresh ray and no link.

## Lifecycle and connection callbacks

RivetKit records bounded callbacks that correspond to meaningful runtime transitions:

```text
rivet.actor.lifecycle.<operation>
rivet.actor.websocket.<operation>
rivet.actor.connection.<operation>
```

Connection callbacks also carry `rivet.connection.id`. WebSocket messages do not create spans by default. Message traffic may be extremely frequent, and payloads may be sensitive, so per-message tracing requires a separate opt-in policy.

Connect and disconnect are separate traces. The hibernatable connection record does not persist trace context. Their stable connection ID and actor fields provide correlation without changing the persisted connection format.

## JavaScript application instrumentation

Rust owns the actor span, while normal JavaScript libraries create application spans. The boundary is explicit:

```text
Rust action span
  └── NAPI returns its neutral trace context
      └── TypeScript enters that context for the action callback
          └── the application's standard instrumentation creates children
```

For example, application-configured Undici instrumentation sees an ordinary `fetch()` inside an action and creates its HTTP span beneath the Rust action span. RivetKit does not install a JavaScript provider, replace an exporter, install HTTP instrumentation, or patch global `fetch`. With no application JavaScript instrumentation, the Rust automatic spans still work and `fetch()` remains unchanged.

The context is entered only while the action callback runs. Concurrent actions therefore cannot inherit each other's trace context.

## Export configuration and ownership

RivetKit uses the Rust OpenTelemetry SDK's normal environment configuration:

- no OTLP endpoint, `OTEL_SDK_DISABLED=true`, or `OTEL_TRACES_EXPORTER=none`: no exporter;
- standard `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG` values configure recording;
- missing configuration uses the SDK's parent-based always-on default;
- invalid sampler configuration emits one warning and uses that default.

One batched Rust provider is shared by all RivetKit registries in a process. Registry shutdown flushes queued telemetry with a timeout but does not destroy that shared provider. Export failures are diagnostics only and never fail actor work. Runtime provider replacement and multiple Rust telemetry configurations in one process are not supported.

## Invocation metrics

RivetKit records completed action and scheduled invocations:

```text
rivetkit_actor_invocations_total
rivetkit_actor_invocation_duration_seconds
```

Both use the bounded labels `actor_name`, `action_name`, `invocation_type`, and `status`. Action names are matched against the actor's configured actions; an unknown client-provided name becomes `unknown` instead of creating an unbounded metric label.

Actor IDs, actor keys, ray IDs, trace IDs, connection IDs, and raw error messages are never metric labels. Existing connection and active-task metrics remain the source of truth for those runtime states.

## Current boundary

This foundation deliberately does not include:

- Workflows or agentOS integration;
- per-WebSocket-message spans;
- global outbound HTTP wrapping;
- Pino-to-OpenTelemetry log export;
- persisted links between WebSocket connect and disconnect;
- exporter dropped-span metrics;
- runtime provider reconfiguration;
- fastrace;
- public configuration documentation.

These exclusions keep the core automatic path small and avoid ambient state, global patches, sensitive payload capture, and overlapping metrics.

## Observable verification

The integration coverage proves behavior at real boundaries:

- an actor action exports through OTLP and still returns its result;
- overlapping actions keep storage and JavaScript child spans isolated;
- an immediate actor-to-actor call preserves parentage and ray identity;
- a durable schedule starts a new trace with the same ray and a link to its creator;
- real WebSocket traffic exports bounded lifecycle and connection spans;
- `OTEL_TRACES_SAMPLER=always_off` exports no spans while actor work succeeds;
- the production Prometheus scrape contains successful, failed, and scheduled invocation metrics.
