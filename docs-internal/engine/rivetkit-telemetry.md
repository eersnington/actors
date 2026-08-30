# RivetKit telemetry architecture

RivetKit records actor-runtime work automatically when OpenTelemetry export is configured. `rivetkit-core` defines the span and metric policy. The native TypeScript runtime moves trace context across Node-API (N-API) and initializes the process-wide exporter.

RivetKit instruments work that it owns. Application instrumentation remains under the application's OpenTelemetry provider.

## Direct actor actions

```text
Rivet Engine gateway
  └── stamps trusted x-rivet-ray-id
      └── rivetkit-core starts <actor>.<action>
          ├── rivet.kv.*
          ├── rivet.sqlite.*
          ├── actor-to-actor calls
          ├── correlated Pino logs
          └── JavaScript OpenTelemetry context for application instrumentation
```

The Rivet Engine gateway stamps `x-rivet-ray-id`. RivetKit actor-to-actor calls use `x-rivetkit-ray-id`. Actor ingress accepts either header and creates a ray when neither contains a valid ID.

An action span is named `<actor>.<action>` and carries:

- `rivet.ray.id`
- `rivet.actor.id`
- `rivet.actor.name`
- `rivet.actor.key`
- `rivet.action.name`
- `rivet.invocation.type`

Incoming W3C `traceparent` and `tracestate` become the action span's remote parent. Invalid context is ignored. Errors set the span status and record the bounded Rivet error identity as `error.type`, for example `actor.action_not_found`. Span data is limited to runtime metadata, excluding arguments, results, state, and raw error messages.

## Runtime operation spans

KV and SQLite operations are children of the action that performed them:

```text
counter.increment
  ├── rivet.kv.get
  ├── rivet.kv.put
  └── rivet.sqlite.execute
```

Operation spans contain the operation name and status. Their runtime metadata excludes KV keys and values, SQL text, bindings, and returned rows.

Each invocation receives `ActorKv` and `SqliteDb` handle clones with its telemetry context. After an action replies, retained handles cannot attach new work to the completed action. A storage operation already in flight may keep the action span open until that operation finishes.

## Immediate actor-to-actor calls

```text
Actor A action
  └── traceparent + tracestate + ray ID
      └── Actor B action
```

RivetKit adds propagation fields to each outbound call. Resolving the context per call keeps Actor B in the same trace and prevents concurrent calls from sharing context through a reusable client handle.

Pino remains the application logger. Logs created inside an actor invocation include the active `trace_id`, `span_id`, and `ray_id`, so logs and traces can be queried using the same IDs.

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

Connection callbacks also carry `rivet.connection.id`. Default WebSocket telemetry stops at lifecycle and connection callbacks, which bounds span volume and avoids message payloads.

Connect and disconnect are separate traces joined by the stable connection ID and actor fields. The persisted hibernatable connection format remains unchanged.

## JavaScript application instrumentation

Rust owns the actor span, while normal JavaScript libraries create application spans. The boundary is explicit:

```text
Rust action span
  └── N-API returns its neutral trace context
      └── TypeScript enters that context for the action callback
          └── the application's standard instrumentation creates children
```

For example, application-configured Undici instrumentation sees an ordinary `fetch()` inside an action and creates its HTTP span beneath the Rust action span. The application owns its JavaScript provider and HTTP instrumentation. Without that instrumentation, `fetch()` remains unchanged while the `rivetkit-core` spans still export.

The context is entered only while the action callback runs. Concurrent actions therefore cannot inherit each other's trace context.

## Export configuration and ownership

RivetKit uses the Rust OpenTelemetry SDK's normal environment configuration:

- No OpenTelemetry Protocol (OTLP) endpoint, `OTEL_SDK_DISABLED=true`, or `OTEL_TRACES_EXPORTER=none`: no exporter
- Standard `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG` values configure recording
- Missing sampler configuration uses the SDK's parent-based always-on default
- Invalid sampler configuration emits one warning and uses that default

One batched Rust provider is shared by all RivetKit registries in a process. Provider configuration is process-wide and fixed at startup. Registry shutdown flushes queued telemetry with a timeout. Export failures remain diagnostic and never fail actor work.

## Invocation metrics

RivetKit records completed action and scheduled invocations:

```text
rivetkit_actor_invocations_total
rivetkit_actor_invocation_duration_seconds
```

Both use the bounded labels `actor_name`, `action_name`, `invocation_type`, and `status`. Action names are matched against the actor's configured actions; an unknown client-provided name becomes `unknown` instead of creating an unbounded metric label.

Metric labels are limited to configured names and bounded statuses. Actor IDs, actor keys, ray IDs, trace IDs, connection IDs, and raw errors remain trace data. Existing connection and active-task metrics remain the source of truth for those runtime states.

## Observable verification

The integration coverage proves behavior at real boundaries:

- an actor action exports through OTLP and still returns its result;
- overlapping actions keep storage and JavaScript child spans isolated;
- an immediate actor-to-actor call preserves parentage and ray identity;
- a durable schedule starts a new trace with the same ray and a link to its creator;
- real WebSocket traffic exports bounded lifecycle and connection spans;
- `OTEL_TRACES_SAMPLER=always_off` exports no spans while actor work succeeds;
- the production Prometheus scrape contains successful, failed, and scheduled invocation metrics.
