import {
	context,
	createTraceState,
	trace,
} from "@opentelemetry/api";
import type { RuntimeActorInvocationTraceContext } from "./runtime";

export function runWithActorInvocationTrace<T>(
	invocation: RuntimeActorInvocationTraceContext | undefined,
	run: () => T,
): T {
	if (!invocation?.span) {
		return run();
	}

	let parent;
	try {
		parent = trace.setSpanContext(context.active(), {
			traceId: invocation.span.traceId,
			spanId: invocation.span.spanId,
			traceFlags: invocation.span.traceFlags,
			traceState: invocation.span.tracestate
				? createTraceState(invocation.span.tracestate)
				: undefined,
			isRemote: false,
		});
	} catch {
		// Telemetry context must never prevent an actor action from running.
		return run();
	}
	return context.with(parent, run);
}
