import {
	context,
	createTraceState,
	trace,
	TraceFlags,
} from "@opentelemetry/api";
import type { RuntimeActorInvocationTraceContext } from "./runtime";

export function runWithActorInvocationTrace<T>(
	invocation: RuntimeActorInvocationTraceContext | undefined,
	run: () => T,
): T {
	if (!invocation) return run();

	let parent;
	try {
		const flags = Number.parseInt(invocation.traceparent.slice(-2), 16);
		if (!Number.isInteger(flags)) return run();

		parent = trace.setSpanContext(context.active(), {
			traceId: invocation.traceId,
			spanId: invocation.spanId,
			traceFlags:
				flags & TraceFlags.SAMPLED ? TraceFlags.SAMPLED : TraceFlags.NONE,
			traceState: invocation.tracestate
				? createTraceState(invocation.tracestate)
				: undefined,
			isRemote: true,
		});
	} catch {
		// Telemetry context must never prevent an actor action from running.
		return run();
	}
	return context.with(parent, run);
}
