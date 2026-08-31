import { existsSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { getEnginePath } from "@rivetkit/engine-cli";
import { z } from "zod/v4";
import { db } from "../../src/db/mod";
import { actor, event, queue, setup, UserError } from "../../src/mod";
import { buildNativeRegistry } from "../../src/registry/native";

if (process.env.RIVETKIT_TEST_JS_OTEL === "1") {
	const [{ registerInstrumentations }, { UndiciInstrumentation }, { NodeTracerProvider }] =
		await Promise.all([
			import("@opentelemetry/instrumentation"),
			import("@opentelemetry/instrumentation-undici"),
			import("@opentelemetry/sdk-trace-node"),
		]);
	new NodeTracerProvider().register();
	registerInstrumentations({
		instrumentations: [
			new UndiciInstrumentation({
				ignoreRequestHook: (request) => request.path !== "/trace-context",
			}),
		],
	});
}

const textDecoder = new TextDecoder();
let readPrometheusMetrics: (() => Promise<string>) | undefined;
let databaseOverlapCount = 0;
const databaseOverlap = Promise.withResolvers<void>();

async function waitForDatabaseOverlap(): Promise<void> {
	databaseOverlapCount += 1;
	if (databaseOverlapCount === 2) databaseOverlap.resolve();
	await databaseOverlap.promise;
}
const fixtureDir = dirname(fileURLToPath(import.meta.url));
const repoEngineBinary = resolve(
	fixtureDir,
	"../../../../../target/debug/rivet-engine",
);

const endpoint = process.env.RIVETKIT_TEST_ENDPOINT ?? "http://127.0.0.1:6642";
const connParamsSchema = z.object({
	userId: z.string().min(1),
});
const validatedActionArgsSchema = z.tuple([
	z.object({
		amount: z.number().int().nonnegative(),
	}),
]);
const countChangedSchema = z.object({
	count: z.number().int(),
});
const jobSchema = z.object({
	id: z.string().min(1),
});

const correlationTarget = actor({
	actions: {
		receive: async (c, token: string) => {
			c.log.info({
				msg: "received correlated actor call",
				correlation_role: "target",
				correlation_token: token,
			});
			return token;
		},
	},
});

const correlationSource = actor({
	db: db(),
	state: {
		scheduledToken: "",
	},
	createVars: (c) => ({ database: c.db, kv: c.kv }),
	onWake: async () => {},
	onConnect: async () => {},
	onDisconnect: async () => {},
	onWebSocket: (c, websocket) => {
		websocket.send(
			JSON.stringify({
				actorId: c.actorId,
				connectionId: c.conn.id,
			}),
		);
	},
	actions: {
		databaseOverlapA: async (c, token: string) => {
			await waitForDatabaseOverlap();
			await c.vars.database.execute("SELECT 1");
			return token;
		},
		databaseOverlapB: async (c, token: string) => {
			await waitForDatabaseOverlap();
			await c.vars.kv.get(token);
			return token;
		},
		failInvocation: () => {
			throw new UserError("expected invocation failure", {
				code: "expected_failure",
			});
		},
		fetchTraceparent: async (c, token: string) => {
			c.log.info({
				msg: "making instrumented outbound request",
				correlation_role: "fetch",
				correlation_token: token,
			});
			const endpoint = process.env.RIVETKIT_TEST_TRACE_ECHO_URL;
			if (!endpoint) throw new Error("missing trace echo URL");
			return await (await fetch(endpoint)).text();
		},
		scheduleOnce: async (c, token: string) => {
			await c.schedule.after(100, "completeScheduled", token);
			return token;
		},
		completeScheduled: (c, token: string) => {
			c.log.info({
				msg: "completed correlated schedule",
				correlation_role: "scheduled",
				correlation_token: token,
			});
			c.state.scheduledToken = token;
			return token;
		},
		getScheduledToken: (c) => c.state.scheduledToken,
		getPrometheusMetrics: async () => {
			if (!readPrometheusMetrics) throw new Error("metrics are not ready");
			return await readPrometheusMetrics();
		},
		relay: async (c, token: string) => {
			c.log.info({
				msg: "sending correlated actor call",
				correlation_role: "source",
				correlation_token: token,
			});
			return await c
				.client<typeof registry>()
				.correlationTarget.getOrCreate([token])
				.receive(token);
		},
		correlateWithoutTracing: async (c, token: string) => {
			c.log.info({
				msg: "starting correlation without trace export",
				correlation_role: "source",
				correlation_token: token,
			});
			await c
				.client<typeof registry>()
				.correlationTarget.getOrCreate([token])
				.receive(token);
			await c.schedule.after(100, "completeScheduled", token);
			return token;
		},
	},
});

function resolveEngineBinaryPath(): string {
	if (existsSync(repoEngineBinary)) {
		return repoEngineBinary;
	}

	return getEnginePath();
}

const integrationActor = actor({
	state: { count: 0 },
	db: db(),
	connParamsSchema,
	actionInputSchemas: {
		validatedAction: validatedActionArgsSchema,
		emitValidatedEvent: z.tuple([countChangedSchema]),
		enqueueValidatedJob: z.tuple([jobSchema]),
	},
	events: {
		countChanged: event({ schema: countChangedSchema }),
	},
	queues: {
		jobs: queue({ message: jobSchema }),
	},
	onBeforeConnect: async () => {},
	actions: {
		ping: async (c) => {
			return c.conn.params.userId;
		},
		getCount: async (c) => {
			return c.state.count;
		},
		validatedAction: async (_c, payload: { amount: number }) => {
			return payload.amount;
		},
		emitValidatedEvent: async (c, payload: { count: number }) => {
			c.broadcast("countChanged", payload);
			return payload.count;
		},
		enqueueValidatedJob: async (c, payload: { id: string }) => {
			await c.queue.send("jobs", payload);
			return payload.id;
		},
		increment: async (c, amount: number) => {
			c.state.count += amount;

			await c.kv.put("count", String(c.state.count));
			await c.db.execute(
				"CREATE TABLE IF NOT EXISTS increments (value INTEGER NOT NULL)",
			);
			await c.db.execute("INSERT INTO increments (value) VALUES (?)", [
				c.state.count,
			]);

			const rows = await c.db.execute<{ value: number }>(
				"SELECT value FROM increments ORDER BY rowid ASC",
			);
			return {
				count: c.state.count,
				sqliteValues: rows.map(({ value }) => Number(value)),
			};
		},
		snapshot: async (c) => {
			const kvValue = await c.kv.get("count");
			await c.db.execute(
				"CREATE TABLE IF NOT EXISTS increments (value INTEGER NOT NULL)",
			);
			const rows = await c.db.execute<{ value: number }>(
				"SELECT value FROM increments ORDER BY rowid ASC",
			);

			return {
				count: c.state.count,
				kvCount: kvValue ? Number(textDecoder.decode(kvValue)) : null,
				sqliteValues: rows.map(({ value }) => Number(value)),
			};
		},
		incrementWithoutSql: async (c, amount: number) => {
			c.state.count += amount;
			await c.kv.put("count", String(c.state.count));
			return {
				count: c.state.count,
			};
		},
		stateSnapshot: async (c) => {
			const kvValue = await c.kv.get("count");
			return {
				count: c.state.count,
				kvCount: kvValue ? Number(textDecoder.decode(kvValue)) : null,
			};
		},
		getCountViaClient: async (c) => {
			const client = c.client<typeof registry>();
			return await client.integrationActor.getForId(c.actorId).getCount();
		},
		throwTypedError: async () => {
			throw new UserError("native typed error", {
				code: "boom",
				metadata: {
					source: "native",
				},
			});
		},
		throwUntypedError: async () => {
			throw new Error("native untyped error");
		},
		goToSleep: async (c) => {
			c.sleep();
			return { ok: true };
		},
	},
});

const registry = setup({
	use: {
		correlationSource,
		correlationTarget,
		integrationActor,
	},
	endpoint,
	namespace: process.env.RIVET_NAMESPACE ?? "default",
	token: process.env.RIVET_TOKEN ?? "dev",
	envoy: {
		poolName: process.env.RIVETKIT_TEST_POOL_NAME ?? "default",
	},
});

export type NapiRuntimeFixtureRegistry = typeof registry;

const { runtime, registry: nativeRegistry, serveConfig } =
	await buildNativeRegistry(registry.parseConfig());
readPrometheusMetrics = async () => {
	if (!runtime.registryMetrics) throw new Error("metrics are not supported");
	const response = await runtime.registryMetrics(nativeRegistry);
	return textDecoder.decode(response.body);
};
serveConfig.engineBinaryPath = resolveEngineBinaryPath();

await nativeRegistry.serve(serveConfig);
