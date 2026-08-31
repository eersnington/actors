import { type ChildProcess, spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import getPort from "get-port";
import { afterEach, describe, expect, test } from "vitest";
import { createClient } from "../src/client/mod";

const TEST_DIR = dirname(fileURLToPath(import.meta.url));
const FIXTURE_PATH = join(TEST_DIR, "fixtures", "napi-runtime-server.ts");
const ENGINE_PATH = resolve(TEST_DIR, "../../../../target/debug/rivet-engine");
const NAMESPACE = "default";
const TOKEN = "dev";
let runtimeLogs = {
	stdout: "",
	stderr: "",
};

function waitFor(
	condition: () => boolean,
	timeoutMs: number,
	message: string,
): Promise<void> {
	return new Promise((resolve, reject) => {
		const deadline = Date.now() + timeoutMs;
		const interval = setInterval(() => {
			if (condition()) {
				clearInterval(interval);
				resolve();
			} else if (Date.now() >= deadline) {
				clearInterval(interval);
				reject(new Error(message));
			}
		}, 25);
	});
}

function logField(line: string, name: string): string | undefined {
	const match = line.match(new RegExp(`(?:^| )${name}=(?:"([^"]*)"|([^ ]+))`));
	return match?.[1] ?? match?.[2];
}

async function closeServer(server: Server): Promise<void> {
	await new Promise<void>((resolve, reject) => {
		server.close((error) => (error ? reject(error) : resolve()));
	});
}

function childOutput(child: ChildProcess): string {
	void child;
	return [runtimeLogs.stdout, runtimeLogs.stderr].filter(Boolean).join("\n");
}

async function waitForHealth(
	child: ChildProcess,
	endpoint: string,
	timeoutMs: number,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;

	while (Date.now() < deadline) {
		if (child.exitCode !== null) {
			throw new Error(
				`native runtime exited before health check passed:\n${childOutput(child)}`,
			);
		}

		try {
			const response = await fetch(`${endpoint}/health`);
			if (response.ok) {
				return;
			}
		} catch {}

		await new Promise((resolve) => setTimeout(resolve, 500));
	}

	throw new Error(
		`timed out waiting for native runtime health:\n${childOutput(child)}`,
	);
}

async function waitForActorSleep(
	endpoint: string,
	actorId: string,
	timeoutMs: number,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;

	while (Date.now() < deadline) {
		const response = await fetch(
			`${endpoint}/actors?actor_ids=${encodeURIComponent(actorId)}&namespace=${encodeURIComponent(NAMESPACE)}`,
			{
				headers: {
					Authorization: `Bearer ${TOKEN}`,
				},
			},
		);
		expect(response.ok).toBe(true);

		const body = (await response.json()) as {
			actors: Array<{ sleep_ts?: number | null }>;
		};
		const actor = body.actors[0];
		if (actor?.sleep_ts) {
			return;
		}

		await new Promise((resolve) => setTimeout(resolve, 500));
	}

	throw new Error(`timed out waiting for actor ${actorId} to sleep`);
}

async function waitForActorReady<T>(
	callback: () => Promise<T>,
	timeoutMs: number,
): Promise<T> {
	const deadline = Date.now() + timeoutMs;
	let lastError: unknown;

	while (Date.now() < deadline) {
		try {
			return await callback();
		} catch (error) {
			lastError = error;
			const errorCode =
				typeof error === "object" &&
				error !== null &&
				"code" in error &&
				typeof error.code === "string"
					? error.code
					: undefined;
			if (
				!(
					(errorCode &&
						/^(no_envoys|actor_ready_timeout|actor_wake_retries_exceeded|service_unavailable)$/.test(
							errorCode,
						)) ||
					(error instanceof Error &&
						/(no_envoys|actor_ready_timeout|actor_wake_retries_exceeded|service_unavailable)/.test(
							error.message,
						))
				)
			) {
				throw error;
			}
		}

		await new Promise((resolve) => setTimeout(resolve, 500));
	}

	throw lastError instanceof Error
		? lastError
		: new Error("timed out waiting for actor to become ready");
}

async function waitForEnvoy(
	child: ChildProcess,
	endpoint: string,
	poolName: string,
	timeoutMs: number,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;

	while (Date.now() < deadline) {
		if (child.exitCode !== null) {
			throw new Error(
				`native runtime exited before envoy registration:\n${childOutput(child)}`,
			);
		}

		const response = await fetch(
			`${endpoint}/envoys?namespace=${encodeURIComponent(NAMESPACE)}&name=${encodeURIComponent(poolName)}`,
			{
				headers: {
					Authorization: `Bearer ${TOKEN}`,
				},
			},
		);

		if (response.ok) {
			const body = (await response.json()) as {
				envoys: Array<{ envoy_key: string }>;
			};

			if (body.envoys.length > 0) {
				return;
			}
		}

		await new Promise((resolve) => setTimeout(resolve, 500));
	}

	throw new Error(
		`timed out waiting for envoy registration in pool ${poolName}\n${childOutput(child)}`,
	);
}

async function upsertNormalRunnerConfig(
	child: ChildProcess,
	endpoint: string,
	poolName: string,
): Promise<void> {
	const datacentersResponse = await fetch(
		`${endpoint}/datacenters?namespace=${encodeURIComponent(NAMESPACE)}`,
		{
			headers: {
				Authorization: `Bearer ${TOKEN}`,
			},
		},
	);

	if (!datacentersResponse.ok) {
		throw new Error(
			`failed to list datacenters: ${datacentersResponse.status} ${await datacentersResponse.text()}\n${childOutput(child)}`,
		);
	}

	const datacentersBody = (await datacentersResponse.json()) as {
		datacenters: Array<{ name: string }>;
	};
	const datacenter = datacentersBody.datacenters[0]?.name;

	if (!datacenter) {
		throw new Error(
			`engine returned no datacenters\n${childOutput(child)}`,
		);
	}

	const response = await fetch(
		`${endpoint}/runner-configs/${encodeURIComponent(poolName)}?namespace=${encodeURIComponent(NAMESPACE)}`,
		{
			method: "PUT",
			headers: {
				Authorization: `Bearer ${TOKEN}`,
				"Content-Type": "application/json",
			},
			body: JSON.stringify({
				datacenters: {
					[datacenter]: {
						normal: {},
					},
				},
			}),
		},
	);

	if (response.ok) {
		return;
	}

	throw new Error(
		`failed to upsert runner config ${poolName}: ${response.status} ${await response.text()}\n${childOutput(child)}`,
	);
}

async function stopRuntime(child: ChildProcess): Promise<void> {
	if (child.exitCode !== null) {
		return;
	}

	child.kill("SIGINT");

	await new Promise<void>((resolve) => {
		const timeout = setTimeout(() => {
			if (child.exitCode === null) {
				child.kill("SIGKILL");
			}
		}, 5_000);

		child.once("exit", () => {
			clearTimeout(timeout);
			resolve();
		});
	});
}

type ExportedSpan = {
	traceId: string;
	spanId: string;
	parentSpanId: string;
};

function protobufFields(message: Uint8Array): Map<number, Uint8Array[]> {
	const fields = new Map<number, Uint8Array[]>();
	for (let offset = 0; offset < message.length; ) {
		let tag = 0;
		let shift = 0;
		while (true) {
			const byte = message[offset++]!;
			tag |= (byte & 0x7f) << shift;
			if ((byte & 0x80) === 0) break;
			shift += 7;
		}
		const wireType = tag & 7;
		if (wireType === 2) {
			let length = 0;
			shift = 0;
			while (true) {
				const byte = message[offset++]!;
				length |= (byte & 0x7f) << shift;
				if ((byte & 0x80) === 0) break;
				shift += 7;
			}
			const fieldNumber = tag >>> 3;
			const values = fields.get(fieldNumber) ?? [];
			values.push(message.subarray(offset, offset + length));
			fields.set(fieldNumber, values);
			offset += length;
		} else if (wireType === 0) {
			while ((message[offset++]! & 0x80) !== 0) {}
		} else if (wireType === 1) {
			offset += 8;
		} else if (wireType === 5) {
			offset += 4;
		} else {
			throw new Error(`unsupported protobuf wire type ${wireType}`);
		}
	}
	return fields;
}

function decodeExportedSpans(bodies: Buffer[]): ExportedSpan[] {
	const spans: ExportedSpan[] = [];
	for (const body of bodies) {
		for (const resourceSpans of protobufFields(body).get(1) ?? []) {
			for (const scopeSpans of protobufFields(resourceSpans).get(2) ?? []) {
				for (const span of protobufFields(scopeSpans).get(2) ?? []) {
					const fields = protobufFields(span);
					spans.push({
						traceId: Buffer.from(fields.get(1)?.[0] ?? []).toString("hex"),
						spanId: Buffer.from(fields.get(2)?.[0] ?? []).toString("hex"),
						parentSpanId: Buffer.from(fields.get(4)?.[0] ?? []).toString(
							"hex",
						),
					});
				}
			}
		}
	}
	return spans;
}

describe.sequential("native NAPI runtime integration", () => {
	let runtime: ChildProcess | undefined;
	let engine: ChildProcess | undefined;
	let collector: Server | undefined;
	let engineStorage: string | undefined;

	afterEach(async () => {
		if (runtime) {
			await stopRuntime(runtime);
			runtime = undefined;
		}
		if (engine) {
			await stopRuntime(engine);
			engine = undefined;
		}
		if (collector) {
			await closeServer(collector);
			collector = undefined;
		}
		if (engineStorage) {
			await rm(engineStorage, { recursive: true, force: true });
			engineStorage = undefined;
		}
	}, 30_000);

	test("runs a TS actor through registry, NAPI, core, envoy, and engine", async () => {
		const poolName = "default";
		const port = await getPort({ host: "127.0.0.1" });
		const endpoint = `http://127.0.0.1:${port}`;
		runtimeLogs = { stdout: "", stderr: "" };
		runtime = spawn(process.execPath, ["--import", "tsx", FIXTURE_PATH], {
			cwd: dirname(TEST_DIR),
			env: {
				...process.env,
				RIVET_TOKEN: TOKEN,
				RIVET_NAMESPACE: NAMESPACE,
				RIVETKIT_TEST_ENDPOINT: endpoint,
				RIVETKIT_TEST_POOL_NAME: poolName,
			},
			stdio: ["ignore", "pipe", "pipe"],
		});
		runtime.stdout?.on("data", (chunk) => {
			runtimeLogs.stdout += chunk.toString();
		});
		runtime.stderr?.on("data", (chunk) => {
			runtimeLogs.stderr += chunk.toString();
		});

		await waitForHealth(runtime, endpoint, 90_000);
		await upsertNormalRunnerConfig(runtime, endpoint, poolName);
		await waitForEnvoy(runtime, endpoint, poolName, 30_000);

		const client = createClient<any>({
			endpoint,
			token: TOKEN,
			namespace: NAMESPACE,
			poolName,
			disableMetadataLookup: true,
		}) as any;

		const handle = await waitForActorReady(
			() =>
				client.integrationActor.create([
					`napi-runtime-${crypto.randomUUID()}`,
				]),
			30_000,
		);
		const actorId = await handle.resolve();

		expect(await waitForActorReady(() => handle.getCount(), 30_000)).toBe(
			0,
		);
		expect(
			await waitForActorReady(
				() => handle.validatedAction({ amount: 4 }),
				30_000,
			),
		).toBe(4);
		await expect(
			waitForActorReady(
				() => handle.validatedAction({ amount: "bad" }),
				30_000,
			),
		).rejects.toMatchObject({
			group: "actor",
			code: "validation_error",
		});
		expect(
			await waitForActorReady(
				() => handle.emitValidatedEvent({ count: 2 }),
				30_000,
			),
		).toBe(2);
		await expect(
			waitForActorReady(
				() => handle.emitValidatedEvent({ count: "bad" }),
				30_000,
			),
		).rejects.toMatchObject({
			group: "actor",
			code: "validation_error",
		});
		expect(
			await waitForActorReady(
				() => handle.enqueueValidatedJob({ id: "job-1" }),
				30_000,
			),
		).toBe("job-1");
		await expect(
			waitForActorReady(
				() => handle.enqueueValidatedJob({ id: "" }),
				30_000,
			),
		).rejects.toMatchObject({
			group: "actor",
			code: "validation_error",
		});

		expect(
			await waitForActorReady(() => handle.increment(2), 30_000),
		).toEqual({
			count: 2,
			sqliteValues: [2],
		});
		expect(await handle.snapshot()).toEqual({
			count: 2,
			kvCount: 2,
			sqliteValues: [2],
		});

		expect(await handle.goToSleep()).toEqual({ ok: true });
		await waitForActorSleep(endpoint, actorId, 30_000);

		expect(
			await waitForActorReady(
				() => handle.incrementWithoutSql(3),
				30_000,
			),
		).toEqual({
			count: 5,
		});
		expect(await handle.getCountViaClient()).toBe(5);

		expect(await handle.stateSnapshot()).toEqual({
			count: 5,
			kvCount: 5,
		});
		await expect(handle.throwTypedError()).rejects.toMatchObject({
			group: "user",
			code: "boom",
			message: "native typed error",
			metadata: {
				source: "native",
			},
		});
		await expect(handle.throwUntypedError()).rejects.toMatchObject({
			group: "core",
			code: "internal_error",
			message: "An internal error occurred",
		});
		await client.dispose();
	}, 120_000);

	test("correlates concurrent actor calls in logs and exported traces", async () => {
		const poolName = "default";
		const [guardPort, apiPeerPort, metricsPort, collectorPort] =
			await Promise.all(
				Array.from({ length: 4 }, () => getPort({ host: "127.0.0.1" })),
			);
		const endpoint = `http://127.0.0.1:${guardPort}`;
		engineStorage = await mkdtemp(join(tmpdir(), "rivetkit-otel-engine-"));

		engine = spawn(ENGINE_PATH, ["start"], {
			env: {
				...process.env,
				RIVET__GUARD__HOST: "127.0.0.1",
				RIVET__GUARD__PORT: String(guardPort),
				RIVET__API_PEER__HOST: "127.0.0.1",
				RIVET__API_PEER__PORT: String(apiPeerPort),
				RIVET__METRICS__HOST: "127.0.0.1",
				RIVET__METRICS__PORT: String(metricsPort),
				RIVET__FILE_SYSTEM__PATH: engineStorage,
			},
			stdio: "ignore",
		});
		await waitForHealth(engine, endpoint, 30_000);

		const otlpBodies: Buffer[] = [];
		collector = createServer((request, response) => {
			const chunks: Buffer[] = [];
			request.on("data", (chunk: Buffer) => chunks.push(chunk));
			request.on("end", () => {
				otlpBodies.push(Buffer.concat(chunks));
				response.writeHead(200).end();
			});
		});
		await new Promise<void>((resolve) =>
			collector?.listen(collectorPort, "127.0.0.1", resolve),
		);

		runtimeLogs = { stdout: "", stderr: "" };
		runtime = spawn(process.execPath, ["--import", "tsx", FIXTURE_PATH], {
			cwd: dirname(TEST_DIR),
			env: {
				...process.env,
				RIVET_TOKEN: TOKEN,
				RIVET_NAMESPACE: NAMESPACE,
				RIVETKIT_TEST_ENDPOINT: endpoint,
				RIVETKIT_TEST_POOL_NAME: poolName,
				RIVET_LOG_LEVEL: "info",
				OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: `http://127.0.0.1:${collectorPort}/v1/traces`,
				OTEL_SERVICE_NAME: "rivetkit-actor-call-integration",
			},
			stdio: ["ignore", "pipe", "pipe"],
		});
		runtime.stdout?.on("data", (chunk) => {
			runtimeLogs.stdout += chunk.toString();
		});
		runtime.stderr?.on("data", (chunk) => {
			runtimeLogs.stderr += chunk.toString();
		});

		await upsertNormalRunnerConfig(runtime, endpoint, poolName);
		await waitForEnvoy(runtime, endpoint, poolName, 30_000);
		const client = createClient<any>({
			endpoint,
			token: TOKEN,
			namespace: NAMESPACE,
			poolName,
			disableMetadataLookup: true,
		}) as any;
		const handle = await waitForActorReady(
			() =>
				client.correlationSource.create([
					`correlation-${crypto.randomUUID()}`,
				]),
			30_000,
		);

		const tokens = [crypto.randomUUID(), crypto.randomUUID()];
		expect(await Promise.all(tokens.map((token) => handle.relay(token)))).toEqual(
			tokens,
		);
		await waitFor(
			() =>
				tokens.every(
					(token) =>
						runtimeLogs.stdout
							.split("\n")
							.filter((line) => line.includes(`correlation_token=${token}`))
							.length === 2,
				),
			10_000,
			`timed out waiting for correlated logs:\n${runtimeLogs.stdout}`,
		);

		const contexts = tokens.map((token) => {
			const lines = runtimeLogs.stdout
				.split("\n")
				.filter((line) => line.includes(`correlation_token=${token}`));
			const source = lines.find(
				(line) => logField(line, "correlation_role") === "source",
			)!;
			const target = lines.find(
				(line) => logField(line, "correlation_role") === "target",
			)!;
			expect(logField(target, "trace_id")).toBe(logField(source, "trace_id"));
			expect(logField(target, "ray_id")).toBe(logField(source, "ray_id"));
			return {
				traceId: logField(source, "trace_id")!,
				sourceSpanId: logField(source, "span_id")!,
				targetSpanId: logField(target, "span_id")!,
			};
		});
		expect(contexts[0]?.traceId).not.toBe(contexts[1]?.traceId);

		await client.dispose();
		await waitFor(
			() => otlpBodies.length > 0,
			10_000,
			`timed out waiting for OTLP export:\n${runtimeLogs.stdout}\n${runtimeLogs.stderr}`,
		);
		await stopRuntime(runtime);
		runtime = undefined;
		const spans = decodeExportedSpans(otlpBodies);
		for (const context of contexts) {
			expect(
				spans.find((span) => span.spanId === context.targetSpanId),
			).toEqual({
				traceId: context.traceId,
				spanId: context.targetSpanId,
				parentSpanId: context.sourceSpanId,
			});
		}
	}, 120_000);
});
