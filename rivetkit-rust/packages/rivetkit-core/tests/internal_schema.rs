use super::*;

#[test]
fn schema_version_is_little_endian_i64() {
	let encoded = encode_schema_version(INTERNAL_SCHEMA_VERSION);
	assert_eq!(
		decode_schema_version(&encoded).unwrap(),
		INTERNAL_SCHEMA_VERSION
	);
}

#[test]
fn ladder_version_matches_migration_count() {
	assert_eq!(MIGRATIONS.len() as i64, INTERNAL_SCHEMA_VERSION);
}

#[test]
fn existing_schedules_survive_trace_context_migration() {
	let conn = rusqlite::Connection::open_in_memory().unwrap();
	conn.execute_batch(CREATE_META_TABLE).unwrap();
	for statement in MIGRATIONS[0] {
		conn.execute_batch(statement).unwrap();
	}
	conn.execute(
		"INSERT INTO _rivet_schedule_events (event_id, trigger_at, action, args, kind, max_history) VALUES (?, ?, ?, ?, ?, ?)",
		rusqlite::params!["existing", 1_i64, "run", Vec::<u8>::new(), 0_i64, 0_i64],
	)
	.unwrap();
	for statement in MIGRATIONS[1] {
		conn.execute_batch(statement).unwrap();
	}

	let values = conn
		.query_row(
			"SELECT event_id, ray_id, traceparent, tracestate FROM _rivet_schedule_events",
			[],
			|row| {
				Ok((
					row.get::<_, String>(0)?,
					row.get::<_, Option<String>>(1)?,
					row.get::<_, Option<String>>(2)?,
					row.get::<_, Option<String>>(3)?,
				))
			},
		)
		.unwrap();
	assert_eq!(values, ("existing".to_owned(), None, None, None));
}

#[test]
fn schema_sql_does_not_embed_workload_annotations() {
	for sql in MIGRATIONS
		.iter()
		.flat_map(|migration| migration.iter().copied())
		.chain([CREATE_META_TABLE])
	{
		assert!(
			!sql.contains("-- W["),
			"workload annotation leaked into SQL: {sql}"
		);
	}
}

#[test]
fn unpublished_schema_has_explicit_values_and_minimal_constraints() {
	let sql = MIGRATIONS
		.iter()
		.flat_map(|migration| migration.iter().copied())
		.collect::<Vec<_>>()
		.join("\n")
		.to_ascii_lowercase();
	assert!(
		!sql.contains(" default "),
		"internal columns must not use defaults"
	);
	assert!(
		!sql.replace("check (id = 1)", "").contains("check"),
		"only the singleton id constraint is allowed"
	);
	assert!(sql.contains("kind             integer not null"));
	assert!(sql.contains("result         integer not null"));

	for statement in MIGRATIONS
		.iter()
		.flat_map(|migration| migration.iter().copied())
		.filter(|statement| statement.trim_start().starts_with("CREATE TABLE"))
	{
		assert!(
			statement.contains("STRICT"),
			"table is not STRICT: {statement}"
		);
	}
}
