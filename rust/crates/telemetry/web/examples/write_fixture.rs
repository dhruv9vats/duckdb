use std::{env, fs, path::PathBuf};

use duckdb_telemetry_store::{DuckDbEvent, EngineEvent, EngineImplementation};
use quent_events::{DynamicAttributes, Event};
use serde_json::json;
use uuid::Uuid;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: write_fixture OUTPUT_DIR")?;
    fs::create_dir_all(&output)?;
    let timestamp = 9_007_199_254_740_993_u64;
    let events = vec![Event::new(
        Uuid::from_u128(1),
        timestamp,
        DuckDbEvent::Engine(EngineEvent::Init {
            implementation: EngineImplementation {
                name: Some("DuckDB".to_owned()),
                version: None,
                custom_attributes: DynamicAttributes::new(),
            },
            instance_name: None,
        }),
    )];
    let payload = postcard::to_allocvec(&events)?;
    let header = json!({
        "protocol_version": 1,
        "schema_hash": "d5dfec3faf35450fad26e546879bac257908f516f4d4539866f62acb0072ad42",
        "capture_id": "10000000-0000-0000-0000-000000000001",
        "run_id": "20000000-0000-0000-0000-000000000001",
        "context_id": "30000000-0000-0000-0000-000000000001",
        "seq": "0",
        "event_count": events.len(),
        "payload_len": payload.len(),
        "min_ts_ns": timestamp.to_string(),
        "max_ts_ns": timestamp.to_string(),
        "overflowed": false
    });
    fs::write(output.join("batch.postcard"), payload)?;
    fs::write(output.join("header.json"), serde_json::to_vec(&header)?)?;

    Ok(())
}
