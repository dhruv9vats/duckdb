use std::{collections::BTreeSet, env, fs, path::PathBuf};

use duckdb_telemetry_store::{DuckDb, DuckDbEvent};
use quent_events::Event;
use quent_store::event::{ModelEventStore, filesystem::Store};
use serde_json::json;
use uuid::Uuid;

const PROTOCOL_VERSION: u16 = 1;
const SCHEMA_HASH: &str = "d5dfec3faf35450fad26e546879bac257908f516f4d4539866f62acb0072ad42";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args_os().skip(1);
    let input = args
        .next()
        .map(PathBuf::from)
        .ok_or("usage: convert_capture INPUT_DIR CONTEXT_ID OUTPUT_DIR")?;
    let context_id = args
        .next()
        .ok_or("missing CONTEXT_ID")?
        .to_string_lossy()
        .parse::<Uuid>()?;
    let output = args.next().map(PathBuf::from).ok_or("missing OUTPUT_DIR")?;
    if args.next().is_some() {
        return Err("too many arguments".into());
    }

    let events = Store::<DuckDb>::new(&input)
        .events(context_id)?
        .collect::<Result<Vec<Event<DuckDbEvent>>, _>>()?;
    if events.is_empty() {
        return Err("capture contains no events".into());
    }

    let engine_id = events
        .iter()
        .find_map(|event| matches!(event.data, DuckDbEvent::Engine(_)).then_some(event.id))
        .ok_or("capture contains no engine")?;
    let query_ids = events
        .iter()
        .filter_map(|event| matches!(event.data, DuckDbEvent::Query(_)).then_some(event.id))
        .collect::<BTreeSet<_>>();
    let min_timestamp = events.iter().map(|event| event.timestamp).min().unwrap();
    let max_timestamp = events.iter().map(|event| event.timestamp).max().unwrap();
    let payload = postcard::to_allocvec(&events)?;
    let header = json!({
        "protocol_version": PROTOCOL_VERSION,
        "schema_hash": SCHEMA_HASH,
        "capture_id": context_id,
        "run_id": engine_id,
        "context_id": context_id,
        "seq": "0",
        "event_count": events.len(),
        "payload_len": payload.len(),
        "min_ts_ns": min_timestamp.to_string(),
        "max_ts_ns": max_timestamp.to_string(),
        "overflowed": false,
    });
    let seal = json!({
        "capture_id": context_id,
        "final_seq": "0",
        "watermark_ns": max_timestamp.to_string(),
        "query_ids": query_ids,
        "outcome": "success",
        "overflowed": false,
    });

    fs::create_dir_all(&output)?;
    fs::write(output.join("batch.postcard"), payload)?;
    fs::write(output.join("header.json"), serde_json::to_vec(&header)?)?;
    fs::write(output.join("seal.json"), serde_json::to_vec(&seal)?)?;

    Ok(())
}
