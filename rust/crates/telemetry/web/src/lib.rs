use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};

use duckdb_telemetry_analyzer::DuckDbUiAnalyzer;
use duckdb_telemetry_store::DuckDbEvent;
use quent_events::Event;
use quent_query_engine_analyzer::{
    EngineEntity, QueryEngineModel, QueryEntity, QueryGroupEntity, ui::UiAnalyzer,
};
use quent_query_engine_ui::{OperatorFilter, QueryFilter};
use quent_ui::{
    entities::request::EntityListRequest,
    timeline::{
        categorical::CategoricalTimelineRequest,
        request::{BulkTimelineRequest, SingleTimelineRequest},
    },
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const PROTOCOL_VERSION: u16 = 1;
include!(concat!(env!("OUT_DIR"), "/manifest.rs"));
const DEFAULT_MAX_BATCH_BYTES: u32 = 4 * 1024 * 1024;
const DEFAULT_MAX_CAPTURE_BYTES: u64 = 32 * 1024 * 1024;
const DEFAULT_MAX_SESSION_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_MAX_SNAPSHOT_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_MAX_HISTORY: usize = 8;
const SNAPSHOT_ENCODED_WEIGHT: u64 = 10;

type Result<T> = std::result::Result<T, ProtocolError>;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Manifest {
    pub protocol_version: u16,
    pub schema_hash: String,
    pub build_id: String,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            schema_hash: SCHEMA_HASH.to_owned(),
            build_id: BUILD_ID.to_owned(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Limits {
    #[serde(default = "default_max_batch_bytes")]
    pub max_batch_bytes: u32,
    #[serde(default = "default_max_capture_bytes")]
    pub max_capture_bytes: u64,
    #[serde(default = "default_max_session_bytes")]
    pub max_session_bytes: u64,
    #[serde(default = "default_max_snapshot_bytes")]
    pub max_snapshot_bytes: u64,
    #[serde(default = "default_max_history")]
    pub max_history: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            max_capture_bytes: DEFAULT_MAX_CAPTURE_BYTES,
            max_session_bytes: DEFAULT_MAX_SESSION_BYTES,
            max_snapshot_bytes: DEFAULT_MAX_SNAPSHOT_BYTES,
            max_history: DEFAULT_MAX_HISTORY,
        }
    }
}

const fn default_max_batch_bytes() -> u32 {
    DEFAULT_MAX_BATCH_BYTES
}

const fn default_max_capture_bytes() -> u64 {
    DEFAULT_MAX_CAPTURE_BYTES
}

const fn default_max_session_bytes() -> u64 {
    DEFAULT_MAX_SESSION_BYTES
}

const fn default_max_history() -> usize {
    DEFAULT_MAX_HISTORY
}

const fn default_max_snapshot_bytes() -> u64 {
    DEFAULT_MAX_SNAPSHOT_BYTES
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InitRequest {
    pub manifest: Manifest,
    #[serde(default)]
    pub limits: Limits,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BatchHeader {
    pub protocol_version: u16,
    pub schema_hash: String,
    pub capture_id: Uuid,
    pub run_id: Uuid,
    pub context_id: Uuid,
    pub seq: String,
    pub event_count: u32,
    pub payload_len: u32,
    pub min_ts_ns: String,
    pub max_ts_ns: String,
    pub overflowed: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Success,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Seal {
    pub capture_id: Uuid,
    pub final_seq: Option<String>,
    pub watermark_ns: String,
    pub query_ids: Vec<Uuid>,
    pub outcome: Outcome,
    pub overflowed: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CaptureState {
    Sealed,
    Incomplete,
    Overflowed,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BatchAck {
    pub capture_id: Uuid,
    pub acked_seq: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SealAck {
    pub capture_id: Uuid,
    pub acked_seq: Option<String>,
    pub revision: Option<String>,
    pub state: CaptureState,
    pub query_ids: Vec<Uuid>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AnalysisRequest {
    pub revision: String,
    pub method: String,
    pub route: String,
    #[serde(default)]
    pub params: serde_json::Value,
    #[serde(default)]
    pub body: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct ProtocolError {
    pub code: &'static str,
    pub message: String,
}

impl ProtocolError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ProtocolError {}

struct StoredBatch {
    header: BatchHeader,
    payload: Vec<u8>,
}

struct PendingCapture {
    run_id: Uuid,
    context_id: Uuid,
    batches: BTreeMap<u64, StoredBatch>,
    bytes: u64,
    max_timestamp: Option<u64>,
    overflowed: bool,
}

struct Snapshot {
    revision: u64,
    analyzer: DuckDbUiAnalyzer,
    engine_id: Uuid,
    context_ids: Vec<Uuid>,
    source_bytes: u64,
}

pub struct CaptureStore {
    limits: Limits,
    pending: BTreeMap<Uuid, PendingCapture>,
    completed: HashSet<Uuid>,
    session_batches: Vec<Vec<u8>>,
    session_bytes: u64,
    snapshots: VecDeque<Snapshot>,
    next_revision: u64,
}

impl CaptureStore {
    pub fn try_new(request: InitRequest) -> Result<Self> {
        validate_manifest(&request.manifest)?;
        if request.limits.max_history == 0 {
            return Err(ProtocolError::new(
                "INVALID_LIMIT",
                "max_history must be positive",
            ));
        }
        if request.limits.max_batch_bytes == 0
            || request.limits.max_capture_bytes == 0
            || request.limits.max_session_bytes == 0
            || request.limits.max_snapshot_bytes == 0
        {
            return Err(ProtocolError::new(
                "INVALID_LIMIT",
                "byte limits must be positive",
            ));
        }

        Ok(Self {
            limits: request.limits,
            pending: BTreeMap::new(),
            completed: HashSet::new(),
            session_batches: Vec::new(),
            session_bytes: 0,
            snapshots: VecDeque::new(),
            next_revision: 1,
        })
    }

    pub fn ingest(&mut self, header: BatchHeader, payload: &[u8]) -> Result<BatchAck> {
        validate_header(&header, payload, self.limits.max_batch_bytes)?;
        if self.completed.contains(&header.capture_id) {
            return Err(ProtocolError::new(
                "CAPTURE_COMPLETED",
                "capture id was already sealed",
            ));
        }
        let sequence = parse_u64("seq", &header.seq)?;
        let min_timestamp = parse_u64("min_ts_ns", &header.min_ts_ns)?;
        let max_timestamp = parse_u64("max_ts_ns", &header.max_ts_ns)?;
        if min_timestamp > max_timestamp {
            return Err(ProtocolError::new(
                "FRAME_BOUNDS",
                "minimum timestamp exceeds maximum timestamp",
            ));
        }
        if let Some(capture) = self.pending.get(&header.capture_id) {
            if capture.run_id != header.run_id || capture.context_id != header.context_id {
                return Err(ProtocolError::new(
                    "CAPTURE_MISMATCH",
                    "run or context changed within a capture",
                ));
            }
            if let Some(existing) = capture.batches.get(&sequence) {
                if existing.header == header && existing.payload == payload {
                    return Ok(BatchAck {
                        capture_id: header.capture_id,
                        acked_seq: header.seq,
                    });
                }
                return Err(ProtocolError::new(
                    "DUPLICATE_MISMATCH",
                    "duplicate sequence has different content",
                ));
            }
        }

        let events: Vec<Event<DuckDbEvent>> = postcard::from_bytes(payload).map_err(|error| {
            ProtocolError::new("FRAME_DECODE", format!("invalid postcard batch: {error}"))
        })?;
        if events.len() != header.event_count as usize {
            return Err(ProtocolError::new(
                "FRAME_BOUNDS",
                "event count does not match decoded payload",
            ));
        }
        validate_timestamp_range(&events, min_timestamp, max_timestamp)?;

        let pending_bytes = self
            .pending
            .values()
            .map(|capture| capture.bytes)
            .sum::<u64>()
            .saturating_add(payload.len() as u64);
        if pending_bytes > self.limits.max_session_bytes {
            return Err(ProtocolError::new(
                "SESSION_LIMIT",
                "total pending byte limit exceeded",
            ));
        }
        let capture = self
            .pending
            .entry(header.capture_id)
            .or_insert_with(|| PendingCapture {
                run_id: header.run_id,
                context_id: header.context_id,
                batches: BTreeMap::new(),
                bytes: 0,
                max_timestamp: None,
                overflowed: false,
            });
        let expected = capture.batches.len() as u64;
        if sequence != expected {
            return Err(ProtocolError::new(
                "SEQUENCE_GAP",
                format!("expected sequence {expected}, got {sequence}"),
            ));
        }
        let next_bytes = capture.bytes.saturating_add(payload.len() as u64);
        if next_bytes > self.limits.max_capture_bytes {
            capture.overflowed = true;
            return Err(ProtocolError::new(
                "CAPTURE_LIMIT",
                "capture byte limit exceeded",
            ));
        }
        capture.bytes = next_bytes;
        capture.max_timestamp = Some(
            capture
                .max_timestamp
                .map_or(max_timestamp, |current| current.max(max_timestamp)),
        );
        capture.overflowed |= header.overflowed;
        capture.batches.insert(
            sequence,
            StoredBatch {
                header: header.clone(),
                payload: payload.to_vec(),
            },
        );

        Ok(BatchAck {
            capture_id: header.capture_id,
            acked_seq: header.seq,
        })
    }

    pub fn seal(&mut self, seal: Seal) -> Result<SealAck> {
        if self.completed.contains(&seal.capture_id) {
            return Err(ProtocolError::new(
                "CAPTURE_COMPLETED",
                "capture id was already sealed",
            ));
        }
        let watermark = parse_u64("watermark_ns", &seal.watermark_ns)?;
        let expected_final = seal
            .final_seq
            .as_deref()
            .map(|value| parse_u64("final_seq", value))
            .transpose()?;
        let Some(capture) = self.pending.get(&seal.capture_id) else {
            if expected_final.is_none()
                && seal.query_ids.is_empty()
                && seal.outcome != Outcome::Success
            {
                self.completed.insert(seal.capture_id);
                return Ok(SealAck {
                    capture_id: seal.capture_id,
                    acked_seq: None,
                    revision: None,
                    state: CaptureState::Failed,
                    query_ids: seal.query_ids,
                });
            }
            return Err(ProtocolError::new(
                "CAPTURE_NOT_FOUND",
                "capture was not ingested",
            ));
        };
        let actual_final = capture
            .batches
            .last_key_value()
            .map(|(sequence, _)| *sequence);
        if expected_final != actual_final {
            return Err(ProtocolError::new(
                "SEQUENCE_GAP",
                format!(
                    "seal final sequence {:?} does not match {:?}",
                    expected_final, actual_final
                ),
            ));
        }
        if capture
            .max_timestamp
            .is_some_and(|timestamp| watermark < timestamp)
        {
            return Err(ProtocolError::new(
                "WATERMARK_REGRESSION",
                "seal watermark precedes accepted events",
            ));
        }
        if self.session_bytes.saturating_add(capture.bytes) > self.limits.max_session_bytes {
            return Err(ProtocolError::new(
                "SESSION_LIMIT",
                "session byte limit exceeded; reset is required",
            ));
        }

        let state = capture_state(seal.outcome, seal.overflowed || capture.overflowed);
        let capture_events = decode_batches(capture.batches.values().map(|batch| &batch.payload))?;
        validate_query_ids(&capture_events, &seal.query_ids)?;
        let revision = if state == CaptureState::Overflowed || capture.batches.is_empty() {
            None
        } else {
            let candidate_bytes = self.session_bytes.saturating_add(capture.bytes);
            let retained_bytes = self
                .snapshots
                .iter()
                .map(|snapshot| snapshot.source_bytes)
                .sum::<u64>()
                .saturating_add(candidate_bytes);
            if retained_bytes.saturating_mul(SNAPSHOT_ENCODED_WEIGHT)
                > self.limits.max_snapshot_bytes
            {
                return Err(ProtocolError::new(
                    "SNAPSHOT_LIMIT",
                    "retained snapshot budget exceeded; reset is required",
                ));
            }
            let all_batches = self
                .session_batches
                .iter()
                .chain(capture.batches.values().map(|batch| &batch.payload));
            let events = decode_batches(all_batches)?;
            let engine_id = engine_id(&events)?;
            let analyzer =
                DuckDbUiAnalyzer::try_new_snapshot(engine_id, events.into_iter(), watermark)
                    .map_err(|error| ProtocolError::new("ANALYSIS_FAILED", error.to_string()))?;
            Some((engine_id, analyzer))
        };

        let capture = self.pending.remove(&seal.capture_id).unwrap();
        self.completed.insert(seal.capture_id);
        let context_id = capture.context_id;
        if state != CaptureState::Overflowed {
            for (_, batch) in capture.batches {
                self.session_bytes += batch.payload.len() as u64;
                self.session_batches.push(batch.payload);
            }
        }
        let revision = revision
            .map(|(engine_id, analyzer)| {
                self.publish(context_id, engine_id, analyzer, self.session_bytes)
            })
            .transpose()?;

        Ok(SealAck {
            capture_id: seal.capture_id,
            acked_seq: actual_final.map(|sequence| sequence.to_string()),
            revision: revision.map(|revision| revision.to_string()),
            state,
            query_ids: seal.query_ids,
        })
    }

    pub fn reset(&mut self) {
        self.pending.clear();
        self.completed.clear();
        self.session_batches.clear();
        self.session_bytes = 0;
        self.snapshots.clear();
    }

    pub fn request(&self, request: AnalysisRequest) -> Result<String> {
        let revision = parse_u64("revision", &request.revision)?;
        let snapshot = self
            .snapshots
            .iter()
            .find(|snapshot| snapshot.revision == revision)
            .ok_or_else(|| {
                ProtocolError::new(
                    "REVISION_NOT_FOUND",
                    "snapshot revision was evicted or unknown",
                )
            })?;
        let segments = request
            .route
            .split('?')
            .next()
            .unwrap_or_default()
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        let response = match (request.method.as_str(), segments.as_slice()) {
            ("GET", ["api", "engines"]) => serde_json::to_value(vec![
                snapshot
                    .analyzer
                    .query_engine_model()
                    .engine()
                    .and_then(EngineEntity::to_ui)
                    .map_err(analysis_error)?,
            ]),
            ("GET", ["api", "engines", engine]) => {
                require_id("engine", engine, snapshot.engine_id)?;
                serde_json::to_value(
                    snapshot
                        .analyzer
                        .query_engine_model()
                        .engine()
                        .and_then(EngineEntity::to_ui)
                        .map_err(analysis_error)?,
                )
            }
            ("GET", ["api", "engines", engine, "contexts"]) => {
                require_id("engine", engine, snapshot.engine_id)?;
                serde_json::to_value(serde_json::json!({
                    "context_ids": snapshot.context_ids,
                }))
            }
            ("GET", ["api", "engines", engine, "query-groups"]) => {
                require_id("engine", engine, snapshot.engine_id)?;
                serde_json::to_value(
                    snapshot
                        .analyzer
                        .query_engine_model()
                        .query_groups()
                        .map(QueryGroupEntity::to_ui)
                        .collect::<Vec<_>>(),
                )
            }
            (
                "GET",
                [
                    "api",
                    "engines",
                    engine,
                    "query_group",
                    query_group,
                    "queries",
                ],
            ) => {
                require_id("engine", engine, snapshot.engine_id)?;
                let query_group = parse_uuid("query group", query_group)?;
                let queries = snapshot
                    .analyzer
                    .query_engine_model()
                    .queries()
                    .filter(|query| query.query_group_id() == Some(query_group))
                    .map(QueryEntity::to_ui)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(analysis_error)?;
                serde_json::to_value(queries)
            }
            ("GET", ["api", "engines", engine, "query", query]) => {
                require_id("engine", engine, snapshot.engine_id)?;
                let query = parse_uuid("query", query)?;
                serde_json::to_value(
                    snapshot
                        .analyzer
                        .query_bundle(query)
                        .map_err(analysis_error)?,
                )
            }
            ("POST", ["api", "engines", engine, "entities"]) => {
                require_id("engine", engine, snapshot.engine_id)?;
                let body: EntityListRequest<QueryFilter, OperatorFilter> =
                    parse_body(request.body)?;
                serde_json::to_value(
                    snapshot
                        .analyzer
                        .list_entities(body)
                        .map_err(analysis_error)?,
                )
            }
            ("POST", ["api", "engines", engine, "timeline", "single"]) => {
                require_id("engine", engine, snapshot.engine_id)?;
                let body: SingleTimelineRequest<QueryFilter, OperatorFilter> =
                    parse_body(request.body)?;
                serde_json::to_value(
                    snapshot
                        .analyzer
                        .single_resource_timeline(body)
                        .map_err(analysis_error)?,
                )
            }
            ("POST", ["api", "engines", engine, "timeline", "bulk"]) => {
                require_id("engine", engine, snapshot.engine_id)?;
                let body: BulkTimelineRequest<QueryFilter, OperatorFilter> =
                    parse_body(request.body)?;
                serde_json::to_value(
                    snapshot
                        .analyzer
                        .bulk_resource_timeline(body)
                        .map_err(analysis_error)?,
                )
            }
            ("POST", ["api", "engines", engine, "timeline", "data-flow"]) => {
                require_id("engine", engine, snapshot.engine_id)?;
                let body: CategoricalTimelineRequest<QueryFilter> = parse_body(request.body)?;
                serde_json::to_value(
                    snapshot
                        .analyzer
                        .data_flow_timeline(body)
                        .map_err(analysis_error)?,
                )
            }
            _ => {
                return Err(ProtocolError::new(
                    "ROUTE_NOT_FOUND",
                    format!("unsupported {} {}", request.method, request.route),
                ));
            }
        }
        .map_err(|error| ProtocolError::new("SERIALIZE_FAILED", error.to_string()))?;

        serde_json::to_string(&response)
            .map_err(|error| ProtocolError::new("SERIALIZE_FAILED", error.to_string()))
    }

    fn publish(
        &mut self,
        context_id: Uuid,
        engine_id: Uuid,
        analyzer: DuckDbUiAnalyzer,
        source_bytes: u64,
    ) -> Result<u64> {
        let revision = self.next_revision;
        self.next_revision = self
            .next_revision
            .checked_add(1)
            .ok_or_else(|| ProtocolError::new("REVISION_OVERFLOW", "snapshot revision overflow"))?;
        let mut context_ids = self
            .snapshots
            .back()
            .map(|snapshot| snapshot.context_ids.clone())
            .unwrap_or_default();
        if !context_ids.contains(&context_id) {
            context_ids.push(context_id);
        }
        self.snapshots.push_back(Snapshot {
            revision,
            analyzer,
            engine_id,
            context_ids,
            source_bytes,
        });
        while self.snapshots.len() > self.limits.max_history {
            self.snapshots.pop_front();
        }

        Ok(revision)
    }
}

fn validate_manifest(manifest: &Manifest) -> Result<()> {
    if manifest.protocol_version != PROTOCOL_VERSION {
        return Err(ProtocolError::new(
            "VERSION_MISMATCH",
            format!("expected protocol {PROTOCOL_VERSION}"),
        ));
    }
    if manifest.schema_hash != SCHEMA_HASH {
        return Err(ProtocolError::new(
            "SCHEMA_MISMATCH",
            "schema hash mismatch",
        ));
    }
    if manifest.build_id != BUILD_ID {
        return Err(ProtocolError::new(
            "BUILD_MISMATCH",
            "build identifier mismatch",
        ));
    }

    Ok(())
}

fn validate_header(header: &BatchHeader, payload: &[u8], max_batch_bytes: u32) -> Result<()> {
    validate_manifest(&Manifest {
        protocol_version: header.protocol_version,
        schema_hash: header.schema_hash.clone(),
        build_id: BUILD_ID.to_owned(),
    })?;
    if header.payload_len as usize != payload.len()
        || header.payload_len > max_batch_bytes
        || header.event_count == 0
    {
        return Err(ProtocolError::new(
            "FRAME_BOUNDS",
            "invalid payload length or event count",
        ));
    }

    Ok(())
}

fn validate_timestamp_range(
    events: &[Event<DuckDbEvent>],
    min_timestamp: u64,
    max_timestamp: u64,
) -> Result<()> {
    let observed_min = events.iter().map(|event| event.timestamp).min();
    let observed_max = events.iter().map(|event| event.timestamp).max();
    if observed_min != Some(min_timestamp) || observed_max != Some(max_timestamp) {
        return Err(ProtocolError::new(
            "FRAME_BOUNDS",
            "timestamp range does not match decoded events",
        ));
    }

    Ok(())
}

fn decode_batches<'a>(
    batches: impl IntoIterator<Item = &'a Vec<u8>>,
) -> Result<Vec<Event<DuckDbEvent>>> {
    let mut events = Vec::new();
    for batch in batches {
        let decoded: Vec<Event<DuckDbEvent>> = postcard::from_bytes(batch).map_err(|error| {
            ProtocolError::new("FRAME_DECODE", format!("invalid retained batch: {error}"))
        })?;
        events.extend(decoded);
    }

    Ok(events)
}

fn validate_query_ids(events: &[Event<DuckDbEvent>], claimed: &[Uuid]) -> Result<()> {
    let actual = events
        .iter()
        .filter_map(|event| match event.data {
            DuckDbEvent::Query(_) => Some(event.id),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let claimed = claimed.iter().copied().collect::<BTreeSet<_>>();
    if actual != claimed {
        return Err(ProtocolError::new(
            "QUERY_ID_MISMATCH",
            "sealed query ids do not match captured query events",
        ));
    }

    Ok(())
}

fn engine_id(events: &[Event<DuckDbEvent>]) -> Result<Uuid> {
    events
        .iter()
        .find_map(|event| match event.data {
            DuckDbEvent::Engine(_) => Some(event.id),
            _ => None,
        })
        .ok_or_else(|| ProtocolError::new("ANALYSIS_FAILED", "capture has no engine event"))
}

fn parse_u64(field: &'static str, value: &str) -> Result<u64> {
    value.parse().map_err(|_| {
        ProtocolError::new(
            "INVALID_INTEGER",
            format!("{field} must be an unsigned decimal string"),
        )
    })
}

fn parse_uuid(field: &'static str, value: &str) -> Result<Uuid> {
    Uuid::parse_str(value)
        .map_err(|_| ProtocolError::new("INVALID_ID", format!("{field} is not a UUID")))
}

fn require_id(field: &'static str, value: &str, expected: Uuid) -> Result<()> {
    if parse_uuid(field, value)? != expected {
        return Err(ProtocolError::new(
            "INVALID_ID",
            format!("unknown {field} id"),
        ));
    }

    Ok(())
}

fn parse_body<T: serde::de::DeserializeOwned>(body: serde_json::Value) -> Result<T> {
    serde_json::from_value(body)
        .map_err(|error| ProtocolError::new("INVALID_REQUEST", error.to_string()))
}

fn analysis_error(error: impl std::fmt::Display) -> ProtocolError {
    ProtocolError::new("ANALYSIS_FAILED", error.to_string())
}

fn capture_state(outcome: Outcome, overflowed: bool) -> CaptureState {
    if overflowed {
        return CaptureState::Overflowed;
    }
    match outcome {
        Outcome::Success | Outcome::Failed => CaptureState::Sealed,
        Outcome::Cancelled => CaptureState::Incomplete,
    }
}

#[cfg(feature = "wasm")]
mod wasm {
    use wasm_bindgen::prelude::*;

    use super::*;

    #[wasm_bindgen]
    pub struct AnalyzerFacade {
        store: CaptureStore,
    }

    #[wasm_bindgen]
    impl AnalyzerFacade {
        #[wasm_bindgen(constructor)]
        pub fn new(request: &str) -> std::result::Result<AnalyzerFacade, JsValue> {
            let request = serde_json::from_str(request).map_err(js_error)?;
            let store = CaptureStore::try_new(request).map_err(js_error)?;

            Ok(Self { store })
        }

        pub fn ingest(
            &mut self,
            header: &str,
            payload: &[u8],
        ) -> std::result::Result<String, JsValue> {
            let header = serde_json::from_str(header).map_err(js_error)?;
            let ack = self.store.ingest(header, payload).map_err(js_error)?;
            serde_json::to_string(&ack).map_err(js_error)
        }

        pub fn seal(&mut self, seal: &str) -> std::result::Result<String, JsValue> {
            let seal = serde_json::from_str(seal).map_err(js_error)?;
            let ack = self.store.seal(seal).map_err(js_error)?;
            serde_json::to_string(&ack).map_err(js_error)
        }

        pub fn reset(&mut self) {
            self.store.reset();
        }

        pub fn request(&self, request: &str) -> std::result::Result<String, JsValue> {
            let request = serde_json::from_str(request).map_err(js_error)?;
            self.store.request(request).map_err(js_error)
        }
    }

    fn js_error(error: impl std::fmt::Display) -> JsValue {
        JsValue::from_str(&error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use quent_events::DynamicAttributes;

    use super::*;

    fn request() -> InitRequest {
        InitRequest {
            manifest: Manifest::default(),
            limits: Limits::default(),
        }
    }

    fn engine_batch(capture_id: Uuid, sequence: &str, overflowed: bool) -> (BatchHeader, Vec<u8>) {
        let events = vec![Event::new(
            Uuid::from_u128(1),
            9_007_199_254_740_993,
            DuckDbEvent::Engine(duckdb_telemetry_store::EngineEvent::Init {
                implementation: duckdb_telemetry_store::EngineImplementation {
                    name: Some("DuckDB".to_owned()),
                    version: None,
                    custom_attributes: DynamicAttributes::new(),
                },
                instance_name: None,
            }),
        )];
        let payload = postcard::to_allocvec(&events).unwrap();
        (
            BatchHeader {
                protocol_version: PROTOCOL_VERSION,
                schema_hash: SCHEMA_HASH.to_owned(),
                capture_id,
                run_id: Uuid::from_u128(3),
                context_id: Uuid::from_u128(4),
                seq: sequence.to_owned(),
                event_count: 1,
                payload_len: payload.len() as u32,
                min_ts_ns: "9007199254740993".to_owned(),
                max_ts_ns: "9007199254740993".to_owned(),
                overflowed,
            },
            payload,
        )
    }

    fn worker_batch(capture_id: Uuid) -> (BatchHeader, Vec<u8>) {
        let timestamp = 9_007_199_254_740_994;
        let events = vec![Event::new(
            Uuid::from_u128(5),
            timestamp,
            DuckDbEvent::Worker(duckdb_telemetry_store::WorkerEvent::Init {
                parent_engine_id: quent_events::EntityRef::new(Uuid::from_u128(1), ()),
                instance_name: "DuckDB worker".to_owned(),
            }),
        )];
        let payload = postcard::to_allocvec(&events).unwrap();
        (
            BatchHeader {
                protocol_version: PROTOCOL_VERSION,
                schema_hash: SCHEMA_HASH.to_owned(),
                capture_id,
                run_id: Uuid::from_u128(6),
                context_id: Uuid::from_u128(4),
                seq: "0".to_owned(),
                event_count: 1,
                payload_len: payload.len() as u32,
                min_ts_ns: timestamp.to_string(),
                max_ts_ns: timestamp.to_string(),
                overflowed: false,
            },
            payload,
        )
    }

    #[test]
    fn rejects_sequence_gap() {
        let mut store = CaptureStore::try_new(request()).unwrap();
        let (mut header, payload) = engine_batch(Uuid::from_u128(2), "1", false);
        let error = store.ingest(header.clone(), &payload).unwrap_err();

        assert_eq!(error.code, "SEQUENCE_GAP");
        header.seq = "0".to_owned();
        assert_eq!(store.ingest(header, &payload).unwrap().acked_seq, "0");
    }

    #[test]
    fn failed_seal_does_not_commit_batch() {
        let capture_id = Uuid::from_u128(2);
        let mut store = CaptureStore::try_new(request()).unwrap();
        let (header, payload) = engine_batch(capture_id, "0", false);
        store.ingest(header, &payload).unwrap();

        let invalid = store
            .seal(Seal {
                capture_id,
                final_seq: Some("0".to_owned()),
                watermark_ns: "9007199254740992".to_owned(),
                query_ids: vec![],
                outcome: Outcome::Success,
                overflowed: false,
            })
            .unwrap_err();
        assert_eq!(invalid.code, "WATERMARK_REGRESSION");
        assert!(store.session_batches.is_empty());

        let ack = store
            .seal(Seal {
                capture_id,
                final_seq: Some("0".to_owned()),
                watermark_ns: "9007199254740993".to_owned(),
                query_ids: vec![],
                outcome: Outcome::Success,
                overflowed: false,
            })
            .unwrap();
        assert_eq!(ack.revision.as_deref(), Some("1"));
    }

    #[test]
    fn overflow_never_publishes_exact_snapshot() {
        let capture_id = Uuid::from_u128(2);
        let mut store = CaptureStore::try_new(request()).unwrap();
        let (header, payload) = engine_batch(capture_id, "0", true);
        store.ingest(header, &payload).unwrap();

        let ack = store
            .seal(Seal {
                capture_id,
                final_seq: Some("0".to_owned()),
                watermark_ns: "9007199254740993".to_owned(),
                query_ids: vec![],
                outcome: Outcome::Success,
                overflowed: true,
            })
            .unwrap();

        assert_eq!(ack.state, CaptureState::Overflowed);
        assert_eq!(ack.revision, None);
        assert!(store.session_batches.is_empty());
        assert_eq!(
            store
                .ingest(engine_batch(capture_id, "0", false).0, &payload)
                .unwrap_err()
                .code,
            "CAPTURE_COMPLETED"
        );
    }

    #[test]
    fn duplicate_must_match_exact_bytes() {
        let capture_id = Uuid::from_u128(2);
        let mut store = CaptureStore::try_new(request()).unwrap();
        let (header, payload) = engine_batch(capture_id, "0", false);
        store.ingest(header.clone(), &payload).unwrap();
        assert_eq!(
            store.ingest(header.clone(), &payload).unwrap().acked_seq,
            "0"
        );

        let mut different = payload;
        different[0] ^= 1;
        let error = store.ingest(header, &different).unwrap_err();
        assert_eq!(error.code, "DUPLICATE_MISMATCH");
    }

    #[test]
    fn rejects_truncated_frame_before_state_change() {
        let capture_id = Uuid::from_u128(2);
        let mut store = CaptureStore::try_new(request()).unwrap();
        let (mut header, mut payload) = engine_batch(capture_id, "0", false);
        payload.pop();
        header.payload_len = payload.len() as u32;

        let error = store.ingest(header, &payload).unwrap_err();

        assert_eq!(error.code, "FRAME_DECODE");
        assert!(!store.pending.contains_key(&capture_id));
    }

    #[test]
    fn snapshot_budget_preserves_last_revision() {
        let first_id = Uuid::from_u128(2);
        let second_id = Uuid::from_u128(7);
        let mut store = CaptureStore::try_new(request()).unwrap();
        let (first_header, first_payload) = engine_batch(first_id, "0", false);
        store.ingest(first_header, &first_payload).unwrap();
        store
            .seal(Seal {
                capture_id: first_id,
                final_seq: Some("0".to_owned()),
                watermark_ns: "9007199254740993".to_owned(),
                query_ids: vec![],
                outcome: Outcome::Success,
                overflowed: false,
            })
            .unwrap();
        let (second_header, second_payload) = worker_batch(second_id);
        store.ingest(second_header, &second_payload).unwrap();
        store.limits.max_snapshot_bytes = store.session_bytes * SNAPSHOT_ENCODED_WEIGHT;

        let error = store
            .seal(Seal {
                capture_id: second_id,
                final_seq: Some("0".to_owned()),
                watermark_ns: "9007199254740994".to_owned(),
                query_ids: vec![],
                outcome: Outcome::Success,
                overflowed: false,
            })
            .unwrap_err();

        assert_eq!(error.code, "SNAPSHOT_LIMIT");
        assert_eq!(store.snapshots.len(), 1);
        assert!(store.pending.contains_key(&second_id));
        assert!(
            store
                .request(AnalysisRequest {
                    revision: "1".to_owned(),
                    method: "GET".to_owned(),
                    route: "/api/engines".to_owned(),
                    params: serde_json::Value::Null,
                    body: serde_json::Value::Null,
                })
                .is_ok()
        );
    }

    #[test]
    fn failed_sql_can_seal_complete_telemetry() {
        assert_eq!(capture_state(Outcome::Failed, false), CaptureState::Sealed);
        assert_eq!(
            capture_state(Outcome::Cancelled, false),
            CaptureState::Incomplete
        );
    }

    #[test]
    fn producer_frames_decode_as_event_vector() {
        let events = [
            Event::new(
                Uuid::from_u128(1),
                10,
                DuckDbEvent::Engine(duckdb_telemetry_store::EngineEvent::Exit),
            ),
            Event::new(
                Uuid::from_u128(2),
                20,
                DuckDbEvent::Engine(duckdb_telemetry_store::EngineEvent::Exit),
            ),
        ];
        let mut producer_frame = postcard::to_allocvec(&events.len()).unwrap();
        for event in &events {
            producer_frame.extend(postcard::to_allocvec(event).unwrap());
        }
        let canonical = postcard::to_allocvec(&events.as_slice()).unwrap();

        assert_eq!(producer_frame, canonical);
        let decoded: Vec<Event<DuckDbEvent>> = postcard::from_bytes(&producer_frame).unwrap();
        assert_eq!(decoded.len(), events.len());
        assert_eq!(decoded[0].timestamp, 10);
        assert_eq!(decoded[1].timestamp, 20);
    }
}
