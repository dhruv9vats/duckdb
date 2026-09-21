//! Browser-only capture backing for the generated C++ facade.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};

use duckdb_telemetry_model::{
    Context as ModelContext, DuckDb, DuckDbEvent, Event, QueryEvent, Uuid,
};
use quent_instrumentation::EventCallback;

use crate::bridge::context::Context;
use crate::bridge::context::ffi::BrowserBatch;

const COUNT_PREFIX_BYTES: usize = std::mem::size_of::<usize>() + 1;

struct BufferedEvent {
    encoded: Vec<u8>,
    timestamp: u64,
    oversize_reported: bool,
}

impl BufferedEvent {
    fn storage_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(self.encoded.len())
    }
}

struct Capture {
    events: VecDeque<BufferedEvent>,
    bytes: usize,
    limit: usize,
    dropped: u64,
    failed: bool,
    overflowed: bool,
    has_started_run: bool,
    query_ids: Vec<Uuid>,
    watermark: u64,
}

impl Capture {
    fn begin_run(&mut self) -> bool {
        if self.failed || self.overflowed || (self.has_started_run && !self.events.is_empty()) {
            return false;
        }
        if self.has_started_run {
            self.query_ids.clear();
        }
        self.has_started_run = true;
        self.watermark = self.watermark.max(quent_time::timestamp());
        true
    }

    fn push(&mut self, event: Event<DuckDbEvent>) {
        self.watermark = self.watermark.max(event.timestamp);
        if self.failed || self.overflowed {
            self.dropped = self.dropped.saturating_add(1);
            return;
        }
        let Ok(encoded) = postcard::to_allocvec(&event) else {
            self.dropped = self.dropped.saturating_add(1);
            self.failed = true;
            return;
        };
        let buffered = BufferedEvent {
            encoded,
            timestamp: event.timestamp,
            oversize_reported: false,
        };
        let Some(bytes) = self.bytes.checked_add(buffered.storage_bytes()) else {
            self.dropped = self.dropped.saturating_add(1);
            self.overflowed = true;
            return;
        };
        if bytes > self.limit {
            self.dropped = self.dropped.saturating_add(1);
            self.overflowed = true;
            return;
        }
        if matches!(&event.data, DuckDbEvent::Query(QueryEvent::Init { .. }))
            && !self.query_ids.contains(&event.id)
        {
            self.query_ids.push(event.id);
        }
        self.bytes = bytes;
        self.events.push_back(buffered);
    }

    fn drain(&mut self, max_bytes: usize) -> BrowserBatch {
        let mut count = 0_usize;
        let mut encoded_bytes = 0_usize;
        for event in &self.events {
            let candidate_count = count.saturating_add(1);
            let mut prefix = [0_u8; COUNT_PREFIX_BYTES];
            let Ok(prefix) = postcard::to_slice(&candidate_count, &mut prefix) else {
                self.failed = true;
                break;
            };
            let candidate_bytes = prefix
                .len()
                .saturating_add(encoded_bytes)
                .saturating_add(event.encoded.len());
            if candidate_bytes > max_bytes {
                break;
            }
            count = candidate_count;
            encoded_bytes = encoded_bytes.saturating_add(event.encoded.len());
        }

        if count == 0 {
            if let Some(event) = self.events.front_mut() {
                if !event.oversize_reported {
                    event.oversize_reported = true;
                    self.dropped = self.dropped.saturating_add(1);
                }
                self.failed = true;
            }
            return BrowserBatch {
                payload: Vec::new(),
                event_count: 0,
                min_timestamp: 0,
                max_timestamp: 0,
                failed: self.failed,
            };
        }

        let mut prefix = [0_u8; COUNT_PREFIX_BYTES];
        let prefix = match postcard::to_slice(&count, &mut prefix) {
            Ok(prefix) => prefix,
            Err(_) => {
                self.failed = true;
                return failed_batch();
            }
        };
        let mut payload = Vec::with_capacity(prefix.len().saturating_add(encoded_bytes));
        payload.extend_from_slice(prefix);
        let mut min_timestamp = u64::MAX;
        let mut max_timestamp = 0;
        for _ in 0..count {
            let event = self.events.pop_front().expect("selected event exists");
            self.bytes = self.bytes.saturating_sub(event.storage_bytes());
            min_timestamp = min_timestamp.min(event.timestamp);
            max_timestamp = max_timestamp.max(event.timestamp);
            payload.extend_from_slice(&event.encoded);
        }

        BrowserBatch {
            payload,
            event_count: count as u32,
            min_timestamp,
            max_timestamp,
            failed: self.failed,
        }
    }
}

type Captures = HashMap<Uuid, std::sync::Weak<Mutex<Capture>>>;

fn captures() -> &'static Mutex<Captures> {
    static CAPTURES: OnceLock<Mutex<Captures>> = OnceLock::new();
    CAPTURES.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn create_browser_context(max_bytes: u64) -> Result<Box<Context>, String> {
    let limit = usize::try_from(max_bytes).map_err(|_| "browser capture limit is too large")?;
    let capture = Arc::new(Mutex::new(Capture {
        events: VecDeque::new(),
        bytes: 0,
        limit,
        dropped: 0,
        failed: false,
        overflowed: false,
        has_started_run: false,
        query_ids: Vec::new(),
        watermark: quent_time::timestamp(),
    }));
    let callback = {
        let capture = Arc::clone(&capture);
        EventCallback::<DuckDbEvent>::new(move |event| {
            if let Ok(mut capture) = capture.lock() {
                capture.push(event);
            }
        })
    };
    let inner = ModelContext::<DuckDb>::try_new(callback).map_err(|error| error.to_string())?;
    let id = inner.id();
    let mut registry = captures()
        .lock()
        .map_err(|_| "browser capture registry is poisoned")?;
    registry.retain(|_, capture| capture.strong_count() != 0);
    registry.insert(id, Arc::downgrade(&capture));
    Ok(Box::new(Context { inner }))
}

fn drain_capture(context: &Context, max_bytes: u64) -> BrowserBatch {
    let Ok(max_bytes) = usize::try_from(max_bytes) else {
        return failed_batch();
    };
    let Ok(mut captures) = captures().lock() else {
        return failed_batch();
    };
    let id = context.inner.id();
    let Some(capture) = captures.get(&id).and_then(std::sync::Weak::upgrade) else {
        captures.remove(&id);
        return failed_batch();
    };
    capture
        .lock()
        .map(|mut capture| capture.drain(max_bytes))
        .unwrap_or_else(|_| failed_batch())
}

fn dropped_capture(context: &Context) -> u64 {
    let Ok(mut captures) = captures().lock() else {
        return 0;
    };
    let id = context.inner.id();
    let Some(capture) = captures.get(&id).and_then(std::sync::Weak::upgrade) else {
        captures.remove(&id);
        return 0;
    };
    capture.lock().map(|capture| capture.dropped).unwrap_or(0)
}

fn query_ids_capture(context: &Context) -> Vec<String> {
    let Ok(mut captures) = captures().lock() else {
        return Vec::new();
    };
    let id = context.inner.id();
    let Some(capture) = captures.get(&id).and_then(std::sync::Weak::upgrade) else {
        captures.remove(&id);
        return Vec::new();
    };
    capture
        .lock()
        .map(|capture| capture.query_ids.iter().map(Uuid::to_string).collect())
        .unwrap_or_default()
}

fn watermark_capture(context: &Context) -> u64 {
    let Ok(mut captures) = captures().lock() else {
        return 0;
    };
    let id = context.inner.id();
    let Some(capture) = captures.get(&id).and_then(std::sync::Weak::upgrade) else {
        captures.remove(&id);
        return 0;
    };
    capture
        .lock()
        .map(|mut capture| {
            capture.watermark = capture.watermark.max(quent_time::timestamp());
            capture.watermark
        })
        .unwrap_or(0)
}

fn context_id_capture(context: &Context) -> String {
    context.inner.id().to_string()
}

fn begin_run_capture(context: &Context) -> bool {
    let Ok(mut captures) = captures().lock() else {
        return false;
    };
    let id = context.inner.id();
    let Some(capture) = captures.get(&id).and_then(std::sync::Weak::upgrade) else {
        captures.remove(&id);
        return false;
    };
    if let Ok(mut capture) = capture.lock() {
        return capture.begin_run();
    }

    false
}

fn failed_batch() -> BrowserBatch {
    BrowserBatch {
        payload: Vec::new(),
        event_count: 0,
        min_timestamp: 0,
        max_timestamp: 0,
        failed: true,
    }
}

impl Context {
    pub fn browser_drain(&self, max_bytes: u64) -> BrowserBatch {
        drain_capture(self, max_bytes)
    }

    pub fn browser_dropped(&self) -> u64 {
        dropped_capture(self)
    }

    pub fn browser_query_ids(&self) -> Vec<String> {
        query_ids_capture(self)
    }

    pub fn browser_watermark(&self) -> u64 {
        watermark_capture(self)
    }

    pub fn browser_context_id(&self) -> String {
        context_id_capture(self)
    }

    pub fn browser_begin_run(&self) -> bool {
        begin_run_capture(self)
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        if let Ok(mut captures) = captures().lock() {
            captures.remove(&self.inner.id());
        }
    }
}

#[cfg(test)]
mod tests {
    use duckdb_telemetry_model::EngineEvent;

    use super::*;

    fn event(timestamp: u64) -> Event<DuckDbEvent> {
        Event::new(Uuid::nil(), timestamp, EngineEvent::Exit.into())
    }

    #[test]
    fn batches_are_bounded_and_decode_as_model_events() {
        let first = postcard::to_allocvec(&event(10)).unwrap();
        let stored_event_bytes = first.len() + std::mem::size_of::<BufferedEvent>();
        let mut capture = Capture {
            events: VecDeque::new(),
            bytes: 0,
            limit: stored_event_bytes * 2,
            dropped: 0,
            failed: false,
            overflowed: false,
            has_started_run: false,
            query_ids: Vec::new(),
            watermark: 0,
        };
        capture.push(event(10));
        capture.push(event(20));
        capture.push(event(30));
        capture.push(event(40));
        assert_eq!(capture.dropped, 2);
        assert_eq!(capture.bytes, stored_event_bytes * 2);

        let batch = capture.drain(usize::MAX);
        let events: Vec<Event<DuckDbEvent>> = postcard::from_bytes(&batch.payload).unwrap();
        assert_eq!(batch.event_count, 2);
        assert_eq!(events.len(), 2);
        assert_eq!((batch.min_timestamp, batch.max_timestamp), (10, 20));
    }

    #[test]
    fn oversized_event_fails_without_exceeding_batch() {
        let mut capture = Capture {
            events: VecDeque::new(),
            bytes: 0,
            limit: usize::MAX,
            dropped: 0,
            failed: false,
            overflowed: false,
            has_started_run: false,
            query_ids: Vec::new(),
            watermark: 0,
        };
        capture.push(event(10));
        let batch = capture.drain(1);
        assert_eq!(batch.event_count, 0);
        assert!(batch.payload.len() <= 1);
        assert!(batch.failed);
        assert_eq!(capture.events.len(), 1);
        assert_eq!(capture.dropped, 1);
    }

    #[test]
    fn batch_limit_includes_vector_prefix() {
        let encoded = postcard::to_allocvec(&event(10)).unwrap();
        let prefix = postcard::to_allocvec(&2_usize).unwrap();
        let max_bytes = prefix.len() + encoded.len() * 2;
        let mut capture = Capture {
            events: VecDeque::new(),
            bytes: 0,
            limit: usize::MAX,
            dropped: 0,
            failed: false,
            overflowed: false,
            has_started_run: false,
            query_ids: Vec::new(),
            watermark: 0,
        };
        capture.push(event(10));
        capture.push(event(20));
        capture.push(event(30));

        let batch = capture.drain(max_bytes);
        assert!(batch.payload.len() <= max_bytes);
        assert_eq!(batch.event_count, 2);
        assert_eq!(capture.events.len(), 1);
        let events: Vec<Event<DuckDbEvent>> = postcard::from_bytes(&batch.payload).unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn prefix_growth_splits_at_128_events() {
        let encoded = postcard::to_allocvec(&event(10)).unwrap();
        let mut prefix = [0_u8; COUNT_PREFIX_BYTES];
        let prefix_127 = postcard::to_slice(&127_usize, &mut prefix).unwrap().len();
        let max_bytes = prefix_127 + encoded.len() * 127;
        let mut capture = Capture {
            events: VecDeque::new(),
            bytes: 0,
            limit: usize::MAX,
            dropped: 0,
            failed: false,
            overflowed: false,
            has_started_run: false,
            query_ids: Vec::new(),
            watermark: 0,
        };
        for _ in 0..128 {
            capture.push(event(10));
        }

        let batch = capture.drain(max_bytes);
        assert_eq!(batch.event_count, 127);
        assert_eq!(batch.payload.len(), max_bytes);
        assert_eq!(capture.events.len(), 1);
        let events: Vec<Event<DuckDbEvent>> = postcard::from_bytes(&batch.payload).unwrap();
        assert_eq!(events.len(), 127);
    }

    #[test]
    fn completed_overflow_stays_failed() {
        let encoded = postcard::to_allocvec(&event(10)).unwrap();
        let mut capture = Capture {
            events: VecDeque::new(),
            bytes: 0,
            limit: std::mem::size_of::<BufferedEvent>() + encoded.len(),
            dropped: 0,
            failed: false,
            overflowed: false,
            has_started_run: false,
            query_ids: Vec::new(),
            watermark: 0,
        };
        capture.push(event(10));
        capture.push(event(20));
        assert!(capture.overflowed);
        let batch = capture.drain(usize::MAX);
        assert_eq!(batch.event_count, 1);

        assert!(!capture.begin_run());
        capture.push(event(30));
        assert!(capture.events.is_empty());
        assert_eq!(capture.dropped, 2);
        assert!(capture.overflowed);
    }

    #[test]
    fn first_run_keeps_startup_events() {
        let startup_query_id = Uuid::from_u128(1);
        let mut capture = Capture {
            events: VecDeque::new(),
            bytes: 0,
            limit: usize::MAX,
            dropped: 0,
            failed: false,
            overflowed: false,
            has_started_run: false,
            query_ids: vec![startup_query_id],
            watermark: 0,
        };
        capture.push(event(10));
        capture.begin_run();
        capture.push(event(20));

        let batch = capture.drain(usize::MAX);
        assert_eq!(batch.event_count, 2);
        assert!(!batch.failed);
        assert_eq!(capture.query_ids, vec![startup_query_id]);
    }

    #[test]
    fn later_run_clears_query_ids() {
        let mut capture = Capture {
            events: VecDeque::new(),
            bytes: 0,
            limit: usize::MAX,
            dropped: 0,
            failed: false,
            overflowed: false,
            has_started_run: true,
            query_ids: vec![Uuid::from_u128(1)],
            watermark: 0,
        };

        assert!(capture.begin_run());
        assert!(capture.query_ids.is_empty());
    }

    #[cfg(target_arch = "wasm32")]
    #[test]
    fn callback_exports_before_context_drop() {
        use duckdb_telemetry_model::{Engine, EngineImplementation};

        let context = create_browser_context(1024 * 1024).unwrap();
        let mut engine = context.inner.observer::<Engine>().handle();
        engine
            .init(
                EngineImplementation {
                    name: None,
                    version: None,
                    custom_attributes: Default::default(),
                },
                None,
            )
            .unwrap();

        let batch = drain_capture(&context, 1024 * 1024);
        assert_eq!(batch.event_count, 1);
    }
}
