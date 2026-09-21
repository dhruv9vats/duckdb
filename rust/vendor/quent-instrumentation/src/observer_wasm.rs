//! Synchronous callback forwarding for single-threaded WASM.

use quent_events::Event;
use quent_io::Exporter;
use std::future::Future;
use std::sync::{Arc, Mutex};
use tracing::warn;
use uuid::Uuid;

pub struct EventSender<T> {
    exporter: Option<Arc<Mutex<Box<dyn Exporter<T>>>>>,
}

impl<T> Clone for EventSender<T> {
    fn clone(&self) -> Self {
        Self {
            exporter: self.exporter.clone(),
        }
    }
}

impl<T> std::fmt::Debug for EventSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventSender").finish_non_exhaustive()
    }
}

impl<T> EventSender<T> {
    pub fn noop() -> Self {
        Self { exporter: None }
    }

    pub fn send(&self, event: Event<T>) {
        let Some(exporter) = &self.exporter else {
            return;
        };
        let Ok(mut exporter) = exporter.lock() else {
            warn!("browser instrumentation exporter is poisoned");
            return;
        };
        if let Err(error) = ready(exporter.push(event)) {
            warn!("browser instrumentation export failed: {error}");
        }
    }

    pub fn emit(&self, id: Uuid, event: impl Into<T>) {
        self.send(Event::new_now(id, event.into()));
    }
}

#[doc(hidden)]
pub struct ObserverInner<T> {
    events_sender: EventSender<T>,
}

impl<T> ObserverInner<T> {
    pub fn noop() -> Self {
        Self {
            events_sender: EventSender::noop(),
        }
    }

    pub(crate) fn direct(exporter: Box<dyn Exporter<T>>) -> Self {
        Self {
            events_sender: EventSender {
                exporter: Some(Arc::new(Mutex::new(exporter))),
            },
        }
    }

    pub fn send(&self, event: Event<T>) {
        self.events_sender.send(event);
    }

    pub fn emit(&self, id: Uuid, event: impl Into<T>) {
        self.events_sender.emit(id, event);
    }

    pub fn sender(&self) -> EventSender<T> {
        self.events_sender.clone()
    }
}

fn ready<F: Future>(future: F) -> F::Output {
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).poll(&mut context) {
        std::task::Poll::Ready(value) => value,
        std::task::Poll::Pending => panic!("browser callback exporter must not suspend"),
    }
}
