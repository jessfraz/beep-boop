//! A connection owns each admitted host effect through its real result.

use std::{collections::HashMap, future::Future};

use anyhow::{Context, Result, ensure};
use tokio::{sync::watch, task::JoinSet};

use crate::protocol::HostResult;

pub enum Capacity {
    Unbounded,
    RegisteredWorker,
}

pub struct Reply {
    pub call_id: String,
    pub result: HostResult,
}

pub enum Admission {
    Started,
    Rejected(Reply),
}

#[must_use = "Host effects must be closed and drained before releasing their owner"]
pub struct HostEffects {
    tasks: JoinSet<Reply>,
    cancellations: HashMap<String, watch::Sender<bool>>,
    capacity: Capacity,
}

#[must_use = "Drain effect receipts before releasing the connection"]
pub struct Draining {
    effects: HostEffects,
}

impl HostEffects {
    pub fn new(capacity: Capacity) -> Self {
        Self {
            tasks: JoinSet::new(),
            cancellations: HashMap::new(),
            capacity,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn spawn<F, E>(&mut self, call_id: String, execute: E) -> Result<Admission>
    where
        F: Future<Output = HostResult> + Send + 'static,
        E: FnOnce(watch::Receiver<bool>) -> F,
    {
        // Check before replacing a sender or starting the effect. A duplicate
        // cannot safely receive a busy reply under another effect's identity.
        ensure!(
            !self.cancellations.contains_key(&call_id),
            "A host call ID is already running"
        );
        if matches!(self.capacity, Capacity::RegisteredWorker) && self.tasks.len() >= 32 {
            return Ok(Admission::Rejected(Reply {
                call_id,
                result: HostResult::Failed {
                    message: "The worker is busy; no command was started".into(),
                },
            }));
        }
        let (cancel, cancelled) = watch::channel(false);
        self.cancellations.insert(call_id.clone(), cancel);
        let future = execute(cancelled);
        self.tasks.spawn(async move {
            Reply {
                call_id,
                result: future.await,
            }
        });
        Ok(Admission::Started)
    }

    pub fn cancel(&self, call_id: &str) -> bool {
        match self.cancellations.get(call_id) {
            Some(cancel) => {
                let _ = cancel.send(true);
                true
            }
            None => false,
        }
    }

    pub async fn next(&mut self) -> Result<Option<Reply>> {
        match self.tasks.join_next().await {
            Some(completed) => {
                let reply = completed.context("Host execution task stopped")?;
                self.cancellations.remove(&reply.call_id);
                Ok(Some(reply))
            }
            None => Ok(None),
        }
    }

    /// Consume admission ownership and cancel processes. The draining owner
    /// yields actual write receipts even after another delivery has failed.
    pub fn shutdown(self) -> Draining {
        for cancel in self.cancellations.values() {
            let _ = cancel.send(true);
        }
        Draining { effects: self }
    }
}

impl Draining {
    pub async fn next(&mut self) -> Option<Result<Reply>> {
        match self.effects.next().await {
            Ok(Some(reply)) => Some(Ok(reply)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }
}

impl Drop for HostEffects {
    fn drop(&mut self) {
        for cancel in self.cancellations.values() {
            let _ = cancel.send(true);
        }
        // A lost connection can no longer deliver receipts, but must not abort
        // a write or its executor. Each detached task retains its real owner
        // until execution and process cleanup finish. Normal exits drain above.
        self.tasks.detach_all();
    }
}

#[cfg(test)]
mod tests;
