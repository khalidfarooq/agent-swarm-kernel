use ask_core::{AgentId, IoTask, MessageEnvelope};
use crossbeam_channel::{bounded, unbounded, Receiver, Sender, TrySendError};
use std::thread::{self, JoinHandle};

const DEFAULT_IO_QUEUE_CAPACITY: usize = 8192;

/// Non-blocking asynchronous I/O worker pool decoupled from pinned [`crate::ShardReactor`] threads.
///
/// Agents submit blocking or external I/O closures via [`ask_core::Context::spawn_io`].
/// Reactor threads dispatch them without blocking (`try_send`), worker threads execute them
/// off-core, and completion envelopes are pushed back to the requesting agent's shard inbox.
pub struct IoWorkerPool {
    task_tx: Option<Sender<IoTask>>,
    workers: Vec<JoinHandle<()>>,
}

impl IoWorkerPool {
    /// Creates an I/O worker pool with `num_workers` background threads and returns
    /// `(pool, per_shard_completion_receivers)`.
    pub fn new(num_shards: usize, num_workers: usize) -> (Self, Vec<Receiver<MessageEnvelope>>) {
        assert!(num_shards > 0, "num_shards must be > 0");
        let worker_count = num_workers.max(1);

        let (task_tx, task_rx) = bounded::<IoTask>(DEFAULT_IO_QUEUE_CAPACITY);

        let mut completion_txs = Vec::with_capacity(num_shards);
        let mut completion_rxs = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            let (tx, rx) = unbounded::<MessageEnvelope>();
            completion_txs.push(tx);
            completion_rxs.push(rx);
        }

        let mut workers = Vec::with_capacity(worker_count);
        for worker_idx in 0..worker_count {
            let rx = task_rx.clone();
            let shard_txs = completion_txs.clone();
            let handle = thread::Builder::new()
                .name(format!("ask-io-worker-{}", worker_idx))
                .spawn(move || {
                    let num_shards = shard_txs.len();
                    let io_kernel_id = AgentId::new(u16::MAX, worker_idx as u64);
                    while let Ok(task) = rx.recv() {
                        let payload = (task.work)();
                        let target_shard = (task.requester.shard_id() as usize) % num_shards;
                        let completion = MessageEnvelope::from_inline(
                            io_kernel_id,
                            task.requester,
                            task.correlation_id,
                            task.completion_kind,
                            payload,
                        );
                        let _ = shard_txs[target_shard].send(completion);
                    }
                })
                .expect("Failed to spawn ask-io-worker thread");
            workers.push(handle);
        }

        (
            Self {
                task_tx: Some(task_tx),
                workers,
            },
            completion_rxs,
        )
    }

    /// Returns a clonable non-blocking submission handle for reactor shards.
    pub fn submission_handle(&self) -> Option<Sender<IoTask>> {
        self.task_tx.clone()
    }

    /// Submits an [`IoTask`] without blocking the calling thread.
    pub fn submit(&self, task: IoTask) -> Result<(), TrySendError<IoTask>> {
        match &self.task_tx {
            Some(tx) => tx.try_send(task),
            None => Err(TrySendError::Disconnected(task)),
        }
    }

    /// Closes the task queue and joins all worker threads cleanly.
    pub fn shutdown(&mut self) {
        self.task_tx.take();
        for handle in self.workers.drain(..) {
            let _ = handle.join();
        }
    }
}

impl Drop for IoWorkerPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}
