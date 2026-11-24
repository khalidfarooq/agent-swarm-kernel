pub mod affinity;
pub mod io_pool;

use ask_core::{
    monotonic_now_ns, Agent, Context, ExecutionResult, ExecutionStats, IoTask, MessageEnvelope,
};
use ask_ipc::{MeshNetwork, ShardEndpoint};
use crossbeam_channel::{Receiver, Sender};
use io_pool::IoWorkerPool;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread::{self, JoinHandle};
use std::time::Instant;

const DENSE_AGENT_LIMIT: usize = 1_048_576;
const TICK_BUDGET_PER_STEP: usize = 256;

/// Live point-in-time telemetry snapshot for a single shard reactor.
#[derive(Copy, Clone, Debug, Default)]
pub struct ShardSnapshot {
    pub core_id: usize,
    pub pinned: bool,
    pub messages_processed: u64,
    pub messages_dropped: u64,
    pub ticks_executed: u64,
    pub io_submitted: u64,
    pub io_completed: u64,
    pub ring_fill_bps: u64,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
}

#[derive(Default)]
struct SharedShardTelemetry {
    pinned: AtomicBool,
    messages_processed: AtomicU64,
    messages_dropped: AtomicU64,
    ticks_executed: AtomicU64,
    io_submitted: AtomicU64,
    io_completed: AtomicU64,
    ring_fill_bps: AtomicU64,
    p50_ns: AtomicU64,
    p99_ns: AtomicU64,
    max_ns: AtomicU64,
}

impl SharedShardTelemetry {
    fn snapshot(&self, core_id: usize) -> ShardSnapshot {
        ShardSnapshot {
            core_id,
            pinned: self.pinned.load(Ordering::Relaxed),
            messages_processed: self.messages_processed.load(Ordering::Relaxed),
            messages_dropped: self.messages_dropped.load(Ordering::Relaxed),
            ticks_executed: self.ticks_executed.load(Ordering::Relaxed),
            io_submitted: self.io_submitted.load(Ordering::Relaxed),
            io_completed: self.io_completed.load(Ordering::Relaxed),
            ring_fill_bps: self.ring_fill_bps.load(Ordering::Relaxed),
            p50_ns: self.p50_ns.load(Ordering::Relaxed),
            p99_ns: self.p99_ns.load(Ordering::Relaxed),
            max_ns: self.max_ns.load(Ordering::Relaxed),
        }
    }
}

/// Individual shard running exclusively on a core reactor thread.
pub struct ShardReactor {
    pub core_id: usize,
    pub num_cores: usize,
    endpoint: ShardEndpoint,
    dense_agents: Vec<Option<Box<dyn Agent>>>,
    dense_indices: Vec<usize>,
    sparse_agents: HashMap<u64, Box<dyn Agent>>,
    tick_cursor: usize,
    local_inbox: Vec<MessageEnvelope>,
    processing_buf: Vec<MessageEnvelope>,
    outgoing_buf: Vec<MessageEnvelope>,
    reusable_ctx: Context,
    io_tx: Option<Sender<IoTask>>,
    io_rx: Receiver<MessageEnvelope>,
    io_drops: u64,
    stats: ExecutionStats,
    telemetry: Arc<SharedShardTelemetry>,
    step_count: u64,
    hist_dirty: bool,
}

impl ShardReactor {
    pub fn new(core_id: usize, num_cores: usize, endpoint: ShardEndpoint) -> Self {
        let (_dummy_tx, dummy_rx) = crossbeam_channel::unbounded();
        Self::with_io(
            core_id,
            num_cores,
            endpoint,
            None,
            dummy_rx,
            Arc::new(SharedShardTelemetry::default()),
        )
    }

    fn with_io(
        core_id: usize,
        num_cores: usize,
        endpoint: ShardEndpoint,
        io_tx: Option<Sender<IoTask>>,
        io_rx: Receiver<MessageEnvelope>,
        telemetry: Arc<SharedShardTelemetry>,
    ) -> Self {
        let mut stats = ExecutionStats::default();
        stats.core_id = core_id;

        Self {
            core_id,
            num_cores,
            endpoint,
            dense_agents: Vec::new(),
            dense_indices: Vec::new(),
            sparse_agents: HashMap::new(),
            tick_cursor: 0,
            local_inbox: Vec::with_capacity(4096),
            processing_buf: Vec::with_capacity(4096),
            outgoing_buf: Vec::with_capacity(4096),
            reusable_ctx: Context::new(),
            io_tx,
            io_rx,
            io_drops: 0,
            stats,
            telemetry,
            step_count: 0,
            hist_dirty: false,
        }
    }

    #[inline]
    pub fn set_pinned(&mut self, pinned: bool) {
        self.stats.pinned = pinned;
        self.telemetry.pinned.store(pinned, Ordering::Relaxed);
    }

    #[inline]
    pub fn register_agent(&mut self, agent: Box<dyn Agent>) {
        let seq = agent.id().local_seq();
        if (seq as usize) < DENSE_AGENT_LIMIT {
            let idx = seq as usize;
            if idx >= self.dense_agents.len() {
                self.dense_agents.resize_with(idx + 1, || None);
            }
            if self.dense_agents[idx].is_none() {
                self.dense_indices.push(idx);
            }
            self.dense_agents[idx] = Some(agent);
        } else {
            self.sparse_agents.insert(seq, agent);
        }
    }

    #[inline]
    pub fn inject_message(&mut self, env: MessageEnvelope) {
        self.local_inbox.push(env);
    }

    #[inline(always)]
    fn dispatch_io_from_ctx(
        io_tx: &Option<Sender<IoTask>>,
        ctx: &mut Context,
        io_submitted: &mut u64,
        io_drops: &mut u64,
    ) {
        if ctx.outgoing_io.is_empty() {
            return;
        }
        for task in ctx.outgoing_io.drain(..) {
            if let Some(tx) = io_tx {
                if tx.try_send(task).is_ok() {
                    *io_submitted += 1;
                } else {
                    *io_drops += 1;
                }
            } else {
                *io_drops += 1;
            }
        }
    }

    #[inline(always)]
    fn route_outgoing(&mut self) {
        for out_msg in self.outgoing_buf.drain(..) {
            let dest_shard = (out_msg.receiver.shard_id() as usize) % self.num_cores;
            if dest_shard == self.core_id {
                self.local_inbox.push(out_msg);
            } else {
                let _ = self.endpoint.try_send(dest_shard, out_msg);
            }
        }
    }

    fn publish_telemetry(&mut self, force_percentiles: bool) {
        self.telemetry
            .messages_processed
            .store(self.stats.messages_processed, Ordering::Relaxed);
        self.telemetry
            .messages_dropped
            .store(self.stats.messages_dropped, Ordering::Relaxed);
        self.telemetry
            .ticks_executed
            .store(self.stats.ticks_executed, Ordering::Relaxed);
        self.telemetry
            .io_submitted
            .store(self.stats.io_submitted, Ordering::Relaxed);
        self.telemetry
            .io_completed
            .store(self.stats.io_completed, Ordering::Relaxed);
        self.telemetry
            .ring_fill_bps
            .store(self.endpoint.ring_utilization_bps(), Ordering::Relaxed);

        if self.hist_dirty && (force_percentiles || (self.step_count & 0x1F) == 0) {
            self.telemetry.p50_ns.store(
                self.stats.latency_histogram.percentile_ns(0.50),
                Ordering::Relaxed,
            );
            self.telemetry.p99_ns.store(
                self.stats.latency_histogram.percentile_ns(0.99),
                Ordering::Relaxed,
            );
            self.telemetry
                .max_ns
                .store(self.stats.latency_histogram.max_ns(), Ordering::Relaxed);
            self.hist_dirty = false;
        }
    }

    /// Single cooperative execution cycle for this core shard.
    /// Drains inter-core SPSC IPC and async I/O completions, then executes local agent handlers.
    pub fn step(&mut self) -> usize {
        let start = Instant::now();
        let mut processed = 0usize;
        self.step_count += 1;

        // 1. Drain incoming messages from peer cores via lock-free SPSC channels
        self.endpoint.drain_incoming(&mut self.local_inbox);

        // 2. Drain asynchronous I/O completions without blocking
        while let Ok(completion) = self.io_rx.try_recv() {
            self.local_inbox.push(completion);
            self.stats.io_completed += 1;
        }

        // 3. Process all pending messages with zero heap allocations on hot path
        std::mem::swap(&mut self.local_inbox, &mut self.processing_buf);
        if !self.processing_buf.is_empty() {
            for idx in 0..self.processing_buf.len() {
                let env = self.processing_buf[idx];
                let target_seq = env.receiver.local_seq();

                // Measure real end-to-end transit latency prior to dispatch
                let transit_ns = monotonic_now_ns().saturating_sub(env.timestamp_ns);

                let agent_opt = if (target_seq as usize) < self.dense_agents.len() {
                    self.dense_agents[target_seq as usize].as_mut()
                } else {
                    self.sparse_agents.get_mut(&target_seq)
                };

                if let Some(agent) = agent_opt {
                    self.stats.latency_histogram.record(transit_ns);
                    self.hist_dirty = true;
                    self.reusable_ctx.clear();
                    let res = agent.handle_message(env, &mut self.reusable_ctx);
                    processed += 1;

                    if res == ExecutionResult::Terminate {
                        // Lifecycle state preserved inside agent; removal handled if needed
                    }

                    if !self.reusable_ctx.outgoing_messages.is_empty() {
                        self.outgoing_buf
                            .extend(self.reusable_ctx.outgoing_messages.drain(..));
                    }
                    Self::dispatch_io_from_ctx(
                        &self.io_tx,
                        &mut self.reusable_ctx,
                        &mut self.stats.io_submitted,
                        &mut self.io_drops,
                    );
                }
            }
            self.processing_buf.clear();
            self.route_outgoing();
        }

        // 4. Cooperative round-robin tick loop with bounded per-step gas budget
        let dense_len = self.dense_indices.len();
        if dense_len > 0 {
            let budget = dense_len.min(TICK_BUDGET_PER_STEP);
            for _ in 0..budget {
                let idx = self.dense_indices[self.tick_cursor];
                self.tick_cursor += 1;
                if self.tick_cursor >= dense_len {
                    self.tick_cursor = 0;
                }
                if let Some(agent) = self.dense_agents[idx].as_mut() {
                    self.reusable_ctx.clear();
                    let _ = agent.tick(&mut self.reusable_ctx);
                    self.stats.ticks_executed += 1;
                    if !self.reusable_ctx.outgoing_messages.is_empty() {
                        self.outgoing_buf
                            .extend(self.reusable_ctx.outgoing_messages.drain(..));
                    }
                    Self::dispatch_io_from_ctx(
                        &self.io_tx,
                        &mut self.reusable_ctx,
                        &mut self.stats.io_submitted,
                        &mut self.io_drops,
                    );
                }
            }
        }
        if !self.sparse_agents.is_empty() {
            for agent in self.sparse_agents.values_mut() {
                self.reusable_ctx.clear();
                let _ = agent.tick(&mut self.reusable_ctx);
                self.stats.ticks_executed += 1;
                if !self.reusable_ctx.outgoing_messages.is_empty() {
                    self.outgoing_buf
                        .extend(self.reusable_ctx.outgoing_messages.drain(..));
                }
                Self::dispatch_io_from_ctx(
                    &self.io_tx,
                    &mut self.reusable_ctx,
                    &mut self.stats.io_submitted,
                    &mut self.io_drops,
                );
            }
        }
        if !self.outgoing_buf.is_empty() {
            self.route_outgoing();
        }

        self.stats.messages_processed += processed as u64;
        self.stats.messages_dropped = self.endpoint.dropped_count() + self.io_drops;
        self.stats.nanos_elapsed += start.elapsed().as_nanos() as u64;

        self.publish_telemetry(processed == 0);
        processed
    }

    pub fn stats(&self) -> ExecutionStats {
        self.stats
    }

    pub fn agent_count(&self) -> usize {
        self.dense_indices.len() + self.sparse_agents.len()
    }
}

/// Multi-threaded Kernel orchestrating all physical core shards and an async I/O worker pool.
pub struct Kernel {
    num_cores: usize,
    shards: Vec<Option<ShardReactor>>,
    telemetry: Vec<Arc<SharedShardTelemetry>>,
    io_pool: Option<IoWorkerPool>,
}

impl Kernel {
    pub fn new(num_cores: usize, ring_capacity: usize) -> Self {
        let io_workers = num_cores.clamp(2, 4);
        Self::with_io_workers(num_cores, ring_capacity, io_workers)
    }

    pub fn with_io_workers(num_cores: usize, ring_capacity: usize, io_workers: usize) -> Self {
        let endpoints = MeshNetwork::create_mesh(num_cores, ring_capacity);
        let (io_pool, io_rxs) = IoWorkerPool::new(num_cores, io_workers);
        let io_tx = io_pool.submission_handle();

        let mut shards = Vec::with_capacity(num_cores);
        let mut telemetry = Vec::with_capacity(num_cores);

        for ((i, ep), io_rx) in endpoints.into_iter().enumerate().zip(io_rxs.into_iter()) {
            let shared = Arc::new(SharedShardTelemetry::default());
            telemetry.push(Arc::clone(&shared));
            shards.push(Some(ShardReactor::with_io(
                i,
                num_cores,
                ep,
                io_tx.clone(),
                io_rx,
                shared,
            )));
        }

        Self {
            num_cores,
            shards,
            telemetry,
            io_pool: Some(io_pool),
        }
    }

    /// Distributes an agent to its designated shard based on its `AgentId` shard_id.
    pub fn register_agent(&mut self, agent: Box<dyn Agent>) {
        let shard_id = (agent.id().shard_id() as usize) % self.num_cores;
        if let Some(shard) = &mut self.shards[shard_id] {
            shard.register_agent(agent);
        }
    }

    /// Inject an initial message directly into a shard.
    pub fn inject_message(&mut self, env: MessageEnvelope) {
        let shard_id = (env.receiver.shard_id() as usize) % self.num_cores;
        if let Some(shard) = &mut self.shards[shard_id] {
            shard.inject_message(env);
        }
    }

    /// Run the swarm kernel synchronously for one cooperative cycle across all shards.
    pub fn step_all(&mut self) -> usize {
        let mut total = 0;
        for shard_opt in &mut self.shards {
            if let Some(shard) = shard_opt {
                total += shard.step();
            }
        }
        total
    }

    /// Returns a live telemetry snapshot across all shards.
    pub fn snapshot(&self) -> Vec<ShardSnapshot> {
        self.telemetry
            .iter()
            .enumerate()
            .map(|(id, t)| t.snapshot(id))
            .collect()
    }

    /// Spawns pinned worker threads for each core shard and returns a controller handle.
    pub fn start(mut self) -> (KernelHandle, Vec<JoinHandle<ExecutionStats>>) {
        let running = Arc::new(AtomicBool::new(true));
        let total_processed = Arc::new(AtomicU64::new(0));
        let pinned_cores = Arc::new(AtomicUsize::new(0));
        let ready_barrier = Arc::new(Barrier::new(self.num_cores + 1));

        let mut handles = Vec::with_capacity(self.num_cores);

        for (core_id, mut shard_opt) in self.shards.into_iter().enumerate() {
            let is_running = Arc::clone(&running);
            let global_counter = Arc::clone(&total_processed);
            let pinned_counter = Arc::clone(&pinned_cores);
            let barrier = Arc::clone(&ready_barrier);

            let handle = thread::Builder::new()
                .name(format!("ask-reactor-{}", core_id))
                .spawn(move || {
                    let mut shard = shard_opt.take().expect("Shard must be present");
                    let pinned = affinity::pin_current_thread(core_id);
                    shard.set_pinned(pinned);
                    if pinned {
                        pinned_counter.fetch_add(1, Ordering::Relaxed);
                    }
                    barrier.wait();

                    while is_running.load(Ordering::Relaxed) {
                        let count = shard.step();
                        if count > 0 {
                            global_counter.fetch_add(count as u64, Ordering::Relaxed);
                        } else {
                            std::thread::yield_now();
                        }
                    }

                    // Drain any final telemetry state before returning
                    shard.publish_telemetry(true);
                    shard.stats()
                })
                .expect("Failed to spawn reactor thread");

            handles.push(handle);
        }

        // Wait until all reactor threads have completed their OS core-pinning call.
        ready_barrier.wait();

        let controller = KernelHandle {
            running,
            total_processed,
            pinned_cores,
            telemetry: self.telemetry,
            io_pool: Mutex::new(self.io_pool.take()),
        };

        (controller, handles)
    }
}

pub struct KernelHandle {
    running: Arc<AtomicBool>,
    total_processed: Arc<AtomicU64>,
    pinned_cores: Arc<AtomicUsize>,
    telemetry: Vec<Arc<SharedShardTelemetry>>,
    io_pool: Mutex<Option<IoWorkerPool>>,
}

impl KernelHandle {
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Stops reactor threads and shuts down the asynchronous I/O worker pool.
    pub fn shutdown(&self) {
        self.stop();
        if let Some(mut pool) = self.io_pool.lock().take() {
            pool.shutdown();
        }
    }

    pub fn total_processed(&self) -> u64 {
        self.total_processed.load(Ordering::Relaxed)
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    pub fn pinned_cores(&self) -> usize {
        self.pinned_cores.load(Ordering::Acquire)
    }

    pub fn num_cores(&self) -> usize {
        self.telemetry.len()
    }

    /// Samples live atomic counters and latency quantiles across all core shards.
    pub fn snapshot(&self) -> Vec<ShardSnapshot> {
        self.telemetry
            .iter()
            .enumerate()
            .map(|(core_id, t)| t.snapshot(core_id))
            .collect()
    }
}

impl Drop for KernelHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ask_core::{AgentId, AgentState, InlinePayload, MessageKind};
    use std::time::Duration;

    struct PingPongAgent {
        id: AgentId,
        partner_id: AgentId,
        pings_received: u64,
        max_pings: u64,
    }

    impl Agent for PingPongAgent {
        fn id(&self) -> AgentId {
            self.id
        }

        fn state(&self) -> AgentState {
            AgentState::Active
        }

        fn handle_message(&mut self, env: MessageEnvelope, ctx: &mut Context) -> ExecutionResult {
            self.pings_received += 1;
            if self.pings_received < self.max_pings {
                ctx.send(MessageEnvelope::new(
                    self.id,
                    self.partner_id,
                    env.seq + 1,
                    MessageKind::Thought,
                    b"pong",
                ));
            }
            ExecutionResult::Ok
        }

        fn tick(&mut self, _ctx: &mut Context) -> ExecutionResult {
            ExecutionResult::Ok
        }
    }

    #[test]
    fn test_cross_shard_ping_pong() {
        let mut kernel = Kernel::new(2, 1024);

        let a1 = AgentId::new(0, 1);
        let a2 = AgentId::new(1, 1);

        kernel.register_agent(Box::new(PingPongAgent {
            id: a1,
            partner_id: a2,
            pings_received: 0,
            max_pings: 50,
        }));

        kernel.register_agent(Box::new(PingPongAgent {
            id: a2,
            partner_id: a1,
            pings_received: 0,
            max_pings: 50,
        }));

        kernel.inject_message(MessageEnvelope::new(
            a1,
            a2,
            0,
            MessageKind::Thought,
            b"ping",
        ));

        let mut total_processed = 0;
        for _ in 0..200 {
            total_processed += kernel.step_all();
            if total_processed >= 98 {
                break;
            }
        }

        assert!(
            total_processed >= 50,
            "Should have processed at least 50 ping-pong rounds"
        );
    }

    struct SlowIoAgent {
        id: AgentId,
        io_completed_count: Arc<AtomicU64>,
        io_payload_matched: Arc<AtomicBool>,
    }

    impl Agent for SlowIoAgent {
        fn id(&self) -> AgentId {
            self.id
        }

        fn state(&self) -> AgentState {
            AgentState::Active
        }

        fn handle_message(&mut self, env: MessageEnvelope, ctx: &mut Context) -> ExecutionResult {
            if env.kind == MessageKind::Task {
                let requester = self.id;
                ctx.spawn_io(requester, 777, MessageKind::Signal, || {
                    thread::sleep(Duration::from_millis(80));
                    InlinePayload::from("io_complete_ok")
                });
            } else if env.kind == MessageKind::Signal && env.seq == 777 {
                if env.payload() == b"io_complete_ok" {
                    self.io_payload_matched.store(true, Ordering::SeqCst);
                }
                self.io_completed_count.fetch_add(1, Ordering::SeqCst);
            }
            ExecutionResult::Ok
        }

        fn tick(&mut self, _ctx: &mut Context) -> ExecutionResult {
            ExecutionResult::Ok
        }
    }

    struct FastRelayAgent {
        id: AgentId,
        partner_id: AgentId,
        msgs_while_io_inflight: Arc<AtomicU64>,
        io_done_flag: Arc<AtomicU64>,
    }

    impl Agent for FastRelayAgent {
        fn id(&self) -> AgentId {
            self.id
        }

        fn state(&self) -> AgentState {
            AgentState::Active
        }

        fn handle_message(&mut self, env: MessageEnvelope, ctx: &mut Context) -> ExecutionResult {
            if self.io_done_flag.load(Ordering::Relaxed) == 0 {
                self.msgs_while_io_inflight.fetch_add(1, Ordering::Relaxed);
            }
            ctx.send(MessageEnvelope::new(
                self.id,
                self.partner_id,
                env.seq + 1,
                MessageKind::Thought,
                b"fast",
            ));
            ExecutionResult::Ok
        }

        fn tick(&mut self, _ctx: &mut Context) -> ExecutionResult {
            ExecutionResult::Ok
        }
    }

    #[test]
    fn test_async_io_does_not_stall_shard_throughput() {
        let mut kernel = Kernel::with_io_workers(2, 1024, 2);

        let io_completed = Arc::new(AtomicU64::new(0));
        let io_payload_matched = Arc::new(AtomicBool::new(false));
        let inflight_msgs = Arc::new(AtomicU64::new(0));

        // Put SlowIoAgent AND both FastRelayAgents on Shard 0 to prove Shard 0 never stalls.
        let io_agent = AgentId::new(0, 0);
        let fast_a = AgentId::new(0, 1);
        let fast_b = AgentId::new(0, 2);

        kernel.register_agent(Box::new(SlowIoAgent {
            id: io_agent,
            io_completed_count: Arc::clone(&io_completed),
            io_payload_matched: Arc::clone(&io_payload_matched),
        }));
        kernel.register_agent(Box::new(FastRelayAgent {
            id: fast_a,
            partner_id: fast_b,
            msgs_while_io_inflight: Arc::clone(&inflight_msgs),
            io_done_flag: Arc::clone(&io_completed),
        }));
        kernel.register_agent(Box::new(FastRelayAgent {
            id: fast_b,
            partner_id: fast_a,
            msgs_while_io_inflight: Arc::clone(&inflight_msgs),
            io_done_flag: Arc::clone(&io_completed),
        }));

        // Trigger slow 80ms I/O task + start fast intra-shard relay
        kernel.inject_message(MessageEnvelope::new(
            io_agent,
            io_agent,
            1,
            MessageKind::Task,
            b"fetch",
        ));
        kernel.inject_message(MessageEnvelope::new(
            fast_a,
            fast_b,
            0,
            MessageKind::Thought,
            b"go",
        ));

        let (controller, handles) = kernel.start();

        // Wait up to 1s for the 80ms async I/O completion to arrive back on Shard 0
        let deadline = Instant::now() + Duration::from_secs(1);
        while io_completed.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }

        controller.shutdown();

        let mut total_io_submitted = 0u64;
        let mut total_io_completed = 0u64;
        let mut total_hist_count = 0u64;
        for h in handles {
            let stats = h.join().expect("reactor thread should join cleanly");
            total_io_submitted += stats.io_submitted;
            total_io_completed += stats.io_completed;
            total_hist_count += stats.latency_histogram.count();
        }

        assert_eq!(io_completed.load(Ordering::SeqCst), 1);
        assert!(io_payload_matched.load(Ordering::SeqCst));
        assert_eq!(total_io_submitted, 1);
        assert_eq!(total_io_completed, 1);
        assert!(total_hist_count > 1000);
        let during_io = inflight_msgs.load(Ordering::Relaxed);
        assert!(
            during_io > 1000,
            "Expected thousands of fast shard messages while 80ms I/O was in flight, got {}",
            during_io
        );
    }
}
