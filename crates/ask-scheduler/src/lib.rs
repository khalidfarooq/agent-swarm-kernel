use ask_core::{Agent, Context, ExecutionResult, ExecutionStats, MessageEnvelope};
use ask_ipc::{MeshNetwork, ShardEndpoint};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Instant;

/// Individual shard running exclusively on a pinned physical core.
pub struct ShardReactor {
    pub core_id: usize,
    pub num_cores: usize,
    endpoint: ShardEndpoint,
    agents: HashMap<u64, Box<dyn Agent>>,
    local_inbox: Vec<MessageEnvelope>,
    stats: ExecutionStats,
}

impl ShardReactor {
    pub fn new(core_id: usize, num_cores: usize, endpoint: ShardEndpoint) -> Self {
        Self {
            core_id,
            num_cores,
            endpoint,
            agents: HashMap::new(),
            local_inbox: Vec::with_capacity(4096),
            stats: ExecutionStats::default(),
        }
    }

    #[inline]
    pub fn register_agent(&mut self, agent: Box<dyn Agent>) {
        let seq = agent.id().local_seq();
        self.agents.insert(seq, agent);
    }

    #[inline]
    pub fn inject_message(&mut self, env: MessageEnvelope) {
        self.local_inbox.push(env);
    }

    /// Single cooperative execution cycle for this core shard.
    /// Drains inter-core IPC and executes local agent message handlers.
    pub fn step(&mut self) -> usize {
        let start = Instant::now();
        let mut processed = 0;

        // 1. Drain incoming messages from peer cores via lock-free SPSC channels
        self.endpoint.drain_incoming(&mut self.local_inbox);

        // 2. Process all pending messages
        if !self.local_inbox.is_empty() {
            let messages = std::mem::take(&mut self.local_inbox);
            let mut outgoing = Vec::new();

            for env in messages {
                let target_seq = env.receiver.local_seq();
                if let Some(agent) = self.agents.get_mut(&target_seq) {
                    let mut ctx = Context::new();
                    let res = agent.handle_message(env, &mut ctx);
                    processed += 1;

                    if res == ExecutionResult::Terminate {
                        // Mark for removal if needed
                    }

                    // Collect outgoing messages
                    outgoing.extend(ctx.outgoing_messages);
                }
            }

            // 3. Route outgoing messages
            for out_msg in outgoing {
                let dest_shard = out_msg.receiver.shard_id() as usize;
                if dest_shard == self.core_id {
                    // Local delivery (zero serialization, same thread!)
                    self.local_inbox.push(out_msg);
                } else if dest_shard < self.num_cores {
                    // Lock-free SPSC transfer to target core
                    let _ = self.endpoint.try_send(dest_shard, out_msg);
                }
            }
        }

        // 4. Run periodic ticks on active agents
        let mut outgoing = Vec::new();
        for agent in self.agents.values_mut() {
            let mut ctx = Context::new();
            agent.tick(&mut ctx);
            outgoing.extend(ctx.outgoing_messages);
            self.stats.ticks_executed += 1;
        }

        for out_msg in outgoing {
            let dest_shard = out_msg.receiver.shard_id() as usize;
            if dest_shard == self.core_id {
                self.local_inbox.push(out_msg);
            } else if dest_shard < self.num_cores {
                let _ = self.endpoint.try_send(dest_shard, out_msg);
            }
        }

        self.stats.messages_processed += processed as u64;
        self.stats.nanos_elapsed += start.elapsed().as_nanos() as u64;
        processed
    }

    pub fn stats(&self) -> ExecutionStats {
        self.stats
    }

    pub fn agent_count(&self) -> usize {
        self.agents.len()
    }
}

/// Multi-threaded Kernel orchestrating all physical core shards.
pub struct Kernel {
    num_cores: usize,
    shards: Vec<Option<ShardReactor>>,
}

impl Kernel {
    pub fn new(num_cores: usize, ring_capacity: usize) -> Self {
        let endpoints = MeshNetwork::create_mesh(num_cores, ring_capacity);
        let mut shards = Vec::with_capacity(num_cores);

        for (i, ep) in endpoints.into_iter().enumerate() {
            shards.push(Some(ShardReactor::new(i, num_cores, ep)));
        }

        Self {
            num_cores,
            shards,
        }
    }

    /// Distributes an agent to its designated shard based on its AgentId shard_id.
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

    /// Run the swarm kernel synchronously for a specified number of total ticks across all shards.
    pub fn step_all(&mut self) -> usize {
        let mut total = 0;
        for shard_opt in &mut self.shards {
            if let Some(shard) = shard_opt {
                total += shard.step();
            }
        }
        total
    }

    /// Spawns background worker threads for each core shard and returns a controller handle.
    pub fn start(self) -> (KernelHandle, Vec<JoinHandle<ExecutionStats>>) {
        let running = Arc::new(AtomicBool::new(true));
        let total_processed = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::with_capacity(self.num_cores);

        for (core_id, mut shard_opt) in self.shards.into_iter().enumerate() {
            let is_running = Arc::clone(&running);
            let global_counter = Arc::clone(&total_processed);

            let handle = thread::Builder::new()
                .name(format!("ask-reactor-{}", core_id))
                .spawn(move || {
                    let mut shard = shard_opt.take().expect("Shard must be present");
                    while is_running.load(Ordering::Relaxed) {
                        let count = shard.step();
                        if count > 0 {
                            global_counter.fetch_add(count as u64, Ordering::Relaxed);
                        } else {
                            // Micro-yield to prevent burning 100% CPU when idle
                            std::thread::yield_now();
                        }
                    }
                    shard.stats()
                })
                .expect("Failed to spawn reactor thread");

            handles.push(handle);
        }

        let controller = KernelHandle {
            running,
            total_processed,
        };

        (controller, handles)
    }
}

pub struct KernelHandle {
    running: Arc<AtomicBool>,
    total_processed: Arc<AtomicU64>,
}

impl KernelHandle {
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    pub fn total_processed(&self) -> u64 {
        self.total_processed.load(Ordering::Relaxed)
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ask_core::{AgentId, AgentState, MessageKind};

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
                    b"pong".to_vec(),
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

        // Kick off with initial ping from a1 to a2
        kernel.inject_message(MessageEnvelope::new(
            a1,
            a2,
            0,
            MessageKind::Thought,
            b"ping".to_vec(),
        ));

        let mut total_processed = 0;
        for _ in 0..200 {
            total_processed += kernel.step_all();
            if total_processed >= 98 {
                break;
            }
        }

        assert!(total_processed >= 50, "Should have processed at least 50 ping-pong rounds");
    }
}
