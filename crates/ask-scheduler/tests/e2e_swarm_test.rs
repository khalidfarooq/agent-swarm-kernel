use ask_core::{Agent, AgentId, AgentState, Context, ExecutionResult, MessageEnvelope, MessageKind};
use ask_scheduler::Kernel;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

struct TestWorkerAgent {
    id: AgentId,
    target_id: AgentId,
    received: Arc<AtomicU64>,
}

impl Agent for TestWorkerAgent {
    fn id(&self) -> AgentId {
        self.id
    }

    fn state(&self) -> AgentState {
        AgentState::Active
    }

    fn handle_message(&mut self, env: MessageEnvelope, ctx: &mut Context) -> ExecutionResult {
        self.received.fetch_add(1, Ordering::Relaxed);
        if env.seq < 100 {
            ctx.send(MessageEnvelope::new(
                self.id,
                self.target_id,
                env.seq + 1,
                MessageKind::Thought,
                b"step".to_vec(),
            ));
        }
        ExecutionResult::Ok
    }

    fn tick(&mut self, _ctx: &mut Context) -> ExecutionResult {
        ExecutionResult::Ok
    }
}

#[test]
fn test_e2e_multi_core_swarm_execution() {
    let cores = 4;
    let mut kernel = Kernel::new(cores, 4096);
    let counter = Arc::new(AtomicU64::new(0));

    // Register 100 agents distributed across 4 cores
    for i in 0..100 {
        let shard_id = (i % cores) as u16;
        let local_seq = (i / cores) as u64;
        let next_i = (i + 1) % 100;
        let next_shard = (next_i % cores) as u16;
        let next_local = (next_i / cores) as u64;

        kernel.register_agent(Box::new(TestWorkerAgent {
            id: AgentId::new(shard_id, local_seq),
            target_id: AgentId::new(next_shard, next_local),
            received: Arc::clone(&counter),
        }));
    }

    // Seed 4 initial messages
    for c in 0..cores {
        kernel.inject_message(MessageEnvelope::new(
            AgentId::new(0, 0),
            AgentId::new(c as u16, 0),
            0,
            MessageKind::Thought,
            b"init".to_vec(),
        ));
    }

    let (controller, handles) = kernel.start();
    thread::sleep(Duration::from_millis(50));
    controller.stop();

    for h in handles {
        let stats = h.join().expect("Thread should join");
        println!("Core processed {} messages", stats.messages_processed);
    }

    let total = counter.load(Ordering::Relaxed);
    println!("Total messages processed in e2e test: {}", total);
    assert!(total > 0, "Swarm should have processed messages across cores");
}
