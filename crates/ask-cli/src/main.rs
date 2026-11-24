use ask_core::{
    monotonic_now_ns, Agent, AgentId, AgentState, Context, ExecutionResult, LatencyHistogram,
    MessageEnvelope, MessageKind,
};
use ask_scheduler::Kernel;
use clap::{Parser, Subcommand, ValueEnum};
use colored::*;
use indicatif::{ProgressBar, ProgressStyle};
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const MESH_NEIGHBORS: usize = 8;
const GOSSIP_FANOUT: usize = 3;
const GOSSIP_TTL: u8 = 10;
const GOSSIP_WAVE_INTERVAL_NS: u64 = 2_000_000; // 2ms between epidemic waves

static MONITOR_STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn handle_sigint(_sig: libc::c_int) {
    MONITOR_STOP.store(true, Ordering::SeqCst);
}

#[derive(Parser)]
#[command(name = "ask")]
#[command(about = "Agent Swarm Kernel: High-Throughput Thread-per-Core Runtime for AI Agent Swarms", long_about = None)]
#[command(version = "0.1.0")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum Topology {
    Ring,
    Mesh,
    Gossip,
}

impl std::fmt::Display for Topology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Topology::Ring => write!(f, "RING"),
            Topology::Mesh => write!(f, "MESH"),
            Topology::Gossip => write!(f, "GOSSIP"),
        }
    }
}

#[derive(Subcommand)]
enum Commands {
    /// High-velocity stress test measuring IPC throughput, transit latency quantiles, and RSS
    Bench {
        /// Total number of concurrent agents to simulate
        #[arg(short, long, default_value_t = 100_000)]
        agents: usize,

        /// Number of core reactor threads to allocate
        #[arg(short, long, default_value_t = 4)]
        cores: usize,

        /// Duration of the benchmark run in seconds
        #[arg(short, long, default_value_t = 3)]
        duration: u64,
    },

    /// Run an agent swarm topology simulation (ring, mesh, or gossip)
    Run {
        /// Number of agents in the swarm
        #[arg(short, long, default_value_t = 10_000)]
        agents: usize,

        /// Swarm communication topology: ring, mesh, or gossip
        #[arg(short, long, value_enum, default_value_t = Topology::Ring)]
        topology: Topology,

        /// Simulation duration in seconds
        #[arg(short, long, default_value_t = 5)]
        duration: u64,

        /// Number of core reactor threads to allocate
        #[arg(short, long, default_value_t = 4)]
        cores: usize,
    },

    /// Real-time terminal monitor of live kernel reactors
    Monitor {
        /// Number of core reactors to run and monitor
        #[arg(short, long, default_value_t = 4)]
        cores: usize,

        /// Number of concurrent agents generating live cross-core traffic
        #[arg(short, long, default_value_t = 10_000)]
        agents: usize,

        /// Optional run duration in seconds (runs until Ctrl+C on TTY if omitted)
        #[arg(short, long)]
        seconds: Option<u64>,
    },
}

#[inline(always)]
fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    if x == 0 {
        x = 0x9E37_79B9_7F4A_7C15;
    }
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

#[inline(always)]
fn agent_id_for_index(idx: usize, cores: usize) -> AgentId {
    let shard_id = (idx % cores) as u16;
    let local_seq = (idx / cores) as u64;
    AgentId::new(shard_id, local_seq)
}

/// Returns the current process resident set size (peak RSS) in bytes using `libc::getrusage`.
fn current_max_rss_bytes() -> u64 {
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) == 0 {
            let raw = usage.ru_maxrss.max(0) as u64;
            #[cfg(target_os = "macos")]
            {
                raw
            }
            #[cfg(target_os = "linux")]
            {
                raw.saturating_mul(1024)
            }
            #[cfg(not(any(target_os = "macos", target_os = "linux")))]
            {
                raw.saturating_mul(1024)
            }
        } else {
            0
        }
    }
}

fn format_ns(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.2} ms", (ns as f64) / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.2} µs", (ns as f64) / 1_000.0)
    } else {
        format!("{} ns", ns)
    }
}

/// High-performance ring relay agent.
struct RelayAgent {
    id: AgentId,
    next_id: AgentId,
    counter: Arc<AtomicU64>,
}

impl Agent for RelayAgent {
    fn id(&self) -> AgentId {
        self.id
    }

    fn state(&self) -> AgentState {
        AgentState::Active
    }

    fn handle_message(&mut self, env: MessageEnvelope, ctx: &mut Context) -> ExecutionResult {
        self.counter.fetch_add(1, Ordering::Relaxed);
        ctx.send(MessageEnvelope::new(
            self.id,
            self.next_id,
            env.seq + 1,
            MessageKind::Thought,
            b"stream",
        ));
        ExecutionResult::Ok
    }

    fn tick(&mut self, _ctx: &mut Context) -> ExecutionResult {
        ExecutionResult::Ok
    }
}

/// Multi-core mesh agent routing across a hashed neighbor pool using fast `xorshift64`.
struct MeshAgent {
    id: AgentId,
    neighbors: [AgentId; MESH_NEIGHBORS],
    neighbor_count: usize,
    rng_state: u64,
    counter: Arc<AtomicU64>,
}

impl MeshAgent {
    fn new(idx: usize, total_agents: usize, cores: usize, counter: Arc<AtomicU64>) -> Self {
        let id = agent_id_for_index(idx, cores);
        let mut rng = ((idx as u64) + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut neighbors = [id; MESH_NEIGHBORS];
        let count = total_agents.saturating_sub(1).min(MESH_NEIGHBORS);

        for slot in 0..count {
            // Spread neighbor hops across shards and distances
            let jump = (xorshift64(&mut rng) as usize) % total_agents.max(1);
            let mut target = (idx + 1 + jump + slot * cores) % total_agents;
            if target == idx && total_agents > 1 {
                target = (idx + 1 + slot) % total_agents;
            }
            neighbors[slot] = agent_id_for_index(target, cores);
        }

        Self {
            id,
            neighbors,
            neighbor_count: count,
            rng_state: rng ^ 0xD1B5_4A32_D192_ED03,
            counter,
        }
    }
}

impl Agent for MeshAgent {
    fn id(&self) -> AgentId {
        self.id
    }

    fn state(&self) -> AgentState {
        AgentState::Active
    }

    fn handle_message(&mut self, env: MessageEnvelope, ctx: &mut Context) -> ExecutionResult {
        self.counter.fetch_add(1, Ordering::Relaxed);
        if self.neighbor_count > 0 {
            let pick = (xorshift64(&mut self.rng_state) as usize) % self.neighbor_count;
            let next_id = self.neighbors[pick];
            ctx.send(MessageEnvelope::new(
                self.id,
                next_id,
                env.seq + 1,
                MessageKind::Task,
                b"mesh_hop",
            ));
        }
        ExecutionResult::Ok
    }

    fn tick(&mut self, _ctx: &mut Context) -> ExecutionResult {
        ExecutionResult::Ok
    }
}

/// Epidemic bounded-fanout + TTL gossip agent using non-allocating `xorshift64`.
struct GossipAgent {
    id: AgentId,
    total_agents: usize,
    cores: usize,
    rng_state: u64,
    last_seen_wave: u32,
    is_originator: bool,
    current_wave: u32,
    next_wave_ns: u64,
    counter: Arc<AtomicU64>,
}

impl GossipAgent {
    fn new(idx: usize, total_agents: usize, cores: usize, counter: Arc<AtomicU64>) -> Self {
        let seed = ((idx as u64) + 1)
            .wrapping_mul(0xBF58_476D_1CE4_E5B9)
            .wrapping_add(0x94D0_49BB_1331_11EB);
        Self {
            id: agent_id_for_index(idx, cores),
            total_agents,
            cores,
            rng_state: seed,
            last_seen_wave: 0,
            is_originator: idx == 0,
            current_wave: 0,
            next_wave_ns: 0,
            counter,
        }
    }

    #[inline]
    fn encode_gossip(wave_id: u32, ttl: u8) -> [u8; 5] {
        let w = wave_id.to_le_bytes();
        [w[0], w[1], w[2], w[3], ttl]
    }

    #[inline]
    fn decode_gossip(bytes: &[u8]) -> Option<(u32, u8)> {
        if bytes.len() < 5 {
            return None;
        }
        let wave_id = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let ttl = bytes[4];
        Some((wave_id, ttl))
    }

    #[inline]
    fn fanout_send(&mut self, wave_id: u32, ttl: u8, seq: u64, ctx: &mut Context) {
        if self.total_agents <= 1 {
            return;
        }
        let payload = Self::encode_gossip(wave_id, ttl);
        let fanout = GOSSIP_FANOUT.min(self.total_agents - 1);
        for _ in 0..fanout {
            let target_idx = (xorshift64(&mut self.rng_state) as usize) % self.total_agents;
            let dest = agent_id_for_index(target_idx, self.cores);
            if dest != self.id {
                ctx.send(MessageEnvelope::new(
                    self.id,
                    dest,
                    seq,
                    MessageKind::Signal,
                    &payload,
                ));
            }
        }
    }
}

impl Agent for GossipAgent {
    fn id(&self) -> AgentId {
        self.id
    }

    fn state(&self) -> AgentState {
        AgentState::Active
    }

    fn handle_message(&mut self, env: MessageEnvelope, ctx: &mut Context) -> ExecutionResult {
        self.counter.fetch_add(1, Ordering::Relaxed);
        if let Some((wave_id, ttl)) = Self::decode_gossip(env.payload()) {
            if wave_id > self.last_seen_wave {
                self.last_seen_wave = wave_id;
                if ttl > 0 {
                    self.fanout_send(wave_id, ttl - 1, env.seq + 1, ctx);
                }
            }
        }
        ExecutionResult::Ok
    }

    fn tick(&mut self, ctx: &mut Context) -> ExecutionResult {
        if self.is_originator {
            let now = monotonic_now_ns();
            if now >= self.next_wave_ns {
                self.current_wave = self.current_wave.wrapping_add(1).max(1);
                self.last_seen_wave = self.current_wave;
                self.next_wave_ns = now.saturating_add(GOSSIP_WAVE_INTERVAL_NS);
                self.fanout_send(self.current_wave, GOSSIP_TTL, self.current_wave as u64, ctx);
            }
        }
        ExecutionResult::Ok
    }
}

fn print_banner() {
    println!("{}", "       _   ____ _____ _   _ _____   ______      ___    ____  __  __   _  _______ ____  _   _ _____ _     ".cyan());
    println!("{}", "      / \\ / ___| ____| \\ | |_   _| / ___\\ \\    / / \\  |  _ \\|  \\/  | | |/ / ____|  _ \\| \\ | | ____| |    ".cyan());
    println!("{}", "     / _ \\ |  _|  _| |  \\| | | |   \\___ \\\\ \\  / / _ \\ | |_) | |\\/| | | ' /|  _| | |_) |  \\| |  _| | |    ".cyan());
    println!("{}", "    / ___ \\ |_| | |__| |\\  | | |    ___) |\\ \\/ / ___ \\|  _ <| |  | | | . \\| |___|  _ <| |\\  | |___| |___ ".cyan());
    println!("{}", "   /_/   \\_\\____|_____|_| \\_| |_|   |____/  \\_/_/   \\_\\_| \\_\\_|  |_| |_|\\_\\_____|_| \\_\\_| \\_|_____|_____|".cyan());
    println!(
        "\n  {} {}",
        "Agent Swarm Kernel (`ask`)".bold().white(),
        "v0.1.0 — Thread-per-Core User-Space Microkernel".green()
    );
    println!(
        "  {}\n",
        "OS Core Pinning · Zero-Alloc Inline Envelopes · Lock-Free SPSC IPC · Async I/O Pool"
            .bright_black()
    );
}

fn run_benchmark(agents: usize, cores: usize, duration_secs: u64) {
    let cores = cores.max(1);
    let agents = agents.max(1);
    print_banner();

    println!(
        "{} Initializing kernel with {} core reactors for {} concurrent agents...",
        "⚙".cyan(),
        cores.to_string().yellow().bold(),
        agents.to_string().yellow().bold()
    );

    let ring_capacity = 65536;
    let mut kernel = Kernel::new(cores, ring_capacity);
    let counter = Arc::new(AtomicU64::new(0));

    let rss_before = current_max_rss_bytes();

    let pb = ProgressBar::new(agents as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} agents registered",
            )
            .unwrap()
            .progress_chars("#>-"),
    );

    for i in 0..agents {
        let agent_id = agent_id_for_index(i, cores);
        let next_id = agent_id_for_index((i + 1) % agents, cores);

        kernel.register_agent(Box::new(RelayAgent {
            id: agent_id,
            next_id,
            counter: Arc::clone(&counter),
        }));

        if i % 1000 == 0 {
            pb.set_position(i as u64);
        }
    }
    pb.finish_with_message("All agents registered in memory");

    let rss_after = current_max_rss_bytes();
    let rss_delta = rss_after.saturating_sub(rss_before);
    let agent_struct_bytes = std::mem::size_of::<RelayAgent>();
    let envelope_bytes = std::mem::size_of::<MessageEnvelope>();

    let seeds = (cores * 128).min(agents);
    println!(
        "{} Injecting {} seed messages into the SPSC mesh...",
        "⚡".yellow(),
        seeds.to_string().bold()
    );

    for s in 0..seeds {
        let target_idx = (s * (agents / seeds).max(1)) % agents;
        kernel.inject_message(MessageEnvelope::new(
            AgentId::new(0, 0),
            agent_id_for_index(target_idx, cores),
            s as u64,
            MessageKind::Thought,
            b"init_spark",
        ));
    }

    println!(
        "{} Launching core reactors and running benchmark for {}s...\n",
        "▶".green(),
        duration_secs
    );

    let start = Instant::now();
    let (controller, handles) = kernel.start();
    let pinned_cores = controller.pinned_cores();

    let run_pb = ProgressBar::new(duration_secs);
    run_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.cyan} Running [{bar:40.green/black}] {pos}/{len}s | Msg Count: {msg}")
            .unwrap()
            .progress_chars("━╾─"),
    );

    for _ in 0..duration_secs {
        thread::sleep(Duration::from_secs(1));
        run_pb.set_message(format!(
            "{:>10}",
            counter.load(Ordering::Relaxed).to_string().yellow().bold()
        ));
        run_pb.inc(1);
    }
    run_pb.finish_and_clear();

    controller.shutdown();

    let mut total_processed = 0u64;
    let mut total_dropped = 0u64;
    let mut merged_hist = LatencyHistogram::new();
    for handle in handles {
        if let Ok(stats) = handle.join() {
            total_processed += stats.messages_processed;
            total_dropped += stats.messages_dropped;
            merged_hist.merge(&stats.latency_histogram);
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let total_msgs = total_processed.max(counter.load(Ordering::Relaxed));
    let msgs_per_sec = (total_msgs as f64) / elapsed.max(0.001);
    let p50_ns = merged_hist.percentile_ns(0.50);
    let p99_ns = merged_hist.percentile_ns(0.99);
    let max_ns = merged_hist.max_ns();
    let rss_mb = (rss_after as f64) / (1024.0 * 1024.0);
    let rss_delta_mb = (rss_delta as f64) / (1024.0 * 1024.0);
    let rss_per_agent = (rss_delta as f64) / (agents as f64);

    println!(
        "\n{}",
        "══════════════════════════════════════════════════════════════════════".cyan()
    );
    println!("  {}", "AGENT SWARM KERNEL BENCHMARK TELEMETRY".bold().white());
    println!(
        "{}",
        "══════════════════════════════════════════════════════════════════════".cyan()
    );
    println!(
        "  {:<34} {:>32}",
        "Active Concurrent Agents:",
        agents.to_string().bold().green()
    );
    println!(
        "  {:<34} {:>32}",
        "Core Reactors (OS-Pinned):",
        format!("{} ({} pinned)", cores, pinned_cores)
            .bold()
            .white()
    );
    println!(
        "  {:<34} {:>32}",
        "Benchmark Duration:",
        format!("{:.2} s", elapsed).bold()
    );
    println!(
        "  {:<34} {:>32}",
        "Total Messages Delivered:",
        total_msgs.to_string().bold().cyan()
    );
    println!(
        "  {:<34} {:>32}",
        "SPSC QueueFull Drops:",
        total_dropped.to_string().bold().yellow()
    );
    println!(
        "  {:<34} {:>32}",
        "Measured Throughput:",
        format!("{:.2} ops/sec", msgs_per_sec).bold().yellow()
    );
    println!(
        "  {:<34} {:>32}",
        "Transit Latency (p50 / p99 / max):",
        format!("{} / {} / {}", format_ns(p50_ns), format_ns(p99_ns), format_ns(max_ns))
            .bold()
            .green()
    );
    println!(
        "  {:<34} {:>32}",
        "Struct Size (Agent / Envelope):",
        format!("{} B / {} B", agent_struct_bytes, envelope_bytes)
            .bold()
            .white()
    );
    println!(
        "  {:<34} {:>32}",
        "Measured RSS Delta (Peak RSS):",
        format!(
            "+{:.2} MB (~{:.1} B/agent) [Peak {:.2} MB]",
            rss_delta_mb, rss_per_agent, rss_mb
        )
        .bold()
        .green()
    );
    println!(
        "{}\n",
        "══════════════════════════════════════════════════════════════════════".cyan()
    );
}

fn run_simulation(agents: usize, topology: Topology, duration_secs: u64, cores: usize) {
    let cores = cores.max(1);
    let agents = agents.max(1);
    print_banner();
    println!(
        "{} Running {} agents across {} cores in {} topology for {}s...",
        "🌐".cyan(),
        agents.to_string().yellow().bold(),
        cores.to_string().yellow().bold(),
        topology.to_string().green().bold(),
        duration_secs
    );

    let mut kernel = Kernel::new(cores, 16384);
    let counter = Arc::new(AtomicU64::new(0));

    match topology {
        Topology::Ring => {
            for i in 0..agents {
                kernel.register_agent(Box::new(RelayAgent {
                    id: agent_id_for_index(i, cores),
                    next_id: agent_id_for_index((i + 1) % agents, cores),
                    counter: Arc::clone(&counter),
                }));
            }
            let seeds = (cores * 16).min(agents);
            for s in 0..seeds {
                let idx = (s * (agents / seeds).max(1)) % agents;
                kernel.inject_message(MessageEnvelope::new(
                    AgentId::new(0, 0),
                    agent_id_for_index(idx, cores),
                    s as u64,
                    MessageKind::Task,
                    b"ring_seed",
                ));
            }
        }
        Topology::Mesh => {
            for i in 0..agents {
                kernel.register_agent(Box::new(MeshAgent::new(
                    i,
                    agents,
                    cores,
                    Arc::clone(&counter),
                )));
            }
            let seeds = (cores * 32).min(agents);
            for s in 0..seeds {
                let idx = (s * (agents / seeds).max(1)) % agents;
                kernel.inject_message(MessageEnvelope::new(
                    AgentId::new(0, 0),
                    agent_id_for_index(idx, cores),
                    s as u64,
                    MessageKind::Task,
                    b"mesh_seed",
                ));
            }
        }
        Topology::Gossip => {
            for i in 0..agents {
                kernel.register_agent(Box::new(GossipAgent::new(
                    i,
                    agents,
                    cores,
                    Arc::clone(&counter),
                )));
            }
            let wave1 = GossipAgent::encode_gossip(1, GOSSIP_TTL);
            kernel.inject_message(MessageEnvelope::new(
                AgentId::new(0, 0),
                agent_id_for_index(0, cores),
                1,
                MessageKind::Signal,
                &wave1,
            ));
        }
    }

    let start = Instant::now();
    let (controller, handles) = kernel.start();
    let pinned_cores = controller.pinned_cores();

    thread::sleep(Duration::from_secs(duration_secs));
    controller.shutdown();

    let elapsed = start.elapsed().as_secs_f64();
    let mut delivered = 0u64;
    let mut dropped = 0u64;
    let mut merged_hist = LatencyHistogram::new();
    for h in handles {
        if let Ok(stats) = h.join() {
            delivered += stats.messages_processed;
            dropped += stats.messages_dropped;
            merged_hist.merge(&stats.latency_histogram);
        }
    }

    let total_attempted = delivered + dropped;
    let delivery_pct = if total_attempted > 0 {
        (delivered as f64 / total_attempted as f64) * 100.0
    } else {
        100.0
    };
    let throughput = (delivered as f64) / elapsed.max(0.001);

    println!(
        "\n{}",
        "──────────────────────────────────────────────────────────────────────".bright_black()
    );
    println!(
        "  {} Topology: {} | Cores: {} ({} OS-pinned) | Elapsed: {:.2}s",
        "✓".green().bold(),
        topology.to_string().cyan().bold(),
        cores,
        pinned_cores,
        elapsed
    );
    println!(
        "  Delivered: {} | Dropped (QueueFull): {} | Delivery Rate: {:.2}%",
        delivered.to_string().yellow().bold(),
        dropped.to_string().red().bold(),
        delivery_pct
    );
    println!(
        "  Throughput: {:.2} ops/sec | Transit Latency p50: {} | p99: {} | max: {}",
        throughput,
        format_ns(merged_hist.percentile_ns(0.50)).green(),
        format_ns(merged_hist.percentile_ns(0.99)).cyan(),
        format_ns(merged_hist.max_ns())
    );
    println!(
        "{}\n",
        "──────────────────────────────────────────────────────────────────────".bright_black()
    );
}

fn run_monitor(cores: usize, agents: usize, seconds: Option<u64>) {
    let cores = cores.max(1);
    let agents = agents.max(cores);
    MONITOR_STOP.store(false, Ordering::SeqCst);

    unsafe {
        libc::signal(libc::SIGINT, handle_sigint as libc::sighandler_t);
    }

    let is_tty = unsafe { libc::isatty(libc::STDOUT_FILENO) != 0 };
    let effective_limit = match seconds {
        Some(s) => Some(Duration::from_secs(s)),
        None if is_tty => None,
        None => Some(Duration::from_secs(2)),
    };

    let mut kernel = Kernel::new(cores, 65536);
    let counter = Arc::new(AtomicU64::new(0));
    for i in 0..agents {
        kernel.register_agent(Box::new(RelayAgent {
            id: agent_id_for_index(i, cores),
            next_id: agent_id_for_index((i + 1) % agents, cores),
            counter: Arc::clone(&counter),
        }));
    }

    let seeds = (cores * 64).min(agents);
    for s in 0..seeds {
        let idx = (s * (agents / seeds).max(1)) % agents;
        kernel.inject_message(MessageEnvelope::new(
            AgentId::new(0, 0),
            agent_id_for_index(idx, cores),
            s as u64,
            MessageKind::Thought,
            b"mon_seed",
        ));
    }

    let (controller, handles) = kernel.start();
    let pinned_cores = controller.pinned_cores();
    let mut prev_snap = controller.snapshot();
    let mut prev_time = Instant::now();
    let monitor_start = prev_time;
    let mut first_frame = true;

    if !is_tty {
        print_banner();
        println!(
            "{} Live Kernel Monitor ({} cores, {} OS-pinned, {} agents, 250ms sampling)...",
            "🖥".cyan(),
            cores,
            pinned_cores,
            agents
        );
    }

    loop {
        thread::sleep(Duration::from_millis(250));
        let now = Instant::now();
        let dt = now.duration_since(prev_time).as_secs_f64().max(0.001);
        let snap = controller.snapshot();

        let mut total_ops_sec = 0.0f64;
        let mut total_msgs = 0u64;
        let mut total_drops = 0u64;
        let mut max_p50 = 0u64;
        let mut max_p99 = 0u64;

        let mut out = String::new();
        if is_tty {
            if first_frame {
                out.push_str("\x1B[2J\x1B[H");
                first_frame = false;
            } else {
                out.push_str("\x1B[H\x1B[J");
            }
            out.push_str(&format!(
                "  {}  {} | Cores: {} ({} OS-pinned) | Agents: {} | Elapsed: {:.1}s\n",
                "ASK LIVE KERNEL MONITOR".bold().cyan(),
                "(Ctrl+C to exit)".bright_black(),
                cores,
                pinned_cores,
                agents,
                now.duration_since(monitor_start).as_secs_f64()
            ));
            out.push_str(
                "────────────────────────────────────────────────────────────────────────────────────────\n",
            );
        } else {
            out.push_str(&format!(
                "── Sample @ {:.2}s ───────────────────────────────────────────────────────────────────\n",
                now.duration_since(monitor_start).as_secs_f64()
            ));
        }

        for (cur, prev) in snap.iter().zip(prev_snap.iter()) {
            let d_msgs = cur.messages_processed.saturating_sub(prev.messages_processed);
            let d_ticks = cur.ticks_executed.saturating_sub(prev.ticks_executed);
            let ops_sec = (d_msgs as f64) / dt;
            let ticks_sec = (d_ticks as f64) / dt;
            let ring_pct = (cur.ring_fill_bps as f64) / 100.0;

            total_ops_sec += ops_sec;
            total_msgs += cur.messages_processed;
            total_drops += cur.messages_dropped;
            max_p50 = max_p50.max(cur.p50_ns);
            max_p99 = max_p99.max(cur.p99_ns);

            let pin_tag = if cur.pinned { "PINNED" } else { "SHARED" };
            out.push_str(&format!(
                "  [Core {:>2}] {:<6} | Ring: {:>5.2}% | Ops/s: {:>10.0} | Ticks/s: {:>8.0} | Drops: {:>4} | p50: {:>7} | p99: {:>7}\n",
                cur.core_id,
                pin_tag,
                ring_pct,
                ops_sec,
                ticks_sec,
                cur.messages_dropped,
                format_ns(cur.p50_ns),
                format_ns(cur.p99_ns)
            ));
        }

        out.push_str(
            "────────────────────────────────────────────────────────────────────────────────────────\n",
        );
        out.push_str(&format!(
            "  Aggregated Throughput: {:>12.0} ops/sec | Total Delivered: {} | Drops: {} | p50/p99: {}/{}\n",
            total_ops_sec,
            total_msgs,
            total_drops,
            format_ns(max_p50),
            format_ns(max_p99)
        ));

        print!("{}", out);
        let _ = io::stdout().flush();

        prev_snap = snap;
        prev_time = now;

        if MONITOR_STOP.load(Ordering::SeqCst) {
            break;
        }
        if let Some(limit) = effective_limit {
            if now.duration_since(monitor_start) >= limit {
                break;
            }
        }
    }

    controller.shutdown();
    for h in handles {
        let _ = h.join();
    }
    println!("\n{} Kernel monitor stopped cleanly.", "✓".green().bold());
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Bench {
            agents,
            cores,
            duration,
        } => {
            run_benchmark(agents, cores, duration);
        }
        Commands::Run {
            agents,
            topology,
            duration,
            cores,
        } => {
            run_simulation(agents, topology, duration, cores);
        }
        Commands::Monitor {
            cores,
            agents,
            seconds,
        } => {
            run_monitor(cores, agents, seconds);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xorshift64_nonzero_distribution() {
        let mut s = 0u64;
        let a = xorshift64(&mut s);
        let b = xorshift64(&mut s);
        assert_ne!(a, 0);
        assert_ne!(b, 0);
        assert_ne!(a, b);
    }

    #[test]
    fn test_gossip_codec_roundtrip() {
        let encoded = GossipAgent::encode_gossip(42, 7);
        assert_eq!(GossipAgent::decode_gossip(&encoded), Some((42, 7)));
        assert_eq!(GossipAgent::decode_gossip(&[1, 2]), None);
    }

    #[test]
    fn test_gossip_deduplication_and_ttl() {
        let counter = Arc::new(AtomicU64::new(0));
        let mut agent = GossipAgent::new(1, 32, 4, Arc::clone(&counter));
        let mut ctx = Context::new();

        let msg1 = MessageEnvelope::new(
            AgentId::new(0, 0),
            agent.id(),
            1,
            MessageKind::Signal,
            &GossipAgent::encode_gossip(5, 3),
        );
        agent.handle_message(msg1, &mut ctx);
        assert!(!ctx.outgoing_messages.is_empty());
        for env in &ctx.outgoing_messages {
            let (wave, ttl) = GossipAgent::decode_gossip(env.payload()).unwrap();
            assert_eq!(wave, 5);
            assert_eq!(ttl, 2);
        }

        // Duplicate wave should not re-broadcast
        ctx.clear();
        agent.handle_message(msg1, &mut ctx);
        assert!(ctx.outgoing_messages.is_empty());
    }

    #[test]
    fn test_rss_measurement_nonzero() {
        assert!(current_max_rss_bytes() > 0);
    }
}
