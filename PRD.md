# 🧠 Agent Swarm Kernel (`ask`)
## Systems Architecture & Product Requirements Document

```text
       _   ____ _____ _   _ _____   ______      ___    ____  __  __   _  _______ ____  _   _ _____ _     
      / \ / ___| ____| \ | |_   _| / ___\ \    / / \  |  _ \|  \/  | | |/ / ____|  _ \| \ | | ____| |    
     / _ \ |  _|  _| |  \| | | |   \___ \\ \  / / _ \ | |_) | |\/| | | ' /|  _| | |_) |  \| |  _| | |    
    / ___ \ |_| | |__| |\  | | |    ___) |\ \/ / ___ \|  _ <| |  | | | . \| |___|  _ <| |\  | |___| |___ 
   /_/   \_\____|_____|_| \_| |_|   |____/  \_/_/   \_\_| \_\_|  |_| |_|\_\_____|_| \_\_| \_|_____|_____|
```

> **The High-Throughput User-Space Microkernel & Lock-Free Thread-per-Core Runtime for Autonomous AI Agent Swarms**  
> *Engineered in Rust. Microsecond-deterministic scheduling. 10M+ message ops/sec. Zero-copy shared memory IPC.*

---

## 1. Executive Summary & Vision

Current multi-agent frameworks (LangGraph, CrewAI, AutoGen) treat autonomous agents as high-level Python objects glued together with `asyncio`, JSON serialization over HTTP/WebSocket, and centralized SQLite/PostgreSQL locking. 

As a result:
- **Framework overhead:** 1,100ms – 4,100ms wasted *per step* in coordination alone.
- **Scaling ceiling:** Frameworks choke and crash when scaling past 200–500 concurrent agents due to the Python GIL and event loop starvation.
- **Resource bloat:** Gigabytes of RAM wasted on duplicate object state and serialization buffers.

**Agent Swarm Kernel (`ask`)** brings high-frequency trading (HFT) and microkernel operating system principles to AI agent orchestration:
1. **Thread-per-Core Sharded Reactor:** Shared-nothing architecture pinning one reactor per physical CPU core, eliminating lock contention.
2. **SPSC Lock-Free Ring Buffers:** Inter-core agent communication via cache-line padded Single-Producer Single-Consumer ring buffers delivering **10M+ message ops/sec with sub-15ns transfer latency**.
3. **Sub-Microsecond Cooperative Scheduling:** Stackless async task dispatch switching agent contexts in **10–50 nanoseconds**.
4. **Zero-Copy Memory Arenas:** Agents exchange complex multi-dimensional thought vectors and message payloads through pre-allocated memory arenas, completely bypassing JSON encoding/decoding.
5. **Massive Concurrency:** Schedules **100,000+ concurrent agents** deterministically on commodity developer hardware.

---

## 2. High-Level Architecture

```mermaid
flowchart TD
    subgraph Host["Hardware Architecture - Physical Cores"]
        subgraph Core0["Core 0 Pinned Reactor"]
            Exec0["Local Executor"]
            Shard0["Agent Shard 0<br/>Agents 0 to N/K"]
            Exec0 --> Shard0
        end

        subgraph Core1["Core 1 Pinned Reactor"]
            Exec1["Local Executor"]
            Shard1["Agent Shard 1<br/>Agents N/K to 2N/K"]
            Exec1 --> Shard1
        end

        subgraph CoreN["Core N Pinned Reactor"]
            ExecN["Local Executor"]
            ShardN["Agent Shard N<br/>Remaining Agents"]
            ExecN --> ShardN
        end
    end

    subgraph IPCMesh["Lock-Free IPC Mesh - Cache-Padded Ring Buffers"]
        Ring01["SPSC Ring Core 0 to 1"]
        Ring10["SPSC Ring Core 1 to 0"]
        Ring0N["SPSC Ring Core 0 to N"]
        RingN0["SPSC Ring Core N to 0"]
    end

    subgraph Memory["Zero-Copy Shared Memory Arena"]
        Arena["Pre-Allocated Memory Slices<br/>Zero-Copy Deserialization"]
    end

    Core0 <--> Ring01
    Core1 <--> Ring10
    Core0 <--> Ring0N
    CoreN <--> RingN0

    Shard0 -.-> Arena
    Shard1 -.-> Arena
    ShardN -.-> Arena
```

---

## 3. Core Subsystems & Crate Hierarchy

```text
agent-swarm-kernel/
├── Cargo.toml                      # Workspace Root
├── crates/
│   ├── ask-core/                   # Core agent traits, AgentId, message envelopes, state machines
│   ├── ask-ipc/                    # Lock-free SPSC ring buffers, cross-core mesh router, arenas
│   ├── ask-scheduler/              # Pinned thread-per-core reactors, sharded executors, tick loops
│   └── ask-cli/                    # 'ask' CLI: benchmarks, topology simulations, TUI monitor
```

### 3.1 `ask-core`
- **`AgentId`**: 64-bit compact identifier encoding shard index and local sequence number.
- **`AgentState`**: Minimal state machine: `Dormant`, `Active`, `Yielded`, `Terminated`.
- **`MessageEnvelope`**: Compact 32-byte header with pre-allocated payload offset into the shared arena.
- **`Agent` Trait**:
  ```rust
  pub trait Agent: Send + 'static {
      fn id(&self) -> AgentId;
      fn handle_message(&mut self, ctx: &mut Context, msg: MessageEnvelope) -> Result<()>;
      fn tick(&mut self, ctx: &mut Context) -> Result<()>;
  }
  ```

### 3.2 `ask-ipc`
- **SPSC Ring Buffers (`rtrb`)**: Cache-line aligned (`CachePadded`) lock-free single-producer single-consumer circular buffers.
- **Inter-Core Routing Mesh**: Directed SPSC channels between each core pair.
- **Shared Memory Arena**: Pre-allocated memory slices for zero-copy message payloads.

### 3.3 `ask-scheduler`
- **`Reactor`**: Pinned to a dedicated physical CPU core via `core_affinity`.
- **`AgentShard`**: Holds and dispatches messages to local agents without cross-thread atomic synchronization.
- **Cooperative Multitasking**: Round-robin tick loop executing agents with bounded per-tick gas/step limits to prevent starvation.

### 3.4 `ask-cli` (`ask`)
- **`ask bench`**: High-velocity stress test spawning 10,000 to 100,000 agents and measuring message throughput, context switch latency, and memory footprint.
- **`ask run`**: Executes multi-agent swarm topologies (`mesh`, `ring`, `gossip`, `hierarchical`).
- **`ask monitor`**: Real-time terminal visualization showing per-core CPU load, message rates, and agent state distributions.

---

## 4. Benchmark Targets vs. Existing Runtimes

| Metric | LangGraph (Python) | CrewAI (Python) | **Agent Swarm Kernel (`ask`)** |
| :--- | :---: | :---: | :---: |
| **Max Concurrent Agents** | ~250 | ~150 | **100,000+** |
| **Message Coordination Latency** | ~1,100 ms | ~3,400 ms | **< 1 µs (< 100 ns intra-core)** |
| **Message Throughput** | ~200 msgs/sec | ~50 msgs/sec | **> 10,000,000 msgs/sec** |
| **Memory per Agent** | ~12 MB | ~25 MB | **< 512 bytes (lightweight)** |
| **Context Switching Overhead** | ~50,000 ns (GIL) | ~120,000 ns | **10 – 50 ns (stackless)** |

---

## 5. Roadmap & Deliverables

1. **Phase 1: Foundation & PRD** — Repo initialization, PRD, and design docs.
2. **Phase 2: Multi-Crate Engine** — `ask-core`, `ask-ipc`, `ask-scheduler`, and `ask-cli`.
3. **Phase 3: Benchmarks & Visuals** — Live 100K-agent stress test, animated terminal demo GIF, MIT License, and CI workflow.
4. **Phase 4: Developer Showcase** — Clean documentation and integration into the developer ecosystem.
