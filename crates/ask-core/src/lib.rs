use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

/// 64-bit compact Agent Identifier.
/// Top 16 bits encode the Shard (core) ID; lower 48 bits encode local agent index.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct AgentId(pub u64);

impl AgentId {
    #[inline(always)]
    pub fn new(shard_id: u16, local_seq: u64) -> Self {
        let val = ((shard_id as u64) << 48) | (local_seq & 0x0000_FFFF_FFFF_FFFF);
        Self(val)
    }

    #[inline(always)]
    pub fn shard_id(&self) -> u16 {
        (self.0 >> 48) as u16
    }

    #[inline(always)]
    pub fn local_seq(&self) -> u64 {
        self.0 & 0x0000_FFFF_FFFF_FFFF
    }
}

impl fmt::Debug for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Agent#{}:{}", self.shard_id(), self.local_seq())
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.shard_id(), self.local_seq())
    }
}

/// Agent lifecycle states.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AgentState {
    Dormant,
    Active,
    Yielded,
    Terminated,
}

/// Classification of inter-agent messages.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MessageKind {
    Thought,
    Task,
    Sync,
    Signal,
    Heartbeat,
}

/// Compact message envelope exchanged between agents.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageEnvelope {
    pub sender: AgentId,
    pub receiver: AgentId,
    pub seq: u64,
    pub kind: MessageKind,
    pub payload: Vec<u8>,
    pub timestamp_ns: u64,
}

impl MessageEnvelope {
    #[inline]
    pub fn new(sender: AgentId, receiver: AgentId, seq: u64, kind: MessageKind, payload: Vec<u8>) -> Self {
        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        Self {
            sender,
            receiver,
            seq,
            kind,
            payload,
            timestamp_ns,
        }
    }
}

/// Execution outcome for agent steps.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionResult {
    Ok,
    Yield,
    Terminate,
    Err(String),
}

/// Outgoing action queue for an agent during a tick.
#[derive(Default)]
pub struct Context {
    pub outgoing_messages: Vec<MessageEnvelope>,
    pub should_yield: bool,
    pub should_terminate: bool,
}

impl Context {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn send(&mut self, env: MessageEnvelope) {
        self.outgoing_messages.push(env);
    }

    #[inline]
    pub fn yield_now(&mut self) {
        self.should_yield = true;
    }

    #[inline]
    pub fn terminate(&mut self) {
        self.should_terminate = true;
    }
}

/// Trait defining an Autonomous Swarm Agent.
pub trait Agent: Send + 'static {
    fn id(&self) -> AgentId;
    fn state(&self) -> AgentState;
    fn handle_message(&mut self, env: MessageEnvelope, ctx: &mut Context) -> ExecutionResult;
    fn tick(&mut self, ctx: &mut Context) -> ExecutionResult;
}

/// Cumulative execution statistics per agent or shard.
#[derive(Default, Copy, Clone, Debug)]
pub struct ExecutionStats {
    pub messages_processed: u64,
    pub ticks_executed: u64,
    pub nanos_elapsed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_agent_id_bitpacking() {
        let id = AgentId::new(4, 1024);
        assert_eq!(id.shard_id(), 4);
        assert_eq!(id.local_seq(), 1024);
    }

    #[test]
    fn test_context_and_envelope() {
        let mut ctx = Context::new();
        let env = MessageEnvelope::new(
            AgentId::new(0, 1),
            AgentId::new(1, 2),
            1,
            MessageKind::Thought,
            b"test".to_vec(),
        );
        ctx.send(env);
        assert_eq!(ctx.outgoing_messages.len(), 1);
    }
}
