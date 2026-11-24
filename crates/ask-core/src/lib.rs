use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::sync::OnceLock;
use std::time::Instant;

/// Maximum inline payload size in bytes for zero-heap-allocation message envelopes.
pub const MAX_INLINE_PAYLOAD: usize = 64;

const HIST_BUCKETS: usize = 256;

static KERNEL_EPOCH: OnceLock<Instant> = OnceLock::new();

/// Returns monotonic nanoseconds elapsed since the kernel clock epoch was initialized.
#[inline]
pub fn monotonic_now_ns() -> u64 {
    KERNEL_EPOCH.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

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

/// Error returned when constructing an inline payload exceeding [`MAX_INLINE_PAYLOAD`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PayloadTooLarge {
    pub actual: usize,
    pub max: usize,
}

impl fmt::Display for PayloadTooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "inline payload length {} exceeds maximum {} bytes",
            self.actual, self.max
        )
    }
}

impl std::error::Error for PayloadTooLarge {}

/// Fixed-capacity stack-allocated inline payload buffer (`Copy`, zero heap allocations).
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct InlinePayload {
    pub bytes: [u8; MAX_INLINE_PAYLOAD],
    pub len: u8,
}

impl Default for InlinePayload {
    fn default() -> Self {
        Self::empty()
    }
}

impl InlinePayload {
    #[inline(always)]
    pub const fn empty() -> Self {
        Self {
            bytes: [0u8; MAX_INLINE_PAYLOAD],
            len: 0,
        }
    }

    /// Copies up to [`MAX_INLINE_PAYLOAD`] bytes into the inline buffer.
    #[inline]
    pub fn from_slice_truncated(src: &[u8]) -> Self {
        let copy_len = src.len().min(MAX_INLINE_PAYLOAD);
        let mut bytes = [0u8; MAX_INLINE_PAYLOAD];
        bytes[..copy_len].copy_from_slice(&src[..copy_len]);
        Self {
            bytes,
            len: copy_len as u8,
        }
    }

    /// Copies `src` into the inline buffer, returning an error if it exceeds [`MAX_INLINE_PAYLOAD`].
    #[inline]
    pub fn try_from_slice(src: &[u8]) -> Result<Self, PayloadTooLarge> {
        if src.len() > MAX_INLINE_PAYLOAD {
            return Err(PayloadTooLarge {
                actual: src.len(),
                max: MAX_INLINE_PAYLOAD,
            });
        }
        Ok(Self::from_slice_truncated(src))
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        let len = (self.len as usize).min(MAX_INLINE_PAYLOAD);
        &self.bytes[..len]
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        (self.len as usize).min(MAX_INLINE_PAYLOAD)
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl From<&[u8]> for InlinePayload {
    #[inline]
    fn from(src: &[u8]) -> Self {
        Self::from_slice_truncated(src)
    }
}

impl From<&str> for InlinePayload {
    #[inline]
    fn from(src: &str) -> Self {
        Self::from_slice_truncated(src.as_bytes())
    }
}

mod inline_payload_serde {
    use super::*;

    pub fn serialize<S>(bytes: &[u8; MAX_INLINE_PAYLOAD], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; MAX_INLINE_PAYLOAD], D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BytesVisitor;

        impl<'de> Visitor<'de> for BytesVisitor {
            type Value = [u8; MAX_INLINE_PAYLOAD];

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a byte array of length {}", MAX_INLINE_PAYLOAD)
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if v.len() != MAX_INLINE_PAYLOAD {
                    return Err(E::invalid_length(v.len(), &self));
                }
                let mut arr = [0u8; MAX_INLINE_PAYLOAD];
                arr.copy_from_slice(v);
                Ok(arr)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut arr = [0u8; MAX_INLINE_PAYLOAD];
                for (i, slot) in arr.iter_mut().enumerate() {
                    *slot = seq
                        .next_element()?
                        .ok_or_else(|| de::Error::invalid_length(i, &self))?;
                }
                Ok(arr)
            }
        }

        deserializer.deserialize_bytes(BytesVisitor)
    }
}

/// Compact zero-allocation message envelope exchanged between agents over lock-free SPSC rings.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MessageEnvelope {
    pub sender: AgentId,
    pub receiver: AgentId,
    pub seq: u64,
    pub kind: MessageKind,
    pub payload_len: u8,
    #[serde(with = "inline_payload_serde")]
    pub payload: [u8; MAX_INLINE_PAYLOAD],
    pub timestamp_ns: u64,
}

impl MessageEnvelope {
    /// Creates a new envelope with an inline payload slice (truncated at 64 bytes if larger)
    /// and monotonic nanosecond timestamp. Allocates zero heap memory.
    #[inline]
    pub fn new(
        sender: AgentId,
        receiver: AgentId,
        seq: u64,
        kind: MessageKind,
        payload: &[u8],
    ) -> Self {
        let inline = InlinePayload::from_slice_truncated(payload);
        Self {
            sender,
            receiver,
            seq,
            kind,
            payload_len: inline.len,
            payload: inline.bytes,
            timestamp_ns: monotonic_now_ns(),
        }
    }

    /// Creates a new envelope, returning [`PayloadTooLarge`] if `payload.len() > MAX_INLINE_PAYLOAD`.
    #[inline]
    pub fn try_new(
        sender: AgentId,
        receiver: AgentId,
        seq: u64,
        kind: MessageKind,
        payload: &[u8],
    ) -> Result<Self, PayloadTooLarge> {
        let inline = InlinePayload::try_from_slice(payload)?;
        Ok(Self {
            sender,
            receiver,
            seq,
            kind,
            payload_len: inline.len,
            payload: inline.bytes,
            timestamp_ns: monotonic_now_ns(),
        })
    }

    /// Creates a new envelope directly from a pre-built [`InlinePayload`].
    #[inline]
    pub fn from_inline(
        sender: AgentId,
        receiver: AgentId,
        seq: u64,
        kind: MessageKind,
        inline: InlinePayload,
    ) -> Self {
        Self {
            sender,
            receiver,
            seq,
            kind,
            payload_len: inline.len,
            payload: inline.bytes,
            timestamp_ns: monotonic_now_ns(),
        }
    }

    /// Overrides the monotonic timestamp (useful for deterministic unit tests).
    #[inline]
    pub fn with_timestamp(mut self, timestamp_ns: u64) -> Self {
        self.timestamp_ns = timestamp_ns;
        self
    }

    /// Returns the active slice of the inline payload.
    #[inline(always)]
    pub fn payload(&self) -> &[u8] {
        let len = (self.payload_len as usize).min(MAX_INLINE_PAYLOAD);
        &self.payload[..len]
    }
}

/// Pure-Rust log2 latency histogram with 4 linear sub-buckets per power-of-two (256 buckets total).
/// Provides exact 1ns resolution for 0..=7ns and ~25% bucket precision up to `u64::MAX` ns.
#[derive(Copy, Clone, Debug)]
pub struct LatencyHistogram {
    buckets: [u64; HIST_BUCKETS],
    count: u64,
    min_ns: u64,
    max_ns: u64,
    sum_ns: u128,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    pub const fn new() -> Self {
        Self {
            buckets: [0u64; HIST_BUCKETS],
            count: 0,
            min_ns: u64::MAX,
            max_ns: 0,
            sum_ns: 0,
        }
    }

    #[inline(always)]
    fn bucket_index(value_ns: u64) -> usize {
        if value_ns < 4 {
            value_ns as usize
        } else {
            let msb = (63 - value_ns.leading_zeros()) as usize;
            let sub = ((value_ns >> (msb - 2)) & 0x3) as usize;
            ((msb - 1) * 4 + sub).min(HIST_BUCKETS - 1)
        }
    }

    #[inline(always)]
    fn bucket_bounds(idx: usize) -> (u64, u64) {
        if idx < 4 {
            let v = idx as u64;
            (v, v)
        } else {
            let msb = (idx / 4) + 1;
            let sub = (idx % 4) as u64;
            let step = 1u64 << (msb - 2);
            let lo = (1u64 << msb) | (sub << (msb - 2));
            let hi = lo.saturating_add(step - 1);
            (lo, hi)
        }
    }

    #[inline]
    pub fn record(&mut self, value_ns: u64) {
        let idx = Self::bucket_index(value_ns);
        self.buckets[idx] += 1;
        self.count += 1;
        self.sum_ns += value_ns as u128;
        if value_ns < self.min_ns {
            self.min_ns = value_ns;
        }
        if value_ns > self.max_ns {
            self.max_ns = value_ns;
        }
    }

    pub fn merge(&mut self, other: &Self) {
        if other.count == 0 {
            return;
        }
        for (dst, src) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *dst += *src;
        }
        self.count += other.count;
        self.sum_ns += other.sum_ns;
        if other.min_ns < self.min_ns {
            self.min_ns = other.min_ns;
        }
        if other.max_ns > self.max_ns {
            self.max_ns = other.max_ns;
        }
    }

    pub fn reset(&mut self) {
        self.buckets.fill(0);
        self.count = 0;
        self.min_ns = u64::MAX;
        self.max_ns = 0;
        self.sum_ns = 0;
    }

    #[inline]
    pub fn count(&self) -> u64 {
        self.count
    }

    #[inline]
    pub fn min_ns(&self) -> u64 {
        if self.count == 0 {
            0
        } else {
            self.min_ns
        }
    }

    #[inline]
    pub fn max_ns(&self) -> u64 {
        self.max_ns
    }

    #[inline]
    pub fn mean_ns(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            (self.sum_ns as f64) / (self.count as f64)
        }
    }

    /// Computes the estimated value in nanoseconds at quantile `q` (`0.0..=1.0`, e.g., `0.50` or `0.99`).
    pub fn percentile_ns(&self, q: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let q_clamped = q.clamp(0.0, 1.0);
        if q_clamped == 0.0 {
            return self.min_ns;
        }
        if q_clamped == 1.0 {
            return self.max_ns;
        }

        let target = ((self.count as f64) * q_clamped).ceil().max(1.0) as u64;
        let mut cumulative = 0u64;

        for (idx, &bucket_count) in self.buckets.iter().enumerate() {
            if bucket_count == 0 {
                continue;
            }
            let prev = cumulative;
            cumulative += bucket_count;
            if cumulative >= target {
                let (lo, hi) = Self::bucket_bounds(idx);
                let offset_in_bucket = target.saturating_sub(prev) as f64;
                let frac = (offset_in_bucket / (bucket_count as f64)).clamp(0.0, 1.0);
                let span = (hi - lo) as f64;
                let est = (lo as f64) + span * frac;
                return (est.round() as u64).clamp(self.min_ns, self.max_ns);
            }
        }

        self.max_ns
    }
}

/// Asynchronous I/O job offloaded from a pinned `ShardReactor` to the [`IoWorkerPool`].
pub struct IoTask {
    pub requester: AgentId,
    pub correlation_id: u64,
    pub completion_kind: MessageKind,
    pub work: Box<dyn FnOnce() -> InlinePayload + Send + 'static>,
}

/// Execution outcome for agent steps.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionResult {
    Ok,
    Yield,
    Terminate,
    Err(String),
}

/// Outgoing action queue for an agent during a tick or message handler.
#[derive(Default)]
pub struct Context {
    pub outgoing_messages: Vec<MessageEnvelope>,
    pub outgoing_io: Vec<IoTask>,
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

    /// Submits a blocking or external I/O closure to run asynchronously on the kernel I/O worker pool.
    /// When the closure returns, a completion [`MessageEnvelope`] is routed back to `requester`
    /// with `seq == correlation_id` and `kind == completion_kind`.
    #[inline]
    pub fn spawn_io<F>(
        &mut self,
        requester: AgentId,
        correlation_id: u64,
        completion_kind: MessageKind,
        work: F,
    ) where
        F: FnOnce() -> InlinePayload + Send + 'static,
    {
        self.outgoing_io.push(IoTask {
            requester,
            correlation_id,
            completion_kind,
            work: Box::new(work),
        });
    }

    #[inline]
    pub fn yield_now(&mut self) {
        self.should_yield = true;
    }

    #[inline]
    pub fn terminate(&mut self) {
        self.should_terminate = true;
    }

    /// Resets queued outputs and lifecycle flags while preserving allocated buffer capacity.
    #[inline]
    pub fn clear(&mut self) {
        self.outgoing_messages.clear();
        self.outgoing_io.clear();
        self.should_yield = false;
        self.should_terminate = false;
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
    pub core_id: usize,
    pub pinned: bool,
    pub messages_processed: u64,
    pub messages_dropped: u64,
    pub io_submitted: u64,
    pub io_completed: u64,
    pub ticks_executed: u64,
    pub nanos_elapsed: u64,
    pub latency_histogram: LatencyHistogram,
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
            b"test",
        );
        assert_eq!(env.payload(), b"test");
        ctx.send(env);
        assert_eq!(ctx.outgoing_messages.len(), 1);
    }

    #[test]
    fn test_inline_payload_bounds_and_serde() {
        let oversized = [7u8; MAX_INLINE_PAYLOAD + 1];
        assert!(MessageEnvelope::try_new(
            AgentId::new(0, 0),
            AgentId::new(0, 1),
            1,
            MessageKind::Signal,
            &oversized,
        )
        .is_err());

        let env = MessageEnvelope::new(
            AgentId::new(1, 9),
            AgentId::new(2, 3),
            42,
            MessageKind::Task,
            b"hello-zero-alloc",
        );
        let json = serde_json::to_string(&env).expect("serialize envelope");
        let decoded: MessageEnvelope = serde_json::from_str(&json).expect("deserialize envelope");
        assert_eq!(decoded, env);
        assert_eq!(decoded.payload(), b"hello-zero-alloc");
    }

    #[test]
    fn test_latency_histogram_percentiles_and_merge() {
        let mut h1 = LatencyHistogram::new();
        for i in 1..=100 {
            h1.record(i * 10); // 10ns .. 1000ns
        }
        assert_eq!(h1.count(), 100);
        assert_eq!(h1.min_ns(), 10);
        assert_eq!(h1.max_ns(), 1000);

        let p50 = h1.percentile_ns(0.50);
        let p99 = h1.percentile_ns(0.99);
        assert!((400..=600).contains(&p50), "p50={}", p50);
        assert!((900..=1000).contains(&p99), "p99={}", p99);

        let mut h2 = LatencyHistogram::new();
        for _ in 0..100 {
            h2.record(2000);
        }
        h1.merge(&h2);
        assert_eq!(h1.count(), 200);
        assert_eq!(h1.max_ns(), 2000);
        assert!(h1.percentile_ns(0.99) >= 1500);
    }

    #[test]
    fn test_context_spawn_io_and_clear() {
        let mut ctx = Context::new();
        ctx.spawn_io(AgentId::new(0, 5), 99, MessageKind::Signal, || {
            InlinePayload::from("done")
        });
        assert_eq!(ctx.outgoing_io.len(), 1);
        ctx.clear();
        assert!(ctx.outgoing_io.is_empty());
        assert!(ctx.outgoing_messages.is_empty());
        assert!(!ctx.should_yield);
        assert!(!ctx.should_terminate);
    }
}
