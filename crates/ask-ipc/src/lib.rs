use ask_core::MessageEnvelope;
use rtrb::{Consumer, Producer, RingBuffer};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum IpcError {
    QueueFull,
    InvalidDestination(u16),
    Disconnected,
}

/// Dedicated IPC endpoint for a single physical core reactor.
pub struct ShardEndpoint {
    pub core_id: usize,
    pub num_cores: usize,
    ring_capacity: usize,
    dropped_count: u64,
    producers: Vec<Option<Producer<MessageEnvelope>>>,
    consumers: Vec<Consumer<MessageEnvelope>>,
}

impl ShardEndpoint {
    /// Send an envelope to a destination core shard via a dedicated lock-free SPSC channel.
    #[inline(always)]
    pub fn send(&mut self, target_core: usize, env: MessageEnvelope) -> Result<(), IpcError> {
        if let Some(Some(producer)) = self.producers.get_mut(target_core) {
            if producer.push(env).is_ok() {
                Ok(())
            } else {
                self.dropped_count += 1;
                Err(IpcError::QueueFull)
            }
        } else {
            self.dropped_count += 1;
            Err(IpcError::InvalidDestination(target_core as u16))
        }
    }

    /// Try to send if possible, otherwise record drop and return `false` without blocking.
    #[inline(always)]
    pub fn try_send(&mut self, target_core: usize, env: MessageEnvelope) -> bool {
        self.send(target_core, env).is_ok()
    }

    /// Drain incoming messages from all peer core shards into the target vector.
    /// Uses lock-free SPSC pop without contention.
    #[inline(always)]
    pub fn drain_incoming(&mut self, dest: &mut Vec<MessageEnvelope>) -> usize {
        let mut count = 0;
        for consumer in &mut self.consumers {
            while let Ok(msg) = consumer.pop() {
                dest.push(msg);
                count += 1;
            }
        }
        count
    }

    /// Returns total envelopes dropped by this endpoint due to full SPSC rings or invalid targets.
    #[inline(always)]
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count
    }

    /// Returns the configured capacity of each pairwise SPSC ring buffer.
    #[inline(always)]
    pub fn ring_capacity(&self) -> usize {
        self.ring_capacity
    }

    /// Returns instantaneous ring buffer utilization in basis points (`0..=10_000`, where `10_000 == 100.00%`).
    /// Computed from live atomic slot counters across this shard's outbound and inbound rings.
    pub fn ring_utilization_bps(&self) -> u64 {
        if self.ring_capacity == 0 {
            return 0;
        }
        let mut total_used = 0usize;
        let mut total_cap = 0usize;

        for producer in self.producers.iter().flatten() {
            let free = producer.slots().min(self.ring_capacity);
            total_used += self.ring_capacity - free;
            total_cap += self.ring_capacity;
        }
        for consumer in &self.consumers {
            let pending = consumer.slots().min(self.ring_capacity);
            total_used += pending;
            total_cap += self.ring_capacity;
        }

        if total_cap == 0 {
            0
        } else {
            ((total_used as u128 * 10_000) / (total_cap as u128)) as u64
        }
    }
}

/// Topology builder creating a full lock-free all-to-all mesh network between N cores.
pub struct MeshNetwork;

impl MeshNetwork {
    /// Creates N ShardEndpoints with pairwise directed SPSC ring buffers.
    pub fn create_mesh(num_cores: usize, ring_capacity: usize) -> Vec<ShardEndpoint> {
        assert!(num_cores > 0, "Number of cores must be greater than 0");

        let mut producers_vec: Vec<Vec<Option<Producer<MessageEnvelope>>>> = (0..num_cores)
            .map(|_| (0..num_cores).map(|_| None).collect())
            .collect();
        let mut consumers_vec: Vec<Vec<Consumer<MessageEnvelope>>> = (0..num_cores)
            .map(|_| Vec::with_capacity(num_cores.saturating_sub(1)))
            .collect();

        for i in 0..num_cores {
            for j in 0..num_cores {
                if i != j {
                    let (p, c) = RingBuffer::new(ring_capacity);
                    producers_vec[i][j] = Some(p);
                    consumers_vec[j].push(c);
                }
            }
        }

        producers_vec
            .into_iter()
            .zip(consumers_vec)
            .enumerate()
            .map(|(core_id, (producers, consumers))| ShardEndpoint {
                core_id,
                num_cores,
                ring_capacity,
                dropped_count: 0,
                producers,
                consumers,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ask_core::{AgentId, MessageKind};

    #[test]
    fn test_pairwise_spsc_mesh() {
        let mut endpoints = MeshNetwork::create_mesh(4, 1024);
        assert_eq!(endpoints.len(), 4);

        let env = MessageEnvelope::new(
            AgentId::new(0, 1),
            AgentId::new(1, 2),
            100,
            MessageKind::Task,
            b"payload",
        );

        endpoints[0].send(1, env).expect("Send should succeed");
        assert!(endpoints[0].ring_utilization_bps() > 0);

        let mut incoming = Vec::new();
        let drained = endpoints[1].drain_incoming(&mut incoming);
        assert_eq!(drained, 1);
        assert_eq!(incoming[0].seq, 100);
        assert_eq!(incoming[0].payload(), b"payload");
        assert_eq!(endpoints[0].dropped_count(), 0);
    }

    #[test]
    fn test_spsc_queue_full_drop_counter() {
        let mut endpoints = MeshNetwork::create_mesh(2, 4);
        let env = MessageEnvelope::new(
            AgentId::new(0, 0),
            AgentId::new(1, 0),
            1,
            MessageKind::Signal,
            b"x",
        );

        for _ in 0..4 {
            assert!(endpoints[0].try_send(1, env));
        }
        assert_eq!(endpoints[0].send(1, env), Err(IpcError::QueueFull));
        assert!(!endpoints[0].try_send(1, env));
        assert_eq!(endpoints[0].dropped_count(), 2);
    }
}
