use ask_core::MessageEnvelope;
use rtrb::{Consumer, Producer, RingBuffer};
use std::collections::HashMap;

#[derive(Debug)]
pub enum IpcError {
    QueueFull,
    InvalidDestination(u16),
    Disconnected,
}

/// Dedicated IPC endpoint for a single physical core reactor.
pub struct ShardEndpoint {
    pub core_id: usize,
    pub num_cores: usize,
    producers: HashMap<usize, Producer<MessageEnvelope>>,
    consumers: HashMap<usize, Consumer<MessageEnvelope>>,
}

impl ShardEndpoint {
    /// Send an envelope to a destination core shard via a dedicated lock-free SPSC channel.
    #[inline(always)]
    pub fn send(&mut self, target_core: usize, env: MessageEnvelope) -> Result<(), IpcError> {
        if let Some(producer) = self.producers.get_mut(&target_core) {
            producer.push(env).map_err(|_| IpcError::QueueFull)
        } else {
            Err(IpcError::InvalidDestination(target_core as u16))
        }
    }

    /// Try to send if possible, otherwise drop or return err without blocking.
    #[inline(always)]
    pub fn try_send(&mut self, target_core: usize, env: MessageEnvelope) -> bool {
        if let Some(producer) = self.producers.get_mut(&target_core) {
            producer.push(env).is_ok()
        } else {
            false
        }
    }

    /// Drain incoming messages from all peer core shards into the target vector.
    /// Uses lock-free SPSC pop without contention.
    #[inline(always)]
    pub fn drain_incoming(&mut self, dest: &mut Vec<MessageEnvelope>) -> usize {
        let mut count = 0;
        for consumer in self.consumers.values_mut() {
            while let Ok(msg) = consumer.pop() {
                dest.push(msg);
                count += 1;
            }
        }
        count
    }
}

/// Topology builder creating a full lock-free all-to-all mesh network between N cores.
pub struct MeshNetwork;

impl MeshNetwork {
    /// Creates N ShardEndpoints with pairwise directed SPSC ring buffers.
    pub fn create_mesh(num_cores: usize, ring_capacity: usize) -> Vec<ShardEndpoint> {
        assert!(num_cores > 0, "Number of cores must be greater than 0");

        let mut producers_map: Vec<HashMap<usize, Producer<MessageEnvelope>>> = 
            (0..num_cores).map(|_| HashMap::new()).collect();
        let mut consumers_map: Vec<HashMap<usize, Consumer<MessageEnvelope>>> = 
            (0..num_cores).map(|_| HashMap::new()).collect();

        for i in 0..num_cores {
            for j in 0..num_cores {
                if i != j {
                    // Create directed SPSC channel from core i to core j
                    let (p, c) = RingBuffer::new(ring_capacity);
                    producers_map[i].insert(j, p);
                    consumers_map[j].insert(i, c);
                }
            }
        }

        let mut endpoints = Vec::with_capacity(num_cores);
        for i in 0..num_cores {
            endpoints.push(ShardEndpoint {
                core_id: i,
                num_cores,
                producers: producers_map.remove(0),
                consumers: consumers_map.remove(0),
            });
        }

        endpoints
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
            b"payload".to_vec(),
        );

        // Core 0 sends to Core 1
        endpoints[0].send(1, env).expect("Send should succeed");

        // Core 1 drains incoming
        let mut incoming = Vec::new();
        let drained = endpoints[1].drain_incoming(&mut incoming);
        assert_eq!(drained, 1);
        assert_eq!(incoming[0].seq, 100);
    }
}
