//! In-memory network simulation.
//!
//! Provides a deterministic simulated network where messages between nodes
//! are delivered in a controlled order based on the seeded RNG.

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

/// A network message in the simulation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetMessage {
    pub from: NodeId,
    pub to: NodeId,
    pub payload: Bytes,
}

/// Node identifier in the simulated network.
pub type NodeId = u64;

/// Handle to a simulated network (cheaply cloneable).
#[derive(Debug, Clone)]
pub struct SimNetworkHandle {
    inner: Arc<SimNetwork>,
}

impl SimNetworkHandle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SimNetwork::new()),
        }
    }

    /// Send a message from one node to another.
    pub fn send(&self, from: NodeId, to: NodeId, payload: Bytes) {
        self.inner.send(from, to, payload);
    }

    /// Receive the next message for a given node, if any.
    pub fn recv(&self, node: NodeId) -> Option<NetMessage> {
        self.inner.recv(node)
    }

    /// Check how many messages are pending for a node.
    pub fn pending_count(&self, node: NodeId) -> usize {
        self.inner.pending_count(node)
    }

    /// Total messages in-flight across all nodes.
    pub fn total_in_flight(&self) -> usize {
        self.inner.total_in_flight()
    }

    /// Drain all messages (for snapshot/comparison).
    pub fn drain_all(&self) -> Vec<NetMessage> {
        self.inner.drain_all()
    }
}

impl Default for SimNetworkHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// The simulated network: a set of per-node message queues.
#[derive(Debug)]
pub struct SimNetwork {
    queues: Mutex<std::collections::BTreeMap<NodeId, VecDeque<NetMessage>>>,
}

impl SimNetwork {
    pub fn new() -> Self {
        Self {
            queues: Mutex::new(std::collections::BTreeMap::new()),
        }
    }

    pub fn send(&self, from: NodeId, to: NodeId, payload: Bytes) {
        let msg = NetMessage { from, to, payload };
        let mut queues = self.queues.lock();
        queues.entry(to).or_default().push_back(msg);
    }

    pub fn recv(&self, node: NodeId) -> Option<NetMessage> {
        let mut queues = self.queues.lock();
        queues.get_mut(&node)?.pop_front()
    }

    pub fn pending_count(&self, node: NodeId) -> usize {
        let queues = self.queues.lock();
        queues.get(&node).map_or(0, |q| q.len())
    }

    pub fn total_in_flight(&self) -> usize {
        let queues = self.queues.lock();
        queues.values().map(|q| q.len()).sum()
    }

    pub fn drain_all(&self) -> Vec<NetMessage> {
        let mut queues = self.queues.lock();
        let mut all = Vec::new();
        for queue in queues.values_mut() {
            all.extend(queue.drain(..));
        }
        all
    }
}

impl Default for SimNetwork {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_and_recv() {
        let net = SimNetworkHandle::new();
        net.send(1, 2, Bytes::from("hello"));
        let msg = net.recv(2).unwrap();
        assert_eq!(msg.from, 1);
        assert_eq!(msg.to, 2);
        assert_eq!(msg.payload, Bytes::from("hello"));
    }

    #[test]
    fn recv_empty() {
        let net = SimNetworkHandle::new();
        assert!(net.recv(1).is_none());
    }

    #[test]
    fn fifo_order() {
        let net = SimNetworkHandle::new();
        net.send(1, 2, Bytes::from("first"));
        net.send(1, 2, Bytes::from("second"));
        assert_eq!(net.recv(2).unwrap().payload, Bytes::from("first"));
        assert_eq!(net.recv(2).unwrap().payload, Bytes::from("second"));
    }

    #[test]
    fn pending_count() {
        let net = SimNetworkHandle::new();
        net.send(1, 2, Bytes::from("a"));
        net.send(1, 2, Bytes::from("b"));
        assert_eq!(net.pending_count(2), 2);
        assert_eq!(net.pending_count(1), 0);
    }

    #[test]
    fn total_in_flight() {
        let net = SimNetworkHandle::new();
        net.send(1, 2, Bytes::from("a"));
        net.send(3, 4, Bytes::from("b"));
        assert_eq!(net.total_in_flight(), 2);
    }

    #[test]
    fn drain_all() {
        let net = SimNetworkHandle::new();
        net.send(1, 2, Bytes::from("a"));
        net.send(3, 4, Bytes::from("b"));
        let all = net.drain_all();
        assert_eq!(all.len(), 2);
        assert_eq!(net.total_in_flight(), 0);
    }
}
