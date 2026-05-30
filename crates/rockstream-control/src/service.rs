//! Control-plane network service for RockStream.
//!
//! The `ControlService` listens on a TCP address and accepts connections from
//! worker nodes. Workers send [`WorkerMessage`] frames (newline-delimited JSON)
//! and receive [`ControlMessage`] responses.
//!
//! ## Wire protocol
//!
//! Each message is a single-line JSON object terminated by `\n`.
//! Messages are framed without any length prefix: each line is one message.
//!
//! ```text
//! Worker → Control:  {"type":"register", ...}\n
//! Control → Worker:  {"type":"registered","worker_id":1}\n
//! Worker → Control:  {"type":"heartbeat","worker_id":1,"capacity_headroom":0.8}\n
//! Worker → Control:  {"type":"deregister","worker_id":1}\n
//! ```

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

use rockstream_types::topology::{ControlMessage, WorkerMessage};

use crate::audit::{AuditEvent, FileAuditLog};
use crate::topology::TopologyCatalog;

/// Handle to the running control service.
pub struct ControlServiceHandle {
    /// Bound address.
    pub addr: SocketAddr,
    /// Shutdown sender; drop or send to stop the service.
    shutdown_tx: broadcast::Sender<()>,
}

impl ControlServiceHandle {
    /// Signal the service to shut down.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// Control-plane service: listens for worker registrations on TCP.
pub struct ControlService {
    catalog: TopologyCatalog,
    audit: Option<Arc<FileAuditLog>>,
}

impl ControlService {
    /// Create a new `ControlService` backed by the given catalog.
    pub fn new(catalog: TopologyCatalog) -> Self {
        Self {
            catalog,
            audit: None,
        }
    }

    /// Attach an audit log; topology events will be written to it.
    pub fn with_audit(mut self, audit: Arc<FileAuditLog>) -> Self {
        self.audit = Some(audit);
        self
    }

    /// Start the service on `bind_addr`.
    ///
    /// Returns a [`ControlServiceHandle`] which can be used to query the
    /// bound address and send a shutdown signal.
    pub async fn start(self, bind_addr: &str) -> io::Result<ControlServiceHandle> {
        let listener = TcpListener::bind(bind_addr).await?;
        let addr = listener.local_addr()?;
        tracing::info!(addr = %addr, "control service listening");

        let (shutdown_tx, _) = broadcast::channel(1);
        let shutdown_tx2 = shutdown_tx.clone();

        let catalog = self.catalog.clone();
        let audit = self.audit.clone();

        tokio::spawn(async move {
            let mut shutdown_rx = shutdown_tx2.subscribe();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, peer)) => {
                                tracing::debug!(%peer, "control: new connection");
                                let cat = catalog.clone();
                                let aud = audit.clone();
                                let mut sd = shutdown_tx2.subscribe();
                                tokio::spawn(async move {
                                    tokio::select! {
                                        _ = handle_connection(stream, peer, cat, aud) => {}
                                        _ = sd.recv() => {}
                                    }
                                });
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "control: accept error");
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        tracing::info!("control service shutting down");
                        break;
                    }
                }
            }
        });

        Ok(ControlServiceHandle { addr, shutdown_tx })
    }
}

/// Handle a single worker connection.
async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    catalog: TopologyCatalog,
    audit: Option<Arc<FileAuditLog>>,
) {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        let msg: WorkerMessage = match serde_json::from_str(&line) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(%peer, error = %e, "control: invalid message");
                continue;
            }
        };

        match msg {
            WorkerMessage::Register(reg) => {
                let worker_id = catalog.register(&reg);
                tracing::info!(
                    worker_id = %worker_id,
                    address = %reg.address,
                    headroom = %reg.capacity_headroom,
                    "control: worker registered"
                );
                if let Some(aud) = &audit {
                    let event =
                        AuditEvent::now("control", "worker.registered", worker_id.to_string())
                            .with_detail(format!(
                                "address={}, headroom={}",
                                reg.address, reg.capacity_headroom
                            ));
                    let _ = aud.append(&event);
                }
                let reply = ControlMessage::Registered { worker_id };
                send_message(&mut writer, &reply).await;
            }
            WorkerMessage::Heartbeat {
                worker_id,
                capacity_headroom,
            } => {
                if catalog.heartbeat(worker_id, capacity_headroom) {
                    tracing::debug!(
                        %worker_id,
                        headroom = %capacity_headroom,
                        "control: heartbeat"
                    );
                } else {
                    tracing::warn!(
                        %worker_id,
                        "control: heartbeat from unknown worker"
                    );
                }
            }
            WorkerMessage::Deregister { worker_id } => {
                let removed = catalog.deregister(worker_id);
                tracing::info!(
                    %worker_id,
                    found = removed.is_some(),
                    "control: worker deregistered"
                );
                if let Some(aud) = &audit {
                    let event =
                        AuditEvent::now("control", "worker.deregistered", worker_id.to_string());
                    let _ = aud.append(&event);
                }
                // Notify remaining workers about topology change
                let workers = catalog.healthy_workers();
                let notify = ControlMessage::TopologyChanged { workers };
                send_message(&mut writer, &notify).await;
            }
        }
    }
    tracing::debug!(%peer, "control: connection closed");
}

async fn send_message(writer: &mut tokio::net::tcp::OwnedWriteHalf, msg: &ControlMessage) {
    match serde_json::to_string(msg) {
        Ok(mut line) => {
            line.push('\n');
            if let Err(e) = writer.write_all(line.as_bytes()).await {
                tracing::warn!(error = %e, "control: write error");
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "control: failed to serialize message");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::TopologyCatalog;
    use rockstream_types::ids::WorkerId;
    use rockstream_types::topology::{
        CapacityHeadroom, NodeRole, WorkerMessage, WorkerRegistration,
    };
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;

    async fn start_test_service() -> (ControlServiceHandle, TopologyCatalog) {
        let catalog = TopologyCatalog::new();
        let svc = ControlService::new(catalog.clone());
        let handle = svc.start("127.0.0.1:0").await.unwrap();
        (handle, catalog)
    }

    async fn send_and_recv(stream: &mut TcpStream, msg: &WorkerMessage) -> String {
        let line = serde_json::to_string(msg).unwrap() + "\n";
        stream.write_all(line.as_bytes()).await.unwrap();
        let mut reader = BufReader::new(&mut *stream);
        let mut resp = String::new();
        reader.read_line(&mut resp).await.unwrap();
        resp
    }

    #[tokio::test]
    async fn worker_registers_and_receives_ack() {
        let (handle, catalog) = start_test_service().await;
        let mut stream = TcpStream::connect(handle.addr).await.unwrap();

        let reg = WorkerRegistration::new(
            WorkerId(1),
            NodeRole::Worker,
            "127.0.0.1:7001",
            CapacityHeadroom::new(0.9),
        );
        let resp = send_and_recv(&mut stream, &WorkerMessage::Register(reg)).await;
        let reply: ControlMessage = serde_json::from_str(resp.trim()).unwrap();
        match reply {
            ControlMessage::Registered { worker_id } => {
                assert_eq!(worker_id, WorkerId(1));
            }
            _ => panic!("expected Registered reply"),
        }
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog.get(WorkerId(1)).unwrap().address, "127.0.0.1:7001");

        handle.shutdown();
    }

    #[tokio::test]
    async fn topology_catalog_updated_after_registration() {
        let (handle, catalog) = start_test_service().await;

        for i in 1..=3u64 {
            let mut stream = TcpStream::connect(handle.addr).await.unwrap();
            let reg = WorkerRegistration::new(
                WorkerId(i),
                NodeRole::Worker,
                format!("127.0.0.1:{}", 7000 + i),
                CapacityHeadroom::new(0.5 + i as f64 * 0.1),
            );
            let line = serde_json::to_string(&WorkerMessage::Register(reg)).unwrap() + "\n";
            stream.write_all(line.as_bytes()).await.unwrap();
            // wait for ack
            let mut reader = BufReader::new(&mut stream);
            let mut resp = String::new();
            reader.read_line(&mut resp).await.unwrap();
        }

        // Allow async tasks to process
        tokio::task::yield_now().await;
        assert_eq!(catalog.len(), 3);
        handle.shutdown();
    }

    #[tokio::test]
    async fn tier2_start_flow() {
        // Tier 2: --role=all means control + worker start in the same process.
        // Verify the control service starts and a worker can self-register.
        let catalog = TopologyCatalog::new();
        let svc = ControlService::new(catalog.clone());
        let handle = svc.start("127.0.0.1:0").await.unwrap();
        let addr = handle.addr.to_string();

        // Simulate a worker connecting to its own in-process control service.
        let mut stream = TcpStream::connect(&addr).await.unwrap();
        let reg = WorkerRegistration::new(
            WorkerId(100),
            NodeRole::All,
            addr.clone(),
            CapacityHeadroom::FULL,
        );
        let line = serde_json::to_string(&WorkerMessage::Register(reg)).unwrap() + "\n";
        stream.write_all(line.as_bytes()).await.unwrap();

        let mut reader = BufReader::new(&mut stream);
        let mut resp = String::new();
        reader.read_line(&mut resp).await.unwrap();
        let reply: ControlMessage = serde_json::from_str(resp.trim()).unwrap();
        assert!(matches!(reply, ControlMessage::Registered { .. }));

        assert!(catalog.get(WorkerId(100)).is_some());
        handle.shutdown();
    }
}
