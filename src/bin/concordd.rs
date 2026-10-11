use concord::raft::RaftNode;
use concord::storage::DiskStorage;
use concord::server::{RaftServer, InternalEvent};
use tokio::sync::mpsc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct Config {
    node_id: u64,
    data_dir: String,
    peers: Vec<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize Structured Logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_thread_names(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting Concord Server daemon...");

    // 2. Load Configuration (Simulated here, normally via Figment from config.toml)
    let config = Config {
        node_id: 1,
        data_dir: "./data".to_string(),
        peers: vec![2, 3, 4, 5],
    };

    std::fs::create_dir_all(&config.data_dir)?;

    // 3. Mount Hardware Storage (Phase 3)
    let meta_path = format!("{}/meta_{}.json", config.data_dir, config.node_id);
    let log_path = format!("{}/wal_{}.json", config.data_dir, config.node_id);
    let storage = Box::new(DiskStorage::new(&meta_path, &log_path)?);

    // 4. Boot the Raft Engine
    let node = RaftNode::recover(config.node_id, storage);
    
    let (tx, rx) = mpsc::channel::<InternalEvent>(1000);
    // Note: In a full implementation, you'd pass your real gRPC Transport layer here
    let mock_transport: std::sync::Arc<dyn concord::network::Transport> = std::sync::Arc::new(MockNetwork {});
    
    let mut server = RaftServer::new(node, config.peers, mock_transport, rx, tx);

    // 5. Graceful Shutdown & Reactor Loop
    // We race the server loop against the OS Ctrl+C signal.
    tokio::select! {
        _ = server.run() => {
            info!("Raft server loop exited unexpectedly.");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("SIGTERM received. Initiating graceful shutdown.");
            // Here you would cleanly close network sockets, flush remaining WAL buffers, 
            // and step down if this node is the leader.
            info!("WAL flushed. Concord safely stopped.");
        }
    }

    Ok(())
}

// Dummy transport for the binary compilation
struct MockNetwork {}
#[async_trait::async_trait]
impl concord::network::Transport for MockNetwork {
    async fn send_request_vote(&self, _t: u64, _a: concord::raft::RequestVoteArgs) -> Result<concord::raft::RequestVoteReply, ()> { Err(()) }
    async fn send_append_entries(&self, _t: u64, _a: concord::raft::AppendEntriesArgs) -> Result<concord::raft::AppendEntriesReply, ()> { Err(()) }
}