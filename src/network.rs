use crate::raft::{AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply, NodeId};
use crate::command::ClientRequest;
use tokio::sync::oneshot;
use async_trait::async_trait;

#[derive(Debug)]
pub enum ClientPayload {
    Put {key: String, value: Vec<u8>},
    Get {key: String},
}

#[derive(Debug)]
pub enum ClientResponse {
    Success(Option<Vec<u8>>),
    NotLeader{leader_id: Option<NodeId>},
    Timeout,
}

pub enum RaftMessage {
    RequestVote {
        args: RequestVoteArgs,
        reply_tx: oneshot::Sender<RequestVoteReply>, 
    },
    AppendEntries {
        args: AppendEntriesArgs,
        reply_tx: oneshot::Sender<AppendEntriesReply>,
    },
    ClientCommand {
        req: ClientRequest,
        reply_tx: oneshot::Sender<ClientResponse>,
    }
}

#[async_trait]
pub trait Transport: Send + Sync {
    async fn send_request_vote(&self, target: NodeId, args: RequestVoteArgs)-> Result<RequestVoteReply,()>;
    async fn send_append_entries(&self, target: NodeId, args: AppendEntriesArgs) -> Result<AppendEntriesReply,()>;
}

