use crate::raft::{AppendEntriesArgs, RequestVoteArgs, RaftNode, Role, NodeId};
use crate::network::{RaftMessage, Transport, ClientResponse};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration, Instant};
use std::sync::Arc;
use rand::Rng;

pub enum InternalEvent {
    Message(RaftMessage),
    VoteResult(crate::raft::RequestVoteReply),
    HeartbeatResult(NodeId, crate::raft::AppendEntriesReply),
}

pub struct RaftServer {
    node: RaftNode,
    peers: Vec<NodeId>,
    transport: Arc<dyn Transport>,
    receiver: mpsc::Receiver<InternalEvent>,
    sender: mpsc::Sender<InternalEvent>,
}

impl RaftServer {
    pub fn new(
        node: RaftNode,
        peers: Vec<NodeId>,
        transport: Arc<dyn Transport>,
        receiver: mpsc::Receiver<InternalEvent>,
        sender: mpsc::Sender<InternalEvent>,
    ) -> Self {
        Self {
            node,
            peers,
            transport,
            receiver,
            sender,
        }
    }

    pub async fn run(&mut self) {
        let mut election_timer = Box::pin(sleep(self.random_election_timeout()));
        let mut heartbeat_interval = tokio::time::interval(Duration::from_millis(50));

        loop {
            tokio::select! {
                Some(event) = self.receiver.recv() => {
                    self.handle_event(event, &mut election_timer).await;
                }
                
                _ = &mut election_timer, if self.node.role != Role::Leader => {
                    self.node.become_candidate();
                    self.start_election().await;
                    election_timer.as_mut().reset(Instant::now() + self.random_election_timeout());
                }
                
                _ = heartbeat_interval.tick(), if self.node.role == Role::Leader => {
                    self.broadcast_heartbeats().await;
                }
            }
        }
    }

    async fn handle_event(
        &mut self,
        event: InternalEvent,
        election_timer: &mut std::pin::Pin<Box<tokio::time::Sleep>>,
    ) {
        match event {
            InternalEvent::Message(RaftMessage::RequestVote { args, reply_tx }) => {
                let reply = self.node.handle_request_vote(args);
                let _ = reply_tx.send(reply);
            }
            
            InternalEvent::Message(RaftMessage::AppendEntries { args, reply_tx }) => {
                let reply = self.node.handle_append_entries(args);
                let _ = reply_tx.send(reply);
                
                if self.node.role == Role::Follower {
                    election_timer.as_mut().reset(Instant::now() + self.random_election_timeout());
                }
            }
            
            InternalEvent::Message(RaftMessage::ClientCommand { req, reply_tx }) => {
                if self.node.role != Role::Leader {
                    let _ = reply_tx.send(ClientResponse::NotLeader {
                        leader_id: self.node.leader_id,
                    });
                    return;
                }

                let entry = crate::raft::LogEntry {
                    term: self.node.current_term,
                    request: req,
                };
                
                self.node.log.push(entry);
                self.node.storage.append_log_entries(&[self.node.log.last().unwrap().clone()]).unwrap();
                
                self.broadcast_heartbeats().await;
                let _ = reply_tx.send(ClientResponse::Success(None));
            }
            
            InternalEvent::VoteResult(reply) => {
                if reply.term > self.node.current_term {
                    self.node.become_follower(reply.term, None);
                    return;
                }
                
                if self.node.role == Role::Candidate && reply.vote_granted {
                    self.node.votes_received += 1;
                    let quorum = (self.peers.len() + 1) / 2 + 1;
                    
                    if self.node.votes_received >= quorum {
                        let cluster = self.peers.clone();
                        self.node.become_leader(&cluster);
                        self.broadcast_heartbeats().await;
                    }
                }
            }
            
            InternalEvent::HeartbeatResult(peer_id, reply) => {
                if reply.term > self.node.current_term {
                    self.node.become_follower(reply.term, None);
                    return;
                }
                
                if self.node.role == Role::Leader {
                    if reply.success {
                        let match_idx = *self.node.next_index.get(&peer_id).unwrap_or(&1) - 1;
                        self.node.match_index.insert(peer_id, match_idx);
                        self.node.next_index.insert(peer_id, match_idx + 1);
                    } else {
                        let next_idx = *self.node.next_index.get(&peer_id).unwrap_or(&1);
                        if next_idx > 1 {
                            self.node.next_index.insert(peer_id, next_idx - 1);
                        }
                    }
                }
            }
        }
    }

    async fn start_election(&self) {
        let args = RequestVoteArgs {
            term: self.node.current_term,
            candidate_id: self.node.id,
            last_log_index: self.node.log.len() as u64,
            last_log_term: self.node.log.last().map(|e| e.term).unwrap_or(0),
        };

        for peer in &self.peers {
            let transport = Arc::clone(&self.transport);
            let sender = self.sender.clone();
            let args_clone = args.clone();
            let peer_id = *peer;

            tokio::spawn(async move {
                if let Ok(reply) = transport.send_request_vote(peer_id, args_clone).await {
                    let _ = sender.send(InternalEvent::VoteResult(reply)).await;
                }
            });
        }
    }

    async fn broadcast_heartbeats(&self) {
        for peer in &self.peers {
            let prev_log_index = *self.node.next_index.get(peer).unwrap_or(&1) - 1;
            
            let prev_log_term = if prev_log_index > 0 && prev_log_index <= self.node.log.len() as u64 {
                self.node.log[(prev_log_index - 1) as usize].term
            } else {
                0
            };

            let entries = if prev_log_index < self.node.log.len() as u64 {
                self.node.log[(prev_log_index as usize)..].to_vec()
            } else {
                Vec::new()
            };

            let args = AppendEntriesArgs {
                term: self.node.current_term,
                leader_id: self.node.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.node.commit_index,
            };

            let transport = Arc::clone(&self.transport);
            let sender = self.sender.clone();
            let peer_id = *peer;

            tokio::spawn(async move {
                if let Ok(reply) = transport.send_append_entries(peer_id, args).await {
                    let _ = sender.send(InternalEvent::HeartbeatResult(peer_id, reply)).await;
                }
            });
        }
    }

    fn random_election_timeout(&self) -> Duration {
        let mut rng = rand::thread_rng();
        Duration::from_millis(rng.gen_range(150..300))
    }
}
