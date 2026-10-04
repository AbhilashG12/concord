use crate::command::ClientRequest;
use std::collections::HashMap;
pub type NodeId = u64;

#[derive(Debug,Clone, Copy, PartialEq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry{
    pub term : u64,
    pub request : ClientRequest,
}

pub struct RaftNode {
    pub id : NodeId,
    pub role : Role,

    pub current_term : u64,
    pub voted_for : Option<NodeId>,
    pub log : Vec<LogEntry>,

    pub commit_index : u64,
    pub last_applied : u64,

    pub leader_id : Option<NodeId>,
    pub votes_received : usize,

    pub next_index : HashMap<NodeId,u64>,
    pub match_index: HashMap<NodeId,u64>,

}

pub struct AppendEntriesArgs {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
}

pub struct RequestVoteArgs {
    pub term : u64,
    pub candidate_id : NodeId,
    pub last_log_index : u64,
    pub last_log_term : u64,
}

pub struct RequestVoteReply {
    pub term : u64,
    pub vote_granted : bool,
}

impl RaftNode {
    pub fn new(id:NodeId) -> Self {
        Self {
            id,
            role : Role::Follower,
            current_term : 0,
            voted_for:None,
            log: Vec::new(),
            commit_index: 0,
            last_applied : 0,
            leader_id: None,
            votes_received:0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        }
    }

    pub fn become_follower(&mut self, term:u64, leader_id:Option<NodeId>){
        self.role = Role::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.leader_id = leader_id;
        self.votes_received = 0;
    }

    pub fn become_candidate(&mut self){
        self.role = Role::Candidate;
        self.current_term +=1;
        self.voted_for = Some(self.id);
        self.votes_received = 1;
        self.leader_id = None;
    }

    pub fn become_leader(&mut self,cluster_nodes: &[NodeId]){
        self.role = Role::Leader;
        self.leader_id = Some(self.id);

        let next_idx = (self.log.len() as u64) + 1;
        self.next_index.clear();
        self.match_index.clear();
        
        for &node_id in cluster_nodes {
            if node_id != self.id {
                self.next_index.insert(node_id, next_idx);
                self.match_index.insert(node_id, 0);
            }
        }
    }

    pub fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        if args.term < self.current_term {
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            };
        }
        if args.term > self.current_term {
            self.become_follower(args.term, None);
        }
        let last_log_index = self.log.len() as u64;
        let last_log_term = self.log.last().map(|e| e.term).unwrap_or(0);

        let is_log_up_to_date = args.last_log_term > last_log_term || 
            (args.last_log_term == last_log_term && args.last_log_index >= last_log_index);

        let can_vote = self.voted_for.is_none() || self.voted_for == Some(args.candidate_id);

        if is_log_up_to_date && can_vote {
            self.voted_for = Some(args.candidate_id);
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: true,
            };
        }

        RequestVoteReply {
            term: self.current_term,
            vote_granted: false,
        }
    }

    pub fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        if args.term < self.current_term {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
            };
        }

        self.become_follower(args.term, Some(args.leader_id));

        if args.prev_log_index > 0 {
            let log_len = self.log.len() as u64;
            if args.prev_log_index > log_len {
                return AppendEntriesReply { term: self.current_term, success: false };
            }
            if self.log[(args.prev_log_index - 1) as usize].term != args.prev_log_term {
                return AppendEntriesReply { term: self.current_term, success: false };
            }
        }

        let mut current_index = args.prev_log_index;
        for entry in args.entries {
            current_index += 1;
            if current_index <= self.log.len() as u64 {
                if self.log[(current_index - 1) as usize].term != entry.term {
                    self.log.truncate((current_index - 1) as usize);
                    self.log.push(entry);
                }
            } else {
                self.log.push(entry);
            }
        }

        if args.leader_commit > self.commit_index {
            let last_new_entry_index = self.log.len() as u64;
            self.commit_index = std::cmp::min(args.leader_commit, last_new_entry_index);
        }

        AppendEntriesReply {
            term: self.current_term,
            success: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_transitions() {
        let mut node = RaftNode::new(1);
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.current_term, 0);

        node.become_candidate();
        assert_eq!(node.role, Role::Candidate);
        assert_eq!(node.current_term, 1);
        assert_eq!(node.voted_for, Some(1));

        //node.become_leader();
        //assert_eq!(node.role, Role::Leader);
        //assert_eq!(node.leader_id, Some(1));

        node.become_follower(2, Some(2));
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.current_term, 2);
        assert_eq!(node.voted_for, None);
    }

    #[test]
    fn test_handle_request_vote_reject_older_term() {
        let mut node = RaftNode::new(1);
        node.current_term = 5;

        let reply = node.handle_request_vote(RequestVoteArgs {
            term: 4, 
            candidate_id: 2,
            last_log_index: 10,
            last_log_term: 5,
        });

        assert_eq!(reply.vote_granted, false);
        assert_eq!(reply.term, 5);
    }

    #[test]
    fn test_handle_request_vote_grant_and_step_down() {
        let mut node = RaftNode::new(1);
        node.current_term = 2;
        node.voted_for = Some(1);

        let reply = node.handle_request_vote(RequestVoteArgs {
            term: 3, 
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        });

        assert_eq!(reply.vote_granted, true);
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.current_term, 3);
        assert_eq!(node.voted_for, Some(2));
    }

    use crate::command::{ClientRequest, Command};

    fn dummy_request() -> ClientRequest {
        ClientRequest { client_id: "test".to_string(), sequence_number: 1, command: Command::Delete { key: "x".to_string() } }
    }

    #[test]
    fn test_append_entries_heartbeat() {
        let mut node = RaftNode::new(1);
        node.current_term = 2;
        
        let reply = node.handle_append_entries(AppendEntriesArgs {
            term: 2, leader_id: 2, prev_log_index: 0, prev_log_term: 0,
            entries: vec![], leader_commit: 0,
        });

        assert_eq!(reply.success, true);
        assert_eq!(node.leader_id, Some(2));
    }

    #[test]
    fn test_append_entries_log_inconsistency() {
        let mut node = RaftNode::new(1);
        node.log.push(LogEntry { term: 1, request: dummy_request() });

        let reply = node.handle_append_entries(AppendEntriesArgs {
            term: 2, leader_id: 2, prev_log_index: 2, prev_log_term: 2,
            entries: vec![], leader_commit: 0,
        });

        assert_eq!(reply.success, false); 
    }

    #[test]
    fn test_append_entries_conflict_truncation() {
        let mut node = RaftNode::new(1);
        node.log.push(LogEntry { term: 1, request: dummy_request() });
        node.log.push(LogEntry { term: 1, request: dummy_request() });

        let new_entry = LogEntry { term: 2, request: dummy_request() };
        let reply = node.handle_append_entries(AppendEntriesArgs {
            term: 2, leader_id: 2,
            prev_log_index: 1, prev_log_term: 1,
            entries: vec![new_entry],
            leader_commit: 0,
        });

        assert_eq!(reply.success, true);
        assert_eq!(node.log.len(), 2);
        assert_eq!(node.log[1].term, 2);
    }
}
