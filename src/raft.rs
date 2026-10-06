use serde::{Serialize, Deserialize};
use crate::command::ClientRequest;
use crate::storage::Storage;
use std::collections::HashMap;

pub type NodeId = u64;

#[derive(Debug,Clone, Copy, PartialEq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

pub struct Snapshot {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Vec<u8>,
}

pub struct InstallSnapshotArgs{
    pub term: u64,
    pub leader_id: NodeId,
    pub last_included_index: u64,
    pub last_included_term:u64,
    pub data: Vec<u8>,
}

pub struct InstallSnapshotReply{
    pub term: u64,
}
#[derive(Debug, Clone, PartialEq)]
pub enum ClusterConfig {
    Single(Vec<NodeId>),
    Joint { old: Vec<NodeId>, new: Vec<NodeId> },
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogPayload {
    Command(ClientRequest),
    ConfigChange(ClusterConfig), 
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

    pub storage: Box<dyn Storage>,


    pub last_included_index: u64,
    pub last_included_term: u64,

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

#[derive(Clone)]
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
    pub fn recover(id:NodeId, mut storage: Box<dyn Storage>) -> Self {
        let (current_term, voted_for) = storage.read_metadata().unwrap_or((0, None));
        let log = storage.read_log().unwrap_or_else(|_| Vec::new());
        Self {
            id,
            role : Role::Follower,
            current_term,
            voted_for,
            log,
            commit_index: 0,
            last_applied : 0,
            leader_id: None,
            votes_received:0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            storage,
        }
    }

    fn array_index(&self, global_index: u64) -> Option<usize> {
        if global_index <= self.last_included_index {
            return None; 
        }
        Some((global_index - self.last_included_index - 1) as usize)
    }

    fn term_at(&self, global_index: u64) -> u64 {
        if global_index == self.last_included_index {
            return self.last_included_term;
        }
        if let Some(idx) = self.array_index(global_index) {
            if idx < self.log.len() {
                return self.log[idx].term;
            }
        }
        0
    }

    pub fn become_follower(&mut self, term:u64, leader_id:Option<NodeId>){
        self.role = Role::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.storage.save_metadata(self.current_term, self.voted_for).unwrap();
        self.leader_id = leader_id;
        self.votes_received = 0;
    }

    pub fn become_candidate(&mut self){
        self.role = Role::Candidate;
        self.current_term +=1;
        self.voted_for = Some(self.id);
        self.storage.save_metadata(self.current_term, self.voted_for).unwrap();
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
        
    pub fn handle_install_snapshot(&mut self, args: InstallSnapshotArgs) -> InstallSnapshotReply {
        if args.term < self.current_term {
            return InstallSnapshotReply { term: self.current_term };
        }
        
        self.become_follower(args.term, Some(args.leader_id));

        if args.last_included_index <= self.last_included_index {
            return InstallSnapshotReply { term: self.current_term };
        }

        if let Some(idx) = self.array_index(args.last_included_index) {
            if idx < self.log.len() && self.log[idx].term == args.last_included_term {
                self.log.drain(0..=idx);
            } else {
                self.log.clear(); 
            }
        } else {
            self.log.clear(); 
        }

        self.last_included_index = args.last_included_index;
        self.last_included_term = args.last_included_term;
        
        self.commit_index = std::cmp::max(self.commit_index, args.last_included_index);
        self.last_applied = std::cmp::max(self.last_applied, args.last_included_index);
        
        InstallSnapshotReply { term: self.current_term }
    }
        
    pub fn create_snapshot(&mut self, compact_index: u64, state_machine_data: Vec<u8>) {
        // Reject if the index is already compacted or hasn't been committed yet.
        // We can only snapshot data that we know is mathematically finalized.
        if compact_index <= self.last_included_index || compact_index > self.commit_index {
            return;
        }

        let new_last_included_term = self.term_at(compact_index);

        // Calculate how many entries to remove from the front of the Vec
        if let Some(array_idx) = self.array_index(compact_index) {
            // Remove everything up to and including the compacted index
            self.log.drain(0..=array_idx);
        } else {
            self.log.clear(); // Edge case: compacting the entire log
        }

        self.last_included_index = compact_index;
        self.last_included_term = new_last_included_term;

        // DURABILITY BARRIER: 
        // 1. Save `state_machine_data` to a snapshot file on disk.
        // 2. Truncate the Write-Ahead Log file so disk space is actually freed.
        // self.storage.save_snapshot(compact_index, state_machine_data).unwrap();
    }

    pub fn is_committed(&self, index: u64, config: &ClusterConfig) -> bool {
        match config {
            ClusterConfig::Single(nodes) => {
                self.check_majority(nodes, index)
            }
            ClusterConfig::Joint { old, new } => {
                self.check_majority(old, index) && self.check_majority(new, index)
            }
        }
    }

    fn check_majority(&self, cluster: &[NodeId], target_index: u64) -> bool {
        let mut count = 0;
        for node in cluster {
            if *node == self.id {
                count += 1; 
            } else if let Some(&match_idx) = self.match_index.get(node) {
                if match_idx >= target_index {
                    count += 1;
                }
            }
        }
        count >= (cluster.len() / 2) + 1
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
            self.storage.save_metadata(self.current_term, self.voted_for).unwrap();
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
        let mut log_was_truncated = false;
        let mut new_entries = Vec::new();
        for entry in args.entries {
            current_index += 1;
            if current_index <= self.log.len() as u64 {
                if self.log[(current_index - 1) as usize].term != entry.term {
                    self.log.truncate((current_index - 1) as usize);
                    self.log.push(entry.clone());
                    log_was_truncated = true;
                    new_entries.push(entry);
                }
            } else {
                self.log.push(entry.clone());
                new_entries.push(entry);
            }
        }

        if log_was_truncated {
            self.storage.truncate_log(current_index - 1).unwrap();
        }

         if !new_entries.is_empty() {
            self.storage.append_log_entries(&new_entries).unwrap();
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
    use crate::command::{ClientRequest, Command};
    use crate::storage::mock::MockStorage;

    fn new_test_node(id: NodeId) -> RaftNode {
        let storage = Box::new(MockStorage { term: 0, voted_for: None, log: vec![] });
        RaftNode::recover(id, storage)
    }

    fn dummy_request() -> ClientRequest {
        ClientRequest { client_id: "test".to_string(), sequence_number: 1, command: Command::Delete { key: "x".to_string() } }
    }

    #[test]
    fn test_role_transitions() {
        let mut node = new_test_node(1); 
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.current_term, 0);

        node.become_candidate();
        assert_eq!(node.role, Role::Candidate);
        assert_eq!(node.current_term, 1);
        assert_eq!(node.voted_for, Some(1));

        node.become_leader(&[1, 2, 3]);
        assert_eq!(node.role, Role::Leader);
        assert_eq!(node.leader_id, Some(1));

        node.become_follower(2, Some(2));
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.current_term, 2);
        assert_eq!(node.voted_for, None); 
    }

    #[test]
    fn test_handle_request_vote_reject_older_term() {
        let mut node = new_test_node(1);
        node.current_term = 5;

        let reply = node.handle_request_vote(RequestVoteArgs {
            term: 4, 
            candidate_id: 2,
            last_log_index: 10,
            last_log_term: 5,
        });

        assert_eq!(reply.vote_granted, false);
    }

    #[test]
    fn test_handle_request_vote_grant_and_step_down() {
        let mut node = new_test_node(1);
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

    #[test]
    fn test_append_entries_heartbeat() {
        let mut node = new_test_node(1); 
        node.current_term = 2;
        
        let reply = node.handle_append_entries(AppendEntriesArgs {
            term: 2, leader_id: 2, prev_log_index: 0, prev_log_term: 0,
            entries: vec![], leader_commit: 0,
        });

        assert_eq!(reply.success, true);
    }

    #[test]
    fn test_append_entries_log_inconsistency() {
        let mut node = new_test_node(1); 
        node.log.push(LogEntry { term: 1, request: dummy_request() });

        let reply = node.handle_append_entries(AppendEntriesArgs {
            term: 2, leader_id: 2, prev_log_index: 2, prev_log_term: 2,
            entries: vec![], leader_commit: 0,
        });

        assert_eq!(reply.success, false); 
    }

    #[test]
    fn test_append_entries_conflict_truncation() {
        let mut node = new_test_node(1);
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

    #[test]
    fn test_crash_and_recovery() {
        let storage = Box::new(MockStorage { term: 0, voted_for: None, log: vec![] });
        let mut node = RaftNode::recover(1, storage); 

        node.become_candidate(); 
        node.storage.save_metadata(node.current_term, node.voted_for).unwrap();

        let saved_storage = node.storage;

        let rebooted_node = RaftNode::recover(1, saved_storage);

        assert_eq!(rebooted_node.current_term, 1);
        assert_eq!(rebooted_node.voted_for, Some(1));
        assert_eq!(rebooted_node.role, Role::Follower); 
    }
    use crate::raft::{ClusterConfig, RaftNode};

    #[test]
    fn test_array_index_translation() {
        let mut node = RaftNode::new(1);
        node.last_included_index = 100; // We have snapshotted the first 100 entries

        // Global index 100 is inside the snapshot, so it shouldn't map to the active log
        assert_eq!(node.array_index(100), None);
        
        // Global index 101 should be the very first item in our Rust Vec (index 0)
        assert_eq!(node.array_index(101), Some(0));
        
        // Global index 150 should be at Vec index 49
        assert_eq!(node.array_index(150), Some(49));
    }

    #[test]
    fn test_joint_consensus_majority() {
        let mut node = RaftNode::new(1); // We are Node 1
        node.match_index.insert(2, 50);  // Node 2 has up to index 50
        node.match_index.insert(3, 50);  // Node 3 has up to index 50
        node.match_index.insert(4, 10);  // Node 4 is lagging (index 10)
        node.match_index.insert(5, 10);  // Node 5 is lagging (index 10)

        // Target index to check: 50.
        // Node 1 (us), Node 2, and Node 3 have it. (Total 3 nodes have it).

        // SCENARIO 1: Old Configuration [1, 2, 3]
        let old_config = vec![1, 2, 3];
        assert_eq!(node.check_majority(&old_config, 50), true); // 3 out of 3 have it.

        // SCENARIO 2: New Configuration [1, 2, 3, 4, 5]
        let new_config = vec![1, 2, 3, 4, 5];
        assert_eq!(node.check_majority(&new_config, 50), true); // 3 out of 5 is a majority.

        // SCENARIO 3: Joint Configuration [1, 2, 3] + [1, 2, 3, 4, 5]
        let joint = ClusterConfig::Joint {
            old: old_config,
            new: new_config,
        };
        // Because BOTH old and new have independent majorities for index 50, it is committed!
        assert_eq!(node.is_committed(50, &joint), true);

        // SCENARIO 4: What if we check index 51? Nobody has it yet.
        assert_eq!(node.is_committed(51, &joint), false);
    }
}
