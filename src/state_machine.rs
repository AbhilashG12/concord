use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use crate::command::{Command, ClientRequest};
use crate::error::{Result, ConcordError};

#[derive(Debug, Clone, PartialEq)]
pub enum WatchEvent {
    Put(String, Vec<u8>),
    Delete(String),
}

#[derive(Debug, Default)]
pub struct StateMachine {
    store : HashMap<String,Vec<u8>>,
    last_applied_index : u64,
    client_sessions : HashMap<String,u64>,

    lease_to_keys: HashMap<u64, HashSet<String>>,
    key_to_lease: HashMap<String, u64>,
    watchers: HashMap<String, Vec<mpsc::Sender<WatchEvent>>>,
}

impl StateMachine {
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
            last_applied_index: 0,
            client_sessions: HashMap::new(),
            lease_to_keys: HashMap::new(),
            key_to_lease: HashMap::new(),
            watchers: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.store.get(key).cloned()
    }

    pub fn watch(&mut self, key: String, sender: mpsc::Sender<WatchEvent>) {
        self.watchers.entry(key).or_insert_with(Vec::new).push(sender);
    }

    fn notify_watchers(&mut self, key: &str, event: WatchEvent) {
        if let Some(channels) = self.watchers.get_mut(key) {
            // Retain only open channels (removes disconnected clients)
            channels.retain(|tx| tx.try_send(event.clone()).is_ok());
        }
    }

    pub fn apply(&mut self, log_index: u64, request: ClientRequest) -> Result<bool> {
        if log_index <= self.last_applied_index {
            return Err(ConcordError::NonMonotonicLogIndex(log_index));
        }

        let highest_seen = self.client_sessions.get(&request.client_id).copied().unwrap_or(0);
        if request.sequence_number <= highest_seen {
            self.last_applied_index = log_index;
            return Ok(true); // Duplicate, safely ignored
        }

        let mut success = true;

        match request.command {
            Command::Put { key, value, lease_id } => {
                self.store.insert(key.clone(), value.clone());
                self.attach_lease(&key, lease_id);
                self.notify_watchers(&key, WatchEvent::Put(key.clone(), value));
            }
            
            Command::PutIfAbsent { key, value, lease_id } => {
                if !self.store.contains_key(&key) {
                    self.store.insert(key.clone(), value.clone());
                    self.attach_lease(&key, lease_id);
                    self.notify_watchers(&key, WatchEvent::Put(key.clone(), value));
                } else {
                    success = false; // Lock acquisition failed
                }
            }
            
            Command::Delete { key } => {
                if self.store.remove(&key).is_some() {
                    self.detach_lease(&key);
                    self.notify_watchers(&key, WatchEvent::Delete(key.clone()));
                }
            }
            
            Command::GrantLease { lease_id } => {
                self.lease_to_keys.entry(lease_id).or_insert_with(HashSet::new);
            }
            
            Command::RevokeLease { lease_id } => {
                // DETERMINISTIC EXPIRATION:
                // The Raft log dictates the lease died. We safely delete all attached data.
                if let Some(keys) = self.lease_to_keys.remove(&lease_id) {
                    for key in keys {
                        self.store.remove(&key);
                        self.key_to_lease.remove(&key);
                        self.notify_watchers(&key, WatchEvent::Delete(key.clone()));
                    }
                }
            }
        }

        self.client_sessions.insert(request.client_id, request.sequence_number);
        self.last_applied_index = log_index;
        Ok(success)
    }

    fn attach_lease(&mut self, key: &str, lease_id: Option<u64>) {
        self.detach_lease(key); // Remove any old lease
        if let Some(lid) = lease_id {
            self.key_to_lease.insert(key.to_string(), lid);
            self.lease_to_keys.entry(lid).or_insert_with(HashSet::new).insert(key.to_string());
        }
    }

    fn detach_lease(&mut self, key: &str) {
        if let Some(lid) = self.key_to_lease.remove(key) {
            if let Some(keys) = self.lease_to_keys.get_mut(&lid) {
                keys.remove(key);
            }
        }
    }

    pub fn last_applied_index(&self) -> u64 {
        self.last_applied_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_put_request(client_id: &str, seq: u64, key: &str, val: &str) -> ClientRequest {
        ClientRequest {
            client_id: client_id.to_string(),
            sequence_number: seq,
            command: Command::Put {
                key: key.to_string(),
                value: val.as_bytes().to_vec(),
                lease_id: None, // Fixed: Added missing lease_id for Phase 1 tests
            },
        }
    }

    fn req(seq: u64, cmd: Command) -> ClientRequest {
        ClientRequest { client_id: "test".to_string(), sequence_number: seq, command: cmd }
    }

    #[test]
    fn test_basic_apply_and_get() {
        let mut sm = StateMachine::new();
        
        let req = create_put_request("client-1", 1, "config/node", "10.0.0.5");
        sm.apply(1, req).unwrap();

        assert_eq!(sm.last_applied_index(), 1);
        assert_eq!(sm.get("config/node").unwrap(), b"10.0.0.5");
    }

    #[test]
    fn test_strict_monotonic_log_index() {
        let mut sm = StateMachine::new();
        
        sm.apply(5, create_put_request("client-1", 1, "A", "1")).unwrap();
        
        let err_same = sm.apply(5, create_put_request("client-2", 1, "B", "2")).unwrap_err();
        let err_older = sm.apply(4, create_put_request("client-2", 2, "C", "3")).unwrap_err();

        assert_eq!(err_same, ConcordError::NonMonotonicLogIndex(5));
        assert_eq!(err_older, ConcordError::NonMonotonicLogIndex(4));
    }

    #[test]
    fn test_idempotency_duplicate_client_requests() {
        let mut sm = StateMachine::new();
        sm.apply(1, create_put_request("client-1", 1, "A", "first_value")).unwrap();
        assert_eq!(sm.get("A").unwrap(), b"first_value");

        sm.apply(2, create_put_request("client-1", 1, "A", "overwritten_value")).unwrap();

        assert_eq!(sm.last_applied_index(), 2);
        assert_eq!(sm.get("A").unwrap(), b"first_value");

        sm.apply(3, create_put_request("client-1", 2, "A", "new_value")).unwrap();
        assert_eq!(sm.get("A").unwrap(), b"new_value");
    }

    #[test]
    fn test_determinism_across_nodes() {
        let mut node_a = StateMachine::new();
        let mut node_b = StateMachine::new();

        let log = vec![
            (1, create_put_request("client-1", 1, "A", "10")),
            (2, create_put_request("client-2", 1, "B", "20")),
            (3, create_put_request("client-1", 1, "A", "10")),
            (4, ClientRequest {
                client_id: "client-2".to_string(),
                sequence_number: 2,
                command: Command::Delete { key: "A".to_string() },
            }),
        ];

        for (index, req) in log.clone() {
            node_a.apply(index, req).unwrap();
        }

        for (index, req) in log {
            node_b.apply(index, req).unwrap();
        }

        assert_eq!(node_a.last_applied_index(), node_b.last_applied_index());
        assert_eq!(node_a.get("B"), node_b.get("B"));
        assert_eq!(node_a.get("A"), None);
        assert_eq!(node_b.get("A"), None);
    }

    #[test]
    fn test_distributed_lock_semantics() {
        let mut sm = StateMachine::new();
        
        let success = sm.apply(1, req(1, Command::PutIfAbsent { 
            key: "lock/worker".to_string(), value: b"client_a".to_vec(), lease_id: None 
        })).unwrap();
        assert!(success);
        assert_eq!(sm.get("lock/worker").unwrap(), b"client_a");

        let success = sm.apply(2, req(2, Command::PutIfAbsent { 
            key: "lock/worker".to_string(), value: b"client_b".to_vec(), lease_id: None 
        })).unwrap();
        assert!(!success); 
        assert_eq!(sm.get("lock/worker").unwrap(), b"client_a"); 
    }

    #[test]
    fn test_deterministic_lease_revocation() {
        let mut sm = StateMachine::new();
        
        sm.apply(1, req(1, Command::GrantLease { lease_id: 100 })).unwrap();
        
        sm.apply(2, req(2, Command::Put { key: "service/api-1".to_string(), value: b"10.0.0.1".to_vec(), lease_id: Some(100) })).unwrap();
        sm.apply(3, req(3, Command::Put { key: "service/api-2".to_string(), value: b"10.0.0.2".to_vec(), lease_id: Some(100) })).unwrap();

        assert!(sm.get("service/api-1").is_some());
        
        sm.apply(4, req(4, Command::RevokeLease { lease_id: 100 })).unwrap();
        
        assert!(sm.get("service/api-1").is_none());
        assert!(sm.get("service/api-2").is_none());
    }

    #[tokio::test]
    async fn test_watch_notifications() {
        let mut sm = StateMachine::new();
        let (tx, mut rx) = mpsc::channel(10);
        
        sm.watch("config/db".to_string(), tx);
        
        sm.apply(1, req(1, Command::Put { key: "config/db".to_string(), value: b"postgres://...".to_vec(), lease_id: None })).unwrap();
        
        let event = rx.recv().await.unwrap();
        assert_eq!(event, WatchEvent::Put("config/db".to_string(), b"postgres://...".to_vec()));
    }
}