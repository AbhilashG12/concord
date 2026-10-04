use std::collections::HashMap;
use crate::command::{Command, ClientRequest};
use crate::error::{Result,ConcordError};

#[derive(Debug, Default)]
pub struct StateMachine {
    
    store : HashMap<String,Vec<u8>>,
    last_applied_index : u64,
    client_sessions : HashMap<String,u64>
}

impl StateMachine {
    pub fn new()->Self{
        Self{
            store:HashMap::new(),
            last_applied_index:0,
            client_sessions: HashMap::new(),
        }
    }

    pub fn get(&self, key:&str) -> Option<Vec<u8>> {
        self.store.get(key).cloned()
    }

    pub fn apply(&mut self, log_index:u64, request:ClientRequest) -> Result<()>{
        if log_index <= self.last_applied_index {
            return Err(ConcordError::NonMonotonicLogIndex(log_index));
        }

    let highest_seen_seq = self.client_sessions.get(&request.client_id).copied().unwrap_or(0);
    if request.sequence_number <= highest_seen_seq {
            self.last_applied_index = log_index;
            return Ok(());
        }

    match request.command {
        Command::Put {key,value} => {
            self.store.insert(key,value);
        }
        Command::Delete {key} => {
            self.store.remove(&key);
        }
    }

        self.client_sessions.insert(request.client_id,request.sequence_number);
        self.last_applied_index = log_index;
        Ok(())
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
            },
        }
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
}
