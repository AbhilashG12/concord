use serde::{Deserialize,Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Command {
    Put { key: String, value: Vec<u8>, lease_id: Option<u64> },
    Delete { key: String },
    
    PutIfAbsent { key: String, value: Vec<u8>, lease_id: Option<u64> },

    GrantLease { lease_id: u64 },
    RevokeLease { lease_id: u64 },
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClientRequest {
    pub client_id : String,
    pub sequence_number : u64,
    pub command : Command,
}
