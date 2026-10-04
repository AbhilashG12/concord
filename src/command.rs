use serde::{Deserialize,Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Command {
    Put {key: String, value: Vec<u8>},
    Delete {key: String},
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClientRequest {
    pub client_id : String,
    pub sequence_number : u64,
    pub command : Command,
}
