use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum ConcordError {
    #[error("Log Index {0} is out of order. Expected strictly monotonic increasing index.")]
    NonMonotonicLogIndex(u64),

    #[error("Key not found : {0}")]
    KeyNotFound(String),
}


pub type Result<T> = std::result::Result<T,ConcordError>;
