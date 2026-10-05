use crate::error::Result;
use crate::raft::{LogEntry, NodeId};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::Path;

pub trait Storage {
    fn save_metadata(&mut self, term: u64, voted_for: Option<NodeId>)-> Result<()>;
    fn read_metadata(&mut self) -> Result<(u64, Option<NodeId>)>;

    fn append_log_entries(&mut self, entries: &[LogEntry]) -> Result<()>;
    fn read_log(&mut self) -> Result<Vec<LogEntry>>;
    fn truncate_log(&mut self, index: u64) -> Result<()>;
}


pub struct DiskStorage {
    meta_file : File,
    log_file : File,
}

impl Storage for DiskStorage {
    fn save_metadata(&mut self, term: u64, voted_for: Option<NodeId>)->Result<()>{
        self.meta_file.seek(SeekFrom::Start(0)).unwrap();
        self.meta_file.set_len(0).unwrap();

        let data = serde_json::to_string(&(term, voted_for)).unwrap();
        self.meta_file.write_all(data.as_bytes()).unwrap();

        self.meta_file.sync_all().unwrap();
        Ok(())
    }

    fn read_metadata(&mut self)->Result<(u64, Option<NodeId>)>{
        self.meta_file.seek(SeekFrom::Start(0)).unwrap();
        let mut data = String::new();
        self.meta_file.read_to_string(&mut data).unwrap();
        if data.is_empty() {
            return Ok((0, None)); 
        }
        let parsed = serde_json::from_str(&data).unwrap();
        Ok(parsed)
    }

    fn append_log_entries(&mut self, entries: &[LogEntry])->Result<()>{
        if entries.is_empty() {return Ok(());}
        for entry in entries {
            let data = serde_json::to_string(entry).unwrap();
            writeln!(self.meta_file,"{}",data).unwrap();
        }
        self.log_file.sync_all().unwrap();
        Ok(())
    }

    fn read_log(&mut self) -> Result<Vec<LogEntry>>{
        self.log_file.seek(SeekFrom::Start(0)).unwrap();
        let mut data = String::new();
        self.log_file.read_to_string(&mut data).unwrap();
        
        let mut entries = Vec::new();
        for line in data.lines() {
            if !line.is_empty() {
                entries.push(serde_json::from_str(line).unwrap());
            }
        }
        Ok(entries)
    }

    fn truncate_log(&mut self, index: u64) -> Result<()> {
        let mut entries = self.read_log()?;
        entries.truncate(index as usize);
        
        self.log_file.set_len(0).unwrap(); 
        self.log_file.seek(SeekFrom::Start(0)).unwrap();
        self.append_log_entries(&entries)?;
        Ok(())
    }
}

#[cfg(test)]
pub mod mock {
    use super::*;

    pub struct MockStorage {
        pub term: u64,
        pub voted_for: Option<NodeId>,
        pub log: Vec<LogEntry>,
    }

    impl Storage for MockStorage {
        fn save_metadata(&mut self, term: u64, voted_for: Option<NodeId>) -> Result<()> {
            self.term = term;
            self.voted_for = voted_for;
            Ok(())
        }
        fn read_metadata(&mut self) -> Result<(u64, Option<NodeId>)> { Ok((self.term, self.voted_for)) }
        fn append_log_entries(&mut self, entries: &[LogEntry]) -> Result<()> {
            self.log.extend_from_slice(entries); Ok(())
        }
        fn read_log(&mut self) -> Result<Vec<LogEntry>> { Ok(self.log.clone()) }
        fn truncate_log(&mut self, index: u64) -> Result<()> {
            self.log.truncate(index as usize); Ok(())
        }
    }
}
