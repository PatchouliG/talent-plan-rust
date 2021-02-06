use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;

pub type FileId = u64;
pub type FileOffset = u64;
pub type BucketId = u64;

pub type Result<T> = std::result::Result<T, failure::Error>;
pub type DBItem = (String, String);

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Get(String),
    Set(String, String),
    Remove(String),
}

impl Command {
    pub fn toString(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
    pub fn fromString(s: &str) -> Command {
        let a: Command = serde_json::from_str(s).unwrap();
        a
    }
}
