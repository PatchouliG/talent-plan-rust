use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub type FileId = u64;
pub type FileOffset = u64;
pub type BucketId = u64;

const BUCKET_NUMBER: u64 = 13;

pub fn toBucketId(s: &str) -> BucketId {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish() % BUCKET_NUMBER
}

#[test]
fn testHash() {
    let s = "sdf";
    assert_eq!(toBucketId(s), 3);
}

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
