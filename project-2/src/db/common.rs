use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, failure::Error>;

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

pub struct ValueIndex {}