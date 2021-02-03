use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::db::common::{BucketId, Command, FileOffset};
use crate::db::common::toBucketId;
use crate::db::db_file::DBIter;
use crate::db::file_manager::{FileManager, ValueIndex};

// value bone if rm
type Map = HashMap<String, ValueIndex>;
type KeyOperationSetFromRequestWork = HashSet<String>;

pub const BUCKET_SIZE: u64 = 13;

#[derive(Clone)]
pub struct DBIndex {
    buckets: Vec<Arc<Mutex<Map>>>
    // record key set/rm from request worker
    // keysLog: HashMap<BucketId, KeyOperationSetFromRequestWork>,
}

// not thread safe, need lock
impl DBIndex {
    pub fn new() -> DBIndex {
        let mut buckets: Vec<Arc<Mutex<Map>>> = Vec::new();
        for i in 0..BUCKET_SIZE {
            buckets.push(Arc::new(Mutex::new(Map::new())));
        }
        DBIndex { buckets }
    }
    pub fn getMap(&self, bId: BucketId) -> MutexGuard<Map> {
        self.buckets.get(bId as usize).unwrap().lock().unwrap()
    }
    pub fn set(&mut self, key: &str, fileOffset: FileOffset) {
        let bId = toBucketId(key);
        let mut m = self.getMap(bId);

        m.insert(key.to_owned(), fileOffset);
    }
    pub fn get(&self, key: &str) -> Option<ValueIndex> {
        let bId = toBucketId(key);
        let m = self.getMap(bId);
        m.get(key).cloned()
    }
    // return true if remove success
    pub fn rm(&mut self, key: &str) -> bool {
        let bId = toBucketId(key);
        let mut m = self.getMap(bId);
        m.remove(key).is_some()
    }

    pub fn load(&mut self, bId: BucketId, iter: DBIter) {
        let mut m = self.getMap(bId);
        for (a, b) in iter {
            let c = serde_json::from_str::<Command>(&a).unwrap();
            match c {
                Command::Remove(key) => { m.remove(&key); }
                Command::Set(key, value) => {
                    m.insert(key, b);
                }
                // ignore others
                _ => {}
            }
        }
    }

    fn test(self) {
        let mut m = self.getMap(10);
        for i in 1..100 {
            m.insert(i.to_string(), i);
        }
    }
}

#[cfg(test)]
mod testIndex {
    use crate::db::common::toBucketId;
    use crate::db::index::DBIndex;

    #[test]
    fn testCallFromRequest() {
        let mut d = DBIndex::new();
        d.set("1", 1);
        d.set("2", 2);
        assert_eq!(d.get("2").unwrap(), 2);
        assert_eq!(d.get("1").unwrap(), 1);

        d.rm("1");
        assert_eq!(d.get("1").is_none(), true);
    }


    #[test]
    fn testCompact() {
        let mut d = DBIndex::new();
        let mut d1 = d.clone();
        let mut d2 = d.clone();
        let h1 = std::thread::spawn(move || {
            d1.set("1", 1);
            d1.set("2", 2);
            d1.set("3", 3);
        }
        );
        h1.join();
        let h2 = std::thread::spawn(move || {
            let b = toBucketId("2");
            let mut m = d2.getMap(b);
            let keys = m.iter().map(|(s, _)| s.clone()).collect::<Vec<String>>();
            for key in keys {
                m.insert(key, 22);
            }
        });

        h2.join();

        assert_eq!(d.get("1").unwrap(), 1);
        assert_eq!(d.get("2").unwrap(), 22);
        assert_eq!(d.get("3").unwrap(), 3);


    }
}
