use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::db::common::{BucketId, FileOffset, Command};
use crate::db::common::toBucketId;
use crate::db::file_manager::{FileManager, ValueIndex};
use crate::db::db_file::DBIter;

// value bone if rm
type Map = HashMap<String, ValueIndex>;
type KeyOperationSetFromRequestWork = HashSet<String>;

pub const BUCKET_SIZE: u64 = 13;

pub struct DBIndex {
    buckets: HashMap<BucketId, Map>,
    // record key set/rm from request worker
    keysLog: HashMap<BucketId, KeyOperationSetFromRequestWork>,
}

// not thread safe, need lock
impl DBIndex {
    pub fn new() -> DBIndex {
        let mut buckets = HashMap::new();
        let mut keysLog = HashMap::new();
        for i in 0..BUCKET_SIZE {
            buckets.insert(i, Map::new());
            keysLog.insert(i, KeyOperationSetFromRequestWork::new());
        }
        DBIndex { buckets, keysLog }
    }
    pub fn set(&mut self, key: &str, valueIndex: ValueIndex) {
        let bId = toBucketId(key);
        let m = self.getMap(bId);
        m.insert(key.to_owned(), valueIndex);
        let kl = self.getKeyLog(bId);
        kl.insert(key.to_owned());
    }
    pub fn get(&self, key: &str) -> Option<&ValueIndex> {
        let bId = toBucketId(key);
        let m = self.buckets.get(&bId).unwrap();
        m.get(key)
    }
    // return true if remove success
    pub fn rm(&mut self, key: &str) -> bool {
        let bId = toBucketId(key);
        let m = self.getMap(bId);
        m.remove(key).is_some()
    }

    // used by compactor
    pub fn updateIndex(&mut self, key: &str, index: ValueIndex) {
        let bId = toBucketId(key);
        let k = self.getKeyLog(bId);
        if !k.contains(key) {
            let m = self.getMap(bId);
            m.insert(key.to_owned(), index);
        }
    }
    pub fn resetKeyLog(&mut self, bId: BucketId) {
        self.getKeyLog(bId).clear();
    }
    pub fn load(&mut self, bId: BucketId, iter: DBIter) {
        let m = self.buckets.get_mut(&bId).unwrap();
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
    fn getKeyLog(&mut self, bId: BucketId) -> &mut KeyOperationSetFromRequestWork {
        self.keysLog.get_mut(&bId).unwrap()
    }
    fn getMap(&mut self, bId: BucketId) -> &mut Map {
        self.buckets.get_mut(&bId).unwrap()
    }
}

#[cfg(test)]
mod testIndex {
    use crate::db::file_manager::ValueIndex;
    use crate::db::index::DBIndex;
    use crate::db::common::toBucketId;

    #[test]
    fn testCallFromRequest() {
        let mut d = DBIndex::new();
        d.set("1", 1);
        d.set("2", 2);
        assert_eq!(*d.get("2").unwrap(), 2);
        assert_eq!(*d.get("1").unwrap(), 1);

        d.rm("1");
        assert_eq!(d.get("1").is_none(), true);
    }

    #[test]
    fn testRequestFromBoth() {
        let mut d = DBIndex::new();
        d.set("1", 1);
        d.set("2", 2);
        d.set("3", 3);

        d.updateIndex("8", 8);
        assert_eq!(*d.get("8").unwrap(), 8);

        // update index work
        d.updateIndex("8", 88);
        assert_eq!(*d.get("8").unwrap(), 88);

        // update index from compact won't take effect, it's set from request worker
        d.updateIndex("3", 111111);
        assert_eq!(*d.get("3").unwrap(), 3);

        let bid = toBucketId("3");
        d.resetKeyLog(bid);

        // it work after reset
        d.updateIndex("3", 111);
        assert_eq!(*d.get("3").unwrap(), 111);

        d.set("3", 33);
        assert_eq!(*d.get("3").unwrap(), 33);
    }
}
