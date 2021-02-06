use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::db::common::{BucketId, Command, FileId, FileOffset};
use crate::db::db_file::DBIter;
use crate::db::db_meta::FileMeta;
use crate::db::file_manager::{FileManager, Loader, ValueIndex};

type Map = HashMap<String, ValueIndex>;

#[derive(Copy, Clone)]
pub struct DBFileStatistic {
    id: FileId,
    totalItem: u64,
    deadItem: u64,
}

impl DBFileStatistic {
    pub fn new(id: FileId) -> DBFileStatistic {
        DBFileStatistic { id, totalItem: 0, deadItem: 0 }
    }
    pub fn usage(&self) -> f32 {
        1.0 - (self.deadItem as f32) / (self.totalItem as f32)
    }
}

pub struct DBIndex {
    map: Map,
    statistic: HashMap<FileId, DBFileStatistic>,
}

pub type DBIndexLock = Arc<Mutex<DBIndex>>;

fn newDBIndexLock() -> DBIndexLock {
    Arc::new(Mutex::new(DBIndex::new()))
}


// not thread safe, need lock
impl DBIndex {
    pub fn new() -> DBIndex {
        DBIndex { map: HashMap::new(), statistic: HashMap::new() }
    }
    pub fn set(&mut self, key: &str, valueIndex: ValueIndex) {
        self.map.insert(key.to_owned(), valueIndex);
        if !self.statistic.contains_key(&valueIndex.fileId) {
            self.statistic.insert(valueIndex.fileId, DBFileStatistic::new(valueIndex.fileId));
        }
        self.statistic.get_mut(&valueIndex.fileId).unwrap().totalItem += 1;
    }
    pub fn get(&self, key: &str) -> Option<ValueIndex> {
        self.map.get(key).map(|v| v.clone())
    }
    // return true if remove success
    pub fn rm(&mut self, key: &str) -> bool {
        let valueIndex = self.map.remove(key);
        if valueIndex.is_some() {
            let fId = valueIndex.unwrap().fileId;
            self.statistic.get_mut(&fId).map(|s| s.deadItem += 1);
        };
        valueIndex.is_some()
    }
    pub fn dbFileStatistic(&self) -> Vec<DBFileStatistic> {
        self.statistic.iter().map(|(key, value)| value.clone()).
            collect::<Vec<DBFileStatistic>>()
    }
}

impl Loader for DBIndex {
    fn load(&mut self, content: &str, index: ValueIndex) {
        let c = serde_json::from_str::<Command>(&content).unwrap();
        match c {
            Command::Set(key, _) => {
                self.set(&key, index);
            }
            Command::Remove(key) => {
                self.rm(&key);
            }
            // ignore
            Command::Get(_) => {}
        }
    }
}

#[cfg(test)]
mod testIndex {
    use std::collections::HashMap;

    use crate::db::common::{Command, FileId};
    use crate::db::file_manager::{Loader, ValueIndex};
    use crate::db::index::{DBFileStatistic, DBIndex};

    #[test]
    fn testLoad() {
        let source: Vec<(&str, ValueIndex)> = vec![("1", ValueIndex::new(1, 1)), ("2", ValueIndex::new(2, 2))];
        let mut index = DBIndex::new();

        for (key, value) in source {
            let content = serde_json::to_string(&Command::Set(key.to_owned(), key.to_owned())).unwrap();
            index.load(&content, value)
        }

        let res = index.get("1").unwrap();
        assert_eq!(res.offset, 1);
        assert_eq!(res.fileId, 1);
    }

    #[test]
    fn testOperation() {
        let mut index = DBIndex::new();
        index.set("a", ValueIndex::new(1, 1));
        index.set("b", ValueIndex::new(2, 1));
        let res = index.get("a").unwrap();
        assert_eq!(res.fileId, 1);
        assert_eq!(res.offset, 1);
        let res = index.rm("a");
        assert_eq!(res, true);

        let res = index.get("a");
        assert_eq!(res.is_none(), true);
    }

    #[test]
    fn testFileStatistic() {
        let mut index = DBIndex::new();
        index.set("a", ValueIndex::new(1, 1));
        index.set("b", ValueIndex::new(1, 1));
        index.set("c", ValueIndex::new(1, 1));
        index.set("d", ValueIndex::new(1, 1));
        index.rm("b");
        index.rm("b");
        index.set("e", ValueIndex::new(1, 2));

        let res = index.dbFileStatistic().iter().
            map(|f| (f.id, *f)).collect::<HashMap<FileId, DBFileStatistic>>();

        assert_eq!(res.get(&(1 as u64)).unwrap().usage(), 0.75);
        assert_eq!(res.get(&(2 as u64)).unwrap().usage(), 1.0);
    }
}
