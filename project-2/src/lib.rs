use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use failure::_core::cell::RefCell;

use db::common::Command;
use db::db_file::DBFile;
use db::db_file::DBIter;
use db::index::DBIndex;

use crate::db::file_manager::{FileManager, FileManagerLock};
use crate::db::index::DBIndexLock;
use crate::db::request_worker::RequestWorker;
use crate::db::compacte_worker::CompactorWorker;
use crate::db::lock_manager::LockManager;

mod db;

pub type Result<T> = db::common::Result<T>;

pub struct KvStore {
    // fmMutex: FileManagerLock,
    // indexMutex: DBIndexLock,
    lm: LockManager
}


impl KvStore {
    pub fn new(work_dir: &Path) -> Result<KvStore> {
        KvStore::open(work_dir)
    }
    pub fn open(work_dir: &Path) -> Result<KvStore> {
        let fm = FileManager::new(work_dir);
        let mut index = DBIndex::new();
        fm.load(&mut index);
        let lm = LockManager::new(fm, index);
        Ok(KvStore { lm })
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        let (a, b) = self.lm.get();
        RequestWorker::new(a, b).handle_get(&key)
    }
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let (a, b) = self.lm.get();
        RequestWorker::new(a, b).handle_set(&key, &value)
    }
    pub fn remove(&mut self, key: String) -> Result<()> {
        let (a, b) = self.lm.get();
        RequestWorker::new(a, b).handle_rm(&key)
    }
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use crate::KvStore;

    #[test]
    fn testOperation() {
        let tmpDir = TempDir::new().unwrap();
        let mut kvs = KvStore::new(tmpDir.path()).unwrap();
        kvs.set("a".to_owned(), "a".to_owned()).unwrap();
        kvs.set("b".to_owned(), "b".to_owned()).unwrap();
        kvs.set("c".to_owned(), "c".to_owned()).unwrap();
        let res = kvs.get("b".to_owned()).unwrap().unwrap();
        assert_eq!(res, "b");
        kvs.remove("b".to_owned()).unwrap();
        let res = kvs.get("b".to_owned()).unwrap();
        assert_eq!(res.is_none(), true);
    }

    #[test]
    fn testLoad() {
        let tmpDir = TempDir::new().unwrap();
        let mut kvs = KvStore::new(tmpDir.path()).unwrap();
        kvs.set("a".to_owned(), "a".to_owned()).unwrap();
        kvs.set("b".to_owned(), "b".to_owned()).unwrap();
        kvs.set("c".to_owned(), "c".to_owned()).unwrap();
        kvs.remove("b".to_owned()).unwrap();
        drop(kvs);

        let mut kvs = KvStore::new(tmpDir.path()).unwrap();
        let res = kvs.get("a".to_owned()).unwrap().unwrap();
        assert_eq!(res, "a");
        let res = kvs.get("b".to_owned()).unwrap();
        assert_eq!(res.is_none(), true);
    }
}
