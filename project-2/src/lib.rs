use std::path::Path;
use std::sync::{Arc, Mutex};

use failure::_core::cell::RefCell;

use db::common::Command;
use db::db_file::DBFile;
use db::db_file::DBIter;
use db::index::DBIndex;

use crate::db::file_manager::FileManager;
use crate::db::worker::CompactorWorker;
use crate::db::request_worker::RequestWorker;

mod db;

pub type Result<T> = db::common::Result<T>;

pub struct KvStore {
    // m: HashMap<String, usize>,
    // db: RefCell<DBFile>,
    worker: RequestWorker
}


impl KvStore {
    pub fn open(work_dir: &Path) -> Result<KvStore> {
        unimplemented!()
        // let fm = FileManager::new(work_dir);
        // let mut index = DBIndex::new();
        // for i in 0..BUCKET_SIZE {
        //     // let it = fm.getDBIter(i);
        //     // index.load(i, it);
        // }
        // let fmLock = Arc::new(Mutex::new(fm));
        // let worker = RequestWorker::new(fmLock.clone(), index.clone());
        // // CompactorWorker::new(fmLock.clone(), index.clone());
        // Ok(KvStore { worker })
    }
    pub fn get(&self, key: String) -> Result<Option<String>> {
        self.worker.handle_get(&key)
    }
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.worker.handle_set(&key, &value)
    }
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.worker.handle_rm(&key)
    }
}
