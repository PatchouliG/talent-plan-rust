use std::path::Path;
use std::sync::{Arc, Mutex};

use failure::_core::cell::RefCell;

use db::common::Command;
use db::db_file::DBFile;
use db::db_file::DBIter;
use db::index::DBIndex;
use db::worker::RequestWorker;

use crate::db::file_manager::FileManager;
use crate::db::worker::CompactorWorker;

mod db;

pub type Result<T> = db::common::Result<T>;

pub struct KvStore {
    // m: HashMap<String, usize>,
    // db: RefCell<DBFile>,
    worker: RequestWorker
}


impl KvStore {
    pub fn open(work_dir: &Path) -> Result<KvStore> {
        let (fm, sx) = FileManager::new(work_dir);
        let index = DBIndex::new();
        let fmLock = Arc::new(Mutex::new(fm));
        let indexMutex = Arc::new(Mutex::new(index));
        let worker = RequestWorker::new(fmLock.clone(), indexMutex.clone());
        CompactorWorker::new(fmLock.clone(), indexMutex.clone());
        Ok(KvStore { worker })
    }
    pub fn get(&self, key: String) -> Result<Option<String>> {
        self.worker.handle_get(&key)
    }
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.worker.handle_set(&key, &value)
    }
    pub fn remove(&mut self, key: String) -> Result<()> {
        // self.m.remove(&key).ok_or(failure::err_msg("Key not found"))?;
        //
        // let command = Command::Remove(key);
        // self.db.borrow_mut().write(&command.toString())?;
        // Result::Ok(())
        self.worker.handle_rm(&key)
        // unimplemented!()
    }
}
