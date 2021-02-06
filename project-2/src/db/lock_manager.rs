use crate::db::file_manager::{FileManagerLock, FileManager};
use crate::db::index::{DBIndexLock, DBIndex};
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Clone)]
pub struct LockManager {
    fmMutex: FileManagerLock,
    indexMutex: DBIndexLock,
}

impl LockManager {
    fn buildLock<T>(t: T) -> Arc<Mutex<T>> {
        Arc::new(Mutex::new(t))
    }
    pub fn new(fm: FileManager, index: DBIndex) -> LockManager {
        LockManager { fmMutex: LockManager::buildLock(fm), indexMutex: LockManager::buildLock(index) }
    }

    pub fn get(&self) -> (MutexGuard<FileManager>, MutexGuard<DBIndex>) {
        (self.fmMutex.lock().unwrap(), self.indexMutex.lock().unwrap())
    }
}
