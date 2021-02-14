use std::collections::HashMap;
use std::sync::{Arc, mpsc, Mutex, MutexGuard};
use std::sync::mpsc::{Receiver, Sender};

use crate::db::common::{Command, FileId, BucketId, FileOffset};
use crate::db::db_file::{DBFile, DBIter};
use crate::db::file_manager::{ValueIndex, FileManagerLock};
use crate::db::index::{DBIndex, DBIndexLock, DBFileStatistic};

use super::common::Result;
use super::file_manager::FileManager;
use failure::_core::time::Duration;
use crate::db::lock_manager::LockManager;
use std::thread::JoinHandle;
use predicates::ord::ne;


const BUFFER_SIZE_THRESH: usize = 3;

pub struct CompactorWorker {
    lm: LockManager,
    checkInterval: Duration,
}

impl CompactorWorker {
    const USAGE_THRESHOLD: f32 = 0.5;
    const CHECK_INTERVAL_MS: u64 = 5000;

    pub fn start(lm: LockManager) {
        CompactorWorker::startDuration(lm, CompactorWorker::CHECK_INTERVAL_MS)
    }
    fn startDuration(lm: LockManager, ms: u64) {
        // start thread
        let mut res: CompactorWorker = CompactorWorker { lm, checkInterval: Duration::from_millis(ms) };
        std::thread::spawn(move || res.handle_compact());
    }
    pub fn handle_compact(self) {
        loop {
            std::thread::sleep(self.checkInterval);
            let (mut fm, mut index) = self.lm.get();
            let needCompact = index.dbFileStatistic().iter().
                filter(|fs| fs.usage() < CompactorWorker::USAGE_THRESHOLD && fm.isReadOnlyFile(fs.id())).
                map(|fs| (fs.id())).
                collect::<Vec<FileId>>();


            needCompact.iter().for_each(|fId| {
                let dbf = fm.idToFile(*fId);
                let iter = DBIter::new(&dbf);
                iter.into_iter().for_each(|(content, offset)| {
                    let c = serde_json::from_str::<Command>(&content).unwrap();
                    match c {
                        Command::Set(k, v) => {
                            let res = index.get(&k);
                            // write to fm, update index
                            if let Some(i) = res {
                                // only migrate if index point to the db file
                                if i.offset == offset && i.fileId == *fId {
                                    let newPosition = fm.writeToCurrent(&content).unwrap();
                                    index.set(&k, newPosition);
                                }
                            }
                        }
                        // ignore rm
                        _ => {}
                    };
                }
                );
                fm.deleteDBFIle(*fId);
            });
        }
    }
}

#[cfg(test)]
mod testCompact {
    use std::sync::{Arc, Mutex};
    use crate::db::file_manager::FileManager;
    use tempfile::TempDir;
    use crate::db::index::DBIndex;
    use crate::db::compacte_worker::CompactorWorker;
    use crate::db::common::{Command};
    use crate::db::lock_manager::LockManager;
    use crate::db::request_worker::RequestWorker;
    use failure::_core::time::Duration;

    #[test]
    fn testCompact() {
        // start compact thread
        // write finish,check db size

        let tmpDir = TempDir::new().unwrap();
        let mut fm = FileManager::newFmWithSize(tmpDir.path(), 4000);
        let mut index = DBIndex::new();
        let lm = LockManager::new(fm, index);

        CompactorWorker::startDuration(lm.clone(), 5);

        let key = "key".to_owned();
        let value = "23333333333333333333333333333333333333333333333333333333333333333333333";
        let content = serde_json::to_string(&Command::Set(key.to_owned(), value.to_owned())).unwrap();

        for i in 1..500 {
            let (a, b) = lm.get();
            // just 3 different key
            RequestWorker::new(a, b).handle_set(&(key.clone() + &(i % 20).to_string()), value).unwrap();
        }
        // wait compact start
        std::thread::sleep(Duration::from_millis(200));

        let (fm, _) = lm.get();
        let len = fm.getReadOnlyFiles().len();
        assert_eq!(len, 0)
    }
}

