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


const BUFFER_SIZE_THRESH: usize = 3;

pub struct CompactorWorker {
    fmLock: FileManagerLock,
    indexLock: DBIndexLock,
    // need compact
    // compact_rx: Receiver<BucketId>,
    // buffer: Vec<FileId>,
}

impl CompactorWorker {
    const USAGE_THRESHOLD: f32 = 0.5;

    pub fn new(fm: FileManagerLock, index: DBIndexLock) {
        // start thread
        let mut res: CompactorWorker = CompactorWorker { fmLock: fm, indexLock: index };
        std::thread::spawn(move || res.handle_compact());
    }
    fn lockDB<>(&self) -> (MutexGuard<FileManager>, MutexGuard<DBIndex>) {
        let fm = self.fmLock.lock().unwrap();
        let index = self.indexLock.lock().unwrap();
        (fm, index)
    }
    pub fn handle_compact(self) {
        loop {
            std::thread::sleep(Duration::new(10, 0));
            let (fm, index) = self.lockDB();
            let dfs = index.dbFileStatistic();
            let needCompacts: Vec<&DBFileStatistic> = dfs.iter().
                filter(|f| f.usage() < CompactorWorker::USAGE_THRESHOLD).
                collect();
            needCompacts.iter().map(|f| {
                // let dbf = fm.idToDBFile(f.id);
                // DBIter::new(&dbf);
                unimplemented!()
                //     todo
            });
            // todo sleep for 10s
            //     fetch index status
            //     check usage
            //     do compact if necessary
        }
        unimplemented!()
    }
}

#[cfg(test)]
mod testCompact {
    use std::sync::{Arc, Mutex};
    use crate::db::file_manager::FileManager;
    use tempfile::TempDir;
    use crate::db::index::DBIndex;
    use crate::db::worker::CompactorWorker;
    use crate::db::common::{Command};

    #[test]
    fn testCompact() {
        // todo  test
        //  write to db  in main thread
        // start compact thread
        // write finish,check db size

        // let tmpDir = TempDir::new().unwrap();
        // let mut fm  = FileManager::new(tmpDir.path());
        // let fmLock = Arc::new(Mutex::new(fm));
        //
        // let mut index = DBIndex::new();
        // let compactor = CompactorWorker::new(fmLock.clone(), index.clone(), rx);
        //
        // let key = "key";
        // let value = "23333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333";
        // let bid = toBucketId(key);
        // let content = serde_json::to_string(&Command::Set(key.to_owned(), value.to_owned())).unwrap();
        // for i in 1..5000 {
        //     let offset = fmLock.lock().unwrap().write(bid, &content).unwrap();
        //     index.set(key, offset);
        // }
        // let a = 3;
    }
}

