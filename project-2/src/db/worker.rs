use std::collections::HashMap;
use std::sync::{Arc, mpsc, Mutex, MutexGuard};
use std::sync::mpsc::{Receiver, Sender};

use crate::db::common::{Command, FileId, toBucketId};
use crate::db::db_file::DBFile;
use crate::db::file_manager::{FILE_SIZE_LIMIT, ValueIndex};
use crate::db::index::DBIndex;

use super::common::Result;
use super::file_manager::FileManager;

pub struct RequestWorker {
    fmLock: Arc<Mutex<FileManager>>,
    index: DBIndex,
}

impl RequestWorker {
    pub fn new(fm: Arc<Mutex<FileManager>>, index: DBIndex) -> RequestWorker {
        let mut res = RequestWorker { fmLock: fm, index };
        // build index
        res.load();
        res
    }
    // call from lib
    pub fn handle_set(&mut self, key: &str, value: &str) -> Result<()> {
        //     if file reach limit, get new file id from fm
        //     set value
        let c = Command::Set(key.to_owned(), value.to_owned());
        let content = c.toString();
        let bucketId = toBucketId(key);
        let mut fm = self.fmLock.lock().unwrap();
        let mut map = &mut self.index;
        let offset = fm.write(bucketId, &content)?;
        map.set(key, offset);
        Ok(())
    }
    pub fn handle_rm(&mut self, key: &str) -> Result<()> {
        let mut index = &mut self.index;
        let mut fm = self.fmLock.lock().unwrap();
        let bId = toBucketId(key);

        let res = index.rm(key);
        if !res {
            return Ok(());
        }
        let c = Command::Remove(key.to_owned());
        let content = serde_json::to_string(&c).unwrap();
        fm.write(bId, &content)?;
        Ok(())
    }
    pub fn handle_get(&self, key: &str) -> Result<Option<String>> {
        let bId = toBucketId(key);
        let index = &self.index;
        let fm = self.fmLock.lock().unwrap();

        let res = index.get(key);

        if let None = res {
            return Ok(None);
        }

        let offset = res.unwrap();

        let (content, _) = fm.read(bId, offset)?;
        let c = serde_json::from_str::<Command>(&content)?;

        if let Command::Set(_, value) = c {
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn load(&mut self) {
        let fm = self.fmLock.lock().unwrap();
    }
}


#[cfg(test)]
mod testRequestWork {
    use crate::db::worker::RequestWorker;
    use crate::db::file_manager::FileManager;
    use tempfile::TempDir;
    use std::sync::{Arc, Mutex};
    use crate::db::index::{DBIndex, BUCKET_SIZE};
    use std::path::Path;

    fn buildWorkDir(p: &Path) -> RequestWorker {
        let (fm, rx) = FileManager::new(p);

        let mut index = DBIndex::new();
        for i in 0..BUCKET_SIZE {
            let it = fm.getDBIter(i);
            index.load(i, it);
        }
        let fmLock = Arc::new(Mutex::new(fm));
        RequestWorker::new(fmLock, index)
    }

    fn buildWork() -> RequestWorker {
        let tmpDir = TempDir::new().unwrap();
        buildWorkDir(tmpDir.path())
    }

    #[test]
    fn testLoadEmpty() {
        let w = buildWork();
    }

    #[test]
    fn testLoad() {
        let tmpDir = TempDir::new().unwrap();
        let mut w = buildWorkDir(tmpDir.path());
        w.handle_set("1", "1");
        w.handle_set("2", "2");
        w.handle_set("3", "3");
        w.handle_rm("2");
        drop(w);
        let mut w = buildWorkDir(tmpDir.path());
        assert_eq!(w.handle_get("1").unwrap().unwrap(), "1");
        assert_eq!(w.handle_get("2").unwrap().is_none(), true);
        assert_eq!(w.handle_get("3").unwrap().unwrap(), "3");
        w.handle_rm("3");
        assert_eq!(w.handle_get("3").unwrap().is_none(), true);
        assert_eq!(w.handle_get("8").unwrap().is_none(), true);
    }
}

const BUFFER_SIZE_THRESH: usize = 3;

pub struct CompactorWorker {
    fm: Arc<Mutex<FileManager>>,
    index: DBIndex,
    // need compact
    compact_rx: Receiver<FileId>,
    // buffer: Vec<FileId>,
}

impl CompactorWorker {
    pub fn new(fm: Arc<Mutex<FileManager>>, index: DBIndex) -> Sender<FileId> {
        let (tx, rx) = mpsc::channel::<FileId>();
        // start thread
        let mut res: CompactorWorker = CompactorWorker { fm, index, compact_rx: rx };
        std::thread::spawn(move || res.handle_compact());
        unimplemented!()
    }
    pub fn handle_compact(&mut self) {
        loop {
            let mut buffer: Vec<FileId> = Vec::new();
            let id = self.compact_rx.recv().unwrap();
            buffer.push(id);
            if buffer.len() < BUFFER_SIZE_THRESH { continue; }
            //     read from index, bucket by bucket
            //     write to fm
        }
    }
}

