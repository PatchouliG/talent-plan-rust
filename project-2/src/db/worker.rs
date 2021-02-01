use std::collections::HashMap;
use std::sync::{Arc, mpsc, Mutex};
use std::sync::mpsc::{Receiver, Sender};

use crate::db::common::{Command, FileId, toBucketId};
use crate::db::db_file::DBFile;
// use crate::db::db_meta::NormalFileMeta;
use crate::db::file_manager::{FILE_SIZE_LIMIT, ValueIndex};
use crate::db::index::DBIndex;

use super::common::Result;
use super::file_manager::FileManager;

pub struct RequestWorker {
    fmLock: Arc<Mutex<FileManager>>,
    index: Arc<Mutex<DBIndex>>,
    // currentFileId: FileId,
    // currentFile: DBFile,
    currentFileSize: u64,
}

impl RequestWorker {
    pub fn new(fm: Arc<Mutex<FileManager>>, index: Arc<Mutex<DBIndex>>) -> (RequestWorker) {
        //     get all normal files from fm
        //     build index
        //     return
        unimplemented!()
    }
    // call from lib
    pub fn handle_set(&mut self, key: &str, value: &str) -> Result<()> {
        //     if file reach limit, get new file id from fm
        //     set value
        let c = Command::Set(key.to_owned(), value.to_owned());
        let content = c.toString();
        let bucketId = toBucketId(key);
        let mut fm = self.fmLock.lock().unwrap();
        let mut map = self.index.lock().unwrap();
        let offset = fm.write(bucketId, &content)?;
        // let index = ValueIndex { id: self.currentFileId, offset };
        map.set(key.to_owned(), ValueIndex { offset });
        unimplemented!()
    }
    pub fn handle_rm(&mut self, key: &str) -> Result<()> {
        //     if file reach limit, get new file id from fm
        //     set value
        unimplemented!()
    }
    pub fn handle_get(&self, key: &str) -> Result<Option<String>> {
        unimplemented!()
    }
}

const BUFFER_SIZE_THRESH: usize = 3;

pub struct CompactorWorker {
    fm: Arc<Mutex<FileManager>>,
    index: Arc<Mutex<DBIndex>>,
    // need compact
    compact_rx: Receiver<FileId>,
    // buffer: Vec<FileId>,
}

impl CompactorWorker {
    pub fn new(fm: Arc<Mutex<FileManager>>, index: Arc<Mutex<DBIndex>>) -> Sender<FileId> {
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

