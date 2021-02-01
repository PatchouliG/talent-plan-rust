use std::collections::HashMap;
use std::sync::{Arc, mpsc, Mutex};
use std::sync::mpsc::{Receiver, Sender};

use crate::db::common::{Command, FileId};
use crate::db::db_file::DBFile;
use crate::db::db_meta::NormalFileMeta;
use crate::db::file_manager::{FILE_SIZE_LIMIT, ValueIndex};
use crate::db::index::DBIndex;

use super::common::Result;
use super::file_manager::FileManager;

pub struct RequestWorker {
    fm: Arc<Mutex<FileManager>>,
    index: Arc<Mutex<DBIndex>>,
    files: HashMap<FileId, NormalFileMeta>,
    // file size reach limit,need to compact
    compactTx: Sender<FileId>,
    // current write file
    currentFileMeta: NormalFileMeta,
    currentFile: DBFile,
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
        let offset = self.currentFile.write(&content)?;
        let index = ValueIndex { id: self.currentFileMeta.getId(), offset };
        self.index.lock().unwrap().set(key.to_owned(), index);
        self.currentFileSize += content.len() as u64;
        if self.currentFileSize > FILE_SIZE_LIMIT {
            //     todo
        }
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
    buffer: Vec<FileId>,
}

impl CompactorWorker {
    pub fn new(fm: Arc<Mutex<FileManager>>, index: Arc<Mutex<DBIndex>>) -> Sender<FileId> {
        let (tx, rx) = mpsc::channel::<FileId>();
        // start
        // thread
        let mut res: CompactorWorker = CompactorWorker { fm, index, compact_rx: rx, buffer: Vec::new() };
        std::thread::spawn(move || res.handle_compact());
        unimplemented!()
    }
    pub fn handle_compact(&mut self) {
        loop {
            let res = self.compact_rx.recv().unwrap();
            //     wait until more than 2 files
            self.buffer.push(3);
            if self.buffer.len() < BUFFER_SIZE_THRESH {
                continue;
            }
            //     get new fileid from fm
            let maxId = self.buffer.iter().max().unwrap();
            let cf = self.fm.lock().unwrap().nextCompactFile(*maxId);
            //     start compact
            //     compact finish,rename file
            //     delete unused,compacted files
            //     next loop
        }
    }
}

