use std::collections::HashMap;
use std::sync::{Arc, mpsc, Mutex};
use std::sync::mpsc::{Receiver, Sender};

use crate::db::common::FileId;
use crate::db::file_manager::NormalFileMeta;
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
    current: NormalFileMeta,
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

pub struct CompactorWorker {
    fm: Arc<Mutex<FileManager>>,
    index: Arc<Mutex<DBIndex>>,
    // need compact
    compact_rx: Receiver<FileId>,
}

impl CompactorWorker {
    pub fn new(fm: Arc<Mutex<FileManager>>, index: Arc<Mutex<DBIndex>>) -> Sender<FileId> {
        let (tx, rx) = mpsc::channel::<FileId>();
        // start
        // thread
        let res: CompactorWorker = CompactorWorker { fm, index, compact_rx: rx };
        std::thread::spawn(move || res.handle_compact());
        unimplemented!()
    }
    pub fn handle_compact(&self) {
        //     wait until more than 2 files
        //     get new fileid from fm
        //     start compact
        //     compact finish,rename file
        //     delete unused,compacted files
        //     next loop
    }
}

