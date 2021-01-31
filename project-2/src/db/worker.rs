use std::collections::HashMap;
use std::sync::{Arc, mpsc, Mutex};
use std::sync::mpsc::{Receiver, Sender};

use crate::db::common::FileId;
use crate::db::file_manager::NormalFileMeta;
use crate::db::index::DBIndex;

use super::common::Result;
use super::file_manager::FileManager;

// enum Request {
//     Get(String),
//     Set(String, String),
//     Rm(String),
// }

pub struct RequestWorker {
    fm: Arc<Mutex<FileManager>>,
    index: Arc<Mutex<DBIndex>>,
    files: HashMap<FileId, NormalFileMeta>,
    // file size reach limit,need to compact
    compactTx: Sender<FileId>,
    // requestRx: Receiver<Request>,
    // response: Sender<Result<String>>,
}

impl RequestWorker {
    pub fn new(fm: Arc<Mutex<FileManager>>) -> RequestWorker {
        //     get all normal files from fm
        //     build index
        //     return
        unimplemented!()
    }
    // call from lib
    pub fn handleSet(&mut self) {
        //     if file reach limit, get new file id from fm
        //     set value
    }
    pub fn handleRm(&mut self) {
        //     if file reach limit, get new file id from fm
        //     set value
    }
    pub fn handleGet(&self, key: &str) -> Result<Option<String>> {
        unimplemented!()
    }
}

pub struct CompactorWorker {
    fm: Arc<Mutex<FileManager>>,
    index: Arc<Mutex<DBIndex>>,
    // need compact
    compactRx: Receiver<FileId>,
}

impl CompactorWorker {
    pub fn new(fm: Arc<Mutex<FileManager>>) -> CompactorWorker {
        // let (tx, rx) =
        //     mpsc::channel();
        //     start thread
        unimplemented!()
    }
    pub fn handleCompact() {
        //     wait until more than 2 files
        //     get new fileid from fm
        //     start compact
        //     compact finish,rename file
        //     delete unused,compacted files
        //     next loop
    }
}

