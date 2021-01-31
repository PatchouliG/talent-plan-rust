use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use failure::_core::cell::RefCell;

use db::worker::RequestWorker;
use db::db_file::DBFile;
use db::common::Command;
use db::db_file::DBIter;
use db::index::DBIndex;
use crate::db::file_manager::FileManager;
use std::sync::{Arc, Mutex};
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
        // let db = DBFile::new(&file_path)?;
        // let iter = DBIter::new(&db);
        // let mut map = HashMap::new();
        // for (command, offset) in iter {
        //     match command {
        //         Command::Set(key, _) => {
        //             map.insert(key, offset);
        //         }
        //         Command::Remove(key) => {
        //             map.remove(&key);
        //         }
        //         _ => {}
        //     }
        // }

        // let res = KvStore {
        //     m: map,
        //     db: RefCell::new(db),
        // };
        // return Result::Ok(res);

        let fm = FileManager::new(work_dir);
        let fmLock = Arc::new(Mutex::new(fm));
        let worker = RequestWorker::new(fmLock.clone());
        CompactorWorker::new(fmLock.clone());
        Ok(KvStore { worker })
        // unimplemented!();
    }
    pub fn get(&self, key: String) -> Result<Option<String>> {
        // let s =self.worker
        self.worker.handleGet(&key)
        // unimplemented!()
    }
    // pub fn get(&self, key: String) -> Result<Option<String>> {
    //     let offset = self.m.get(&key);
    //     match offset {
    //         None => Result::Ok(None),
    //         Some(o) => {
    //             let (res, _) = self.db.borrow_mut().get((*o) as u64)?;
    //             let command = Command::fromString(&res);
    //             if let Command::Set(_, v) = command {
    //                 Result::Ok(Some(v))
    //             } else {
    //                 Result::Ok(None)
    //             }
    //         }
    //     }
    // }
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // let c = Command::Set(key.clone(), value);
        // let offset = self.db.borrow_mut().write(&c.toString())?;
        // self.m.insert(key, offset);
        // Result::Ok(())
        unimplemented!()
    }
    pub fn remove(&mut self, key: String) -> Result<()> {
        // self.m.remove(&key).ok_or(failure::err_msg("Key not found"))?;
        //
        // let command = Command::Remove(key);
        // self.db.borrow_mut().write(&command.toString())?;
        // Result::Ok(())
        unimplemented!()
    }
}
