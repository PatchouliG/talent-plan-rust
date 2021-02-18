use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use failure::_core::cell::RefCell;
use serde::{Deserialize, Serialize};

pub use db::common::Command;
use db::db_file::DBFile;
use db::db_file::DBIter;
use db::index::DBIndex;

use crate::db::compacte_worker::CompactorWorker;
use crate::db::file_manager::{FileManager, FileManagerLock};
use crate::db::index::DBIndexLock;
use crate::db::lock_manager::LockManager;
use crate::db::request_worker::RequestWorker;

mod db;

pub type Result<T> = db::common::Result<T>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RequestResp {
    Error,
    None,
    Value(String),
}


// #[derive(Serialize, Deserialize, Debug)]
// pub enum SetRequestResp {
//     Error,
//     Ok,
// }

// #[derive(Serialize, Deserialize, Debug)]
// pub enum GetRequestResp {
//     Error,
//     Ok,
// }

pub type SetRequestResp = Result<()>;

pub enum GetRequestResp {
    Error,
    None,
    Value(String),
}

pub type RmRequestResp = Result<()>;

pub fn resp_to_str(request: RequestResp) -> String {
    let res = serde_json::to_string(&request).unwrap();
    res
}

pub fn str_to_request_result(s: String) -> RequestResp {
    serde_json::from_str(&s).unwrap()
}

pub trait KvsEngine {
    fn get(&self, key: String) -> Result<Option<String>>;
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn remove(&mut self, key: String) -> Result<()>;
}

pub enum EngineType {
    kvs,
    sled,
}

pub fn getStore(engineType: EngineType, path: &Path) -> Box<dyn KvsEngine> {
    match engineType {
        EngineType::kvs => {
            let kvs = KvStore::new(path).unwrap();
            Box::new(kvs)
        }
        EngineType::sled => {
            let kvs = SledStore::new(path);
            Box::new(kvs)
        }
    }
}


pub struct KvStore {
    lm: LockManager
}

impl KvsEngine for KvStore {
    fn get(&self, key: String) -> Result<Option<String>> {
        self.get(key)
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.set(key, value)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.remove(key)
    }
}

impl KvStore {
    // i like new, but test need open
    pub fn new(work_dir: &Path) -> Result<KvStore> {
        KvStore::open(work_dir)
    }
    pub fn open(work_dir: &Path) -> Result<KvStore> {
        let fm = FileManager::new(work_dir);
        let mut index = DBIndex::new();
        fm.load(&mut index);
        let lm = LockManager::new(fm, index);
        CompactorWorker::start(lm.clone());
        Ok(KvStore { lm })
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        let (a, b) = self.lm.get();
        RequestWorker::new(a, b).handle_get(&key)
    }
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let (a, b) = self.lm.get();
        RequestWorker::new(a, b).handle_set(&key, &value)
    }
    pub fn remove(&mut self, key: String) -> Result<()> {
        let (a, b) = self.lm.get();
        RequestWorker::new(a, b).handle_rm(&key)
    }
}

struct SledStore {
    db: sled::Db
}

impl SledStore {
    fn new(p: &Path) -> SledStore {
        SledStore { db: sled::open(p).unwrap() }
    }
}

impl KvsEngine for SledStore {
    fn get(&self, key: String) -> Result<Option<String>> {
        let value = self.db.get(key);
        let res = value.map(|o| o.map(|i| String::from_utf8(i.to_vec()).unwrap()));
        let r = res.map_err(|e| failure::err_msg("sled store get error"));
        r
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        let res = self.db.insert(key.as_str(), value.as_str());
        let r = res.map(|_| ());
        r.map_err(|e| failure::err_msg("sled store get error"))
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let res = self.db.remove(key.as_str());
        match res {
            std::result::Result::Ok(o) => {
                if let None = o {
                    return Err(failure::err_msg("Key not Found"));
                }
                Ok(())
            }
            std::result::Result::Err(_) => {
                Err(failure::err_msg("error"))
            }
        }
        // let r = res.map(|o| ());
        // r.map_err(|e| failure::err_msg("sled store get error"))
    }
}


#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use crate::KvStore;
    use std::collections::HashMap;
    use rand::{Rng, SeedableRng};
    use failure::_core::time::Duration;
    use std::thread::sleep;

    #[test]
    fn testOperation() {
        let tmpDir = TempDir::new().unwrap();
        let mut kvs = KvStore::new(tmpDir.path()).unwrap();
        kvs.set("a".to_owned(), "a".to_owned()).unwrap();
        kvs.set("b".to_owned(), "b".to_owned()).unwrap();
        kvs.set("c".to_owned(), "c".to_owned()).unwrap();
        let res = kvs.get("b".to_owned()).unwrap().unwrap();
        assert_eq!(res, "b");
        kvs.remove("b".to_owned()).unwrap();
        let res = kvs.get("b".to_owned()).unwrap();
        assert_eq!(res.is_none(), true);
    }

    #[test]
    fn testLoad() {
        let tmpDir = TempDir::new().unwrap();
        let mut kvs = KvStore::new(tmpDir.path()).unwrap();
        kvs.set("a".to_owned(), "a".to_owned()).unwrap();
        kvs.set("b".to_owned(), "b".to_owned()).unwrap();
        kvs.set("c".to_owned(), "c".to_owned()).unwrap();
        kvs.remove("b".to_owned()).unwrap();
        drop(kvs);

        let mut kvs = KvStore::new(tmpDir.path()).unwrap();
        let res = kvs.get("a".to_owned()).unwrap().unwrap();
        assert_eq!(res, "a");
        let res = kvs.get("b".to_owned()).unwrap();
        assert_eq!(res.is_none(), true);
    }

    #[test]
    fn testRandom() {
        let tmpDir = TempDir::new().unwrap();
        let mut kvs = KvStore::new(tmpDir.path()).unwrap();
        let mut map: HashMap<String, String> = HashMap::new();
        let number = 3000;
        let mut rng = rand::rngs::StdRng::seed_from_u64(23);
        for i in 0..number {
            let r = rng.gen_bool(0.8);
            let key = "key_".to_owned() + (i % 50).to_string().as_str();
            let value = String::from("2222222222222222222222222222222222222222222222222222222222222222");
            match r {
                // set
                true => {
                    kvs.set(key.clone(), value.clone()).unwrap();
                    map.insert(key, value);
                }
                // rm
                false => {
                    let a = map.iter().next();
                    if let Some((key, _)) = a {
                        let k = key.clone();
                        kvs.remove(k.clone());
                        map.remove(&k);
                    }
                }
            }
        }
        // wait compact
        sleep(Duration::new(6, 0));
        for (key, value) in map.iter() {
            assert_eq!(kvs.get(key.clone()).unwrap().unwrap(), *value);
        }
    }
}