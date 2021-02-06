use std::sync::{Arc, Mutex, MutexGuard};

use crate::db::common::{Command, Result};
use crate::db::file_manager::{FileManager, FileManagerLock};
use crate::db::index::{DBIndex, DBIndexLock};
use std::panic::panic_any;
use std::fs::read_to_string;

pub struct RequestWorker<'a> {
    fm: MutexGuard<'a, FileManager>,
    index: MutexGuard<'a, DBIndex>,
}


impl<'a> RequestWorker<'a> {
    pub fn new(fm: MutexGuard<'a, FileManager>, index: MutexGuard<'a, DBIndex>) -> RequestWorker<'a> {
        RequestWorker { fm, index }
    }

    pub fn handle_set(&mut self, key: &str, value: &str) -> Result<()> {
        let c = Command::Set(key.to_owned(), value.to_owned());
        let content = serde_json::to_string(&c)?;
        let res = self.fm.writeToCurrent(&content)?;
        self.index.set(key, res);

        Ok(())
    }
    pub fn handle_rm(&mut self, key: &str) -> Result<()> {
        let c = Command::Remove(key.to_owned());
        let res = self.index.rm(key);
        if !res {
            return Ok(());
        }
        let content = serde_json::to_string(&c)?;
        let res = self.fm.writeToCurrent(&content)?;
        Ok(())
    }
    pub fn handle_get(&self, key: &str) -> Result<Option<String>> {
        let res = self.index.get(key);
        match res {
            None => {
                Ok(None)
            }
            Some(v) => {
                let (content, _) = self.fm.read(v)?.unwrap();
                let c = serde_json::from_str::<Command>(&content).unwrap();
                match c {
                    Command::Set(key, value) => { Ok(Some(value)) }
                    _ => { panic!(); }
                }
            }
        }
    }
}


#[cfg(test)]
mod testRequestWork {
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    use tempfile::TempDir;

    use crate::db::file_manager::FileManager;
    use crate::db::index::DBIndex;
    use crate::db::request_worker::RequestWorker;


    #[test]
    fn testOperation() {
        let tmpDir = TempDir::new().unwrap();
        let fml = Arc::new(Mutex::new(FileManager::new(&tmpDir.path())));
        let indexLock = Arc::new(Mutex::new(DBIndex::new()));
        let mut w = RequestWorker::new(fml.lock().unwrap(), indexLock.lock().unwrap());

        w.handle_set("1", "1");
        w.handle_set("2", "2");
        let res = w.handle_get("1").unwrap().unwrap();
        assert_eq!(res, "1");
        let res = w.handle_get("2").unwrap().unwrap();
        assert_eq!(res, "2");
        let res = w.handle_rm("1");
        assert_eq!(res.is_ok(), true);
        // not found
        let res = w.handle_get("3");
        assert_eq!(res.is_ok(), true);
        assert_eq!(res.unwrap().is_none(), true);
    }
}
