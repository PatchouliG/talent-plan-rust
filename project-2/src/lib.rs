// pub mod kvs {
use std::collections::HashMap;
use std::fs::{File, read_to_string, read};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use failure::_core::cell::RefCell;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Get(String),
    Set(String, String),
    Remove(String),
}

struct DB {
    file: RefCell<File>,
    path: String,
    end_position: usize,
}

struct DBIter<'a> {
    position: u64,
    db: &'a DB,
}

impl<'a> DBIter<'a> {
    fn new(db: &DB) -> DBIter {
        DBIter { position: 0, db }
    }
}

impl<'a> Iterator for DBIter<'a> {
    type Item = (Command, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let p = self.position as usize;
        let (c, size) = self.db.get(self.position).ok()?;
        if size == 0 {
            return None;
        }
        self.position += size as u64;
        let res: Command = serde_json::from_str(&c).unwrap();
        Some((res, p))
    }
}


const db_file: &str = "kvs.db";

impl DB {
    fn new_by_file(db_file_path: &str) -> Result<DB> {
        let file = RefCell::new(OpenOptions::new().read(true).append(true).
            create(true).
            open(Path::new(&db_file_path))?);
        let len = file.borrow_mut().seek(SeekFrom::End(0)).unwrap();
        Result::Ok(DB { file, path: db_file_path.to_owned(), end_position: len as usize })
    }

    fn new(work_dir: &Path) -> Result<DB> {
        let d = work_dir.join(db_file);
        let path_str = d.to_str().unwrap();
        DB::new_by_file(path_str)
    }

    fn write(&mut self, command: &Command) -> Result<usize> {
        let res = self.end_position;
        let s = serde_json::to_string(&command)?;
        let b = s.as_bytes();
        let len = b.len().to_be_bytes();
        let file_mut = self.file.get_mut();
        self.end_position += file_mut.write(&len)?;
        self.end_position += file_mut.write(b)?;
        file_mut.flush()?;
        Result::Ok(res)
    }
    fn get(&self, offset: u64) -> Result<(String, usize)> {
        let mut file_mut = self.file.borrow_mut();

        let position = file_mut.seek(SeekFrom::Current(0))?;
        file_mut.seek(SeekFrom::Start(offset))?;
        let mut _size: usize = 0;
        let size_data = &mut _size.to_be_bytes();

        _size += file_mut.read(size_data)?;
        let a = usize::from_be_bytes(*size_data);
        let mut buffer = vec![0; a];
        _size += file_mut.read(&mut buffer)?;
        // restore position
        file_mut.seek(SeekFrom::Start(position))?;
        Result::Ok((std::str::from_utf8(buffer.as_slice())?.to_owned(), _size))
    }
    pub fn delete(self) {
        std::fs::remove_file(Path::new(&self.path));
    }
}

// #[test]
// fn testWrite() {
//     let mut kv = KvStore::open(Path::new("/Users/wn/code/talent-plan/courses/rust/projects/project-1_mine")).unwrap();
//     kv.set("123".to_owned(), "234".to_owned()).unwrap();
//     kv.set("123".to_owned(), "234a".to_owned()).unwrap();
//     let t = kv.get("123".to_owned()).unwrap().unwrap();
//     assert_eq!(t, "234".to_owned());
// }

pub struct KvStore {
    m: HashMap<String, usize>,
    db: RefCell<DB>,
}


pub type Result<T> = std::result::Result<T, failure::Error>;

impl KvStore {
    pub fn open(file_path: &Path) -> Result<KvStore> {
        let db = DB::new(&file_path)?;
        let iter = DBIter::new(&db);
        let mut map = HashMap::new();
        for (command, offset) in iter {
            match command {
                Command::Set(key, _) => {
                    map.insert(key, offset);
                }
                Command::Remove(key) => {
                    map.remove(&key);
                }
                _ => {}
            }
        }

        let mut res = (KvStore {
            m: map,
            db: RefCell::new(db),
        });
        // res.compact();
        return Result::Ok(res);
    }
    pub fn get(&self, key: String) -> Result<Option<String>> {
        let offset = self.m.get(&key);
        match offset {
            None => Result::Ok(None),
            Some(o) => {
                let (res, _) = self.db.borrow_mut().get((*o) as u64)?;
                let command: serde_json::Result<Command> = serde_json::from_str(&res);
                if let Command::Set(_, v) = command.unwrap() {
                    Result::Ok(Some(v))
                } else {
                    Result::Ok(None)
                }
            }
        }
    }
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let c = Command::Set(key.clone(), value);
        let offset = self.db.borrow_mut().write(&c)?;
        self.m.insert(key, offset);
        Result::Ok(())
    }
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.m.remove(&key).ok_or(failure::err_msg("Key not found"))?;

        let command = Command::Remove(key);
        self.db.borrow_mut().write(&command)?;
        Result::Ok(())
    }
    // fn compact(&mut self) {
    //     let tmp_file = std::env::current_dir().unwrap().join("compaction_tmp");
    //     OpenOptions::new().write(true).create(true).open(&tmp_file).unwrap();
    //     let mut db = DB::new_by_file(tmp_file.to_str().unwrap()).unwrap();
    //     let mut m = HashMap::new();
    //     for (key, offset) in &self.m {
    //         let (value, _) = self.db.borrow().get(*offset as u64).unwrap();
    //         let c: Command = serde_json::from_str(&value).unwrap();
    //         let offset = db.write(&c).unwrap();
    //         m.insert(key.clone(), offset);
    //     }
    //     self.m = m;
    //     std::fs::remove_file(Path::new(&self.db.borrow().path)).unwrap();
    //     std::fs::rename(&tmp_file, &self.db.borrow().path);
    //     let db = DB::new_by_file(&self.db.borrow().path.as_str()).unwrap();
    //     self.db.replace(db);
    // }
}
