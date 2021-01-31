use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use failure::_core::cell::RefCell;
use failure::_core::cmp::Ordering;
use regex::Regex;

use super::common::*;
use super::index;

const DB_FILE_NAME: &str = "kvs.db";

pub type FileId = u64;
type FileOffset = u64;

pub struct ValueIndex {
    id: FileId,
    offset: FileOffset,
}

const FILE_SIZE_LIMIT: u64 = 234;

const START_ID: i32 = 1;

pub struct FileManager {
    nextId: FileId,
    normalFiles: Vec<NormalFileMeta>,
    compactFiles: Option<CompactFileMeta>,
}


impl FileManager {
    pub fn new(workDir: &Path) -> FileManager {
        let res = std::fs::read_dir(workDir).unwrap();
        let mut maxId = 0;
        let mut files = Vec::new();
        for i in res.into_iter() {
            let name = i.unwrap().file_name().to_str().unwrap().to_owned();
            let mut id = 0;
            if let Some(n) = NormalFileMeta::new(&name) {
                id = n.id;
                files.push(n);
                //     discard unfinished compact files,
            } else if let Some(c) = CompactFileMeta::new(&name) {
                // id = c.id;
                std::fs::remove_file(workDir.join(name)).expect("discard compact file error");
            }
            // let id = DBFIleMeta::fromFileName(&name).getId();
            if id > maxId { maxId = id }
        }

        FileManager {
            nextId: maxId + 1,
            normalFiles: files,
            compactFiles: None,
        }
    }
    pub fn nextFile(&mut self) -> NormalFileMeta {
        let res = NormalFileMeta { id: self.nextId };
        self.nextId += 1;
        self.normalFiles.push(res.clone());
        res
    }
    pub fn nextCompacteFile(&mut self) -> CompactFileMeta {
        let res = CompactFileMeta { id: self.nextId };
        self.nextId += 1;
        res
    }

    // pub fn startCompactOutputFile(&mut self) -> (FileId, Vec<FileId>) { unimplemented!() }
    // pub fn endCompact(&mut self, id: FileId) { unimplemented!() }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct NormalFileMeta {
    id: FileId
}

impl NormalFileMeta {
    fn new(s: &str) -> Option<NormalFileMeta> {
        let reg = Regex::new(r"^(\d+)$").unwrap();
        reg.captures(s).map(|x| {
            let a = x.get(1).unwrap().as_str().parse::<FileId>().unwrap();
            NormalFileMeta { id: a }
        })
    }
    fn toStr(&self) -> String {
        self.id.to_string()
    }
}

#[derive(Debug, PartialEq)]
struct CompactFileMeta {
    id: FileId
}

impl CompactFileMeta {
    fn new(s: &str) -> Option<CompactFileMeta> {
        let reg = Regex::new(r"^compact_(\d+)$").unwrap();
        reg.captures(s).map(|x| {
            let a = x.get(1).unwrap().as_str().parse::<FileId>().unwrap();
            CompactFileMeta { id: a }
        })
    }

    fn toStr(&self) -> String {
        format!("compact_{}", self.id)
    }

    // change name
    fn finish(&mut self) -> String {
        self.toStr()
    }
}

pub struct DBFile {
    file: RefCell<File>,
    path: String,
    end_position: usize,
}

impl DBFile {
    fn new_by_file(db_file_path: &str) -> Result<DBFile> {
        let file = RefCell::new(OpenOptions::new().read(true).append(true).
            create(true).
            open(Path::new(&db_file_path))?);
        let len = file.borrow_mut().seek(SeekFrom::End(0)).unwrap();
        Result::Ok(DBFile { file, path: db_file_path.to_owned(), end_position: len as usize })
    }

    pub fn new(work_dir: &Path) -> Result<DBFile> {
        let d = work_dir.join(DB_FILE_NAME);
        let path_str = d.to_str().unwrap();
        DBFile::new_by_file(path_str)
    }

    pub fn write(&mut self, content: &str) -> Result<usize> {
        let res = self.end_position;

        let b = content.as_bytes();
        let len = b.len().to_be_bytes();
        let file_mut = self.file.get_mut();
        self.end_position += file_mut.write(&len)?;
        self.end_position += file_mut.write(b)?;
        file_mut.flush()?;
        Result::Ok(res)
    }
    pub fn get(&self, offset: FileId) -> Result<(String, usize)> {
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
}

pub struct DBIter<'a> {
    position: FileId,
    db: &'a DBFile,
}

impl<'a> DBIter<'a> {
    pub fn new(db: &DBFile) -> DBIter {
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
        self.position += size as FileId;
        let res: Command = serde_json::from_str(&c).unwrap();
        Some((res, p))
    }
}

#[cfg(test)]
mod test {
    use std::fs::OpenOptions;
    use std::path::Path;
    use std::process::id;

    use tempfile::TempDir;

    use crate::db::file_manager::{CompactFileMeta, FileManager, NormalFileMeta};

    #[test]
    fn testFileId() {
        let tmpDir = TempDir::new().unwrap();
        let id = 2;
        OpenOptions::new().write(true).create(true).open(tmpDir.path().
            join(id.to_string()).as_path()).unwrap();
        let mut fm = FileManager::new(tmpDir.path());

        let id1 = fm.nextFile();
        let id2 = fm.nextFile();
        let id3 = fm.nextCompacteFile();

        assert_ne!(id1, id2);
        assert_eq!(id1.id, id + 1);
        assert_eq!(id2.id, id + 2);
        assert_eq!(id3.id, id + 3);
    }

    #[test]
    fn debug() {
        use regex::Regex;
        let re = Regex::new(r"compact_(\d+)").unwrap();
        print!("{}", re.captures("compact_3234").unwrap().get(1).unwrap().as_str());
        assert!(re.is_match("compact_3234"));
    }

    #[test]
    fn testDiscardCompactFile() {
        let n = TempDir::new().unwrap();
        let tmpDir = n.path().to_owned();
        let c = CompactFileMeta { id: 9 };
        let s = c.toStr();
        let p = tmpDir.join(s).as_path().to_owned();

        let n = NormalFileMeta { id: 34 };

        OpenOptions::new().write(true).create(true).open(&p).unwrap();
        OpenOptions::new().write(true).create(true).open(
            tmpDir.join(Path::new(&n.toStr())).as_path()
        ).unwrap();
        assert_eq!(p.exists(), true);

        let mut fm = FileManager::new(tmpDir.as_path());
        assert_ne!(p.exists(), true);

        assert_eq!(fm.normalFiles.len(), 1);
        assert_eq!(fm.normalFiles[0], n);
    }


    #[test]
    fn testCompactFileMeta() {
        let a = CompactFileMeta { id: 1 };
        let s = a.toStr();
        assert_eq!(s, "compact_1");

        let a2 = CompactFileMeta::new(&s).unwrap();
        assert_eq!(a2, a);

        let b = CompactFileMeta { id: 5 };
        let s = b.toStr();
        assert_eq!(s, "compact_5");

        let mut c = CompactFileMeta::new(&s).unwrap();
        assert_eq!(b, c);
        c.finish();
    }

    #[test]
    fn testNormalFileMeta() {
        let a = NormalFileMeta { id: 123 };
        let s = a.toStr();
        assert_eq!(s, "123");

        let b = NormalFileMeta::new(&s).unwrap();
        assert_eq!(a, b);
    }
}