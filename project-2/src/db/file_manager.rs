use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use regex::Regex;
use serde::Serialize;
use serde::Deserialize;

use super::common::*;
use super::index;
use crate::db::db_file::DBFile;
use failure::_core::cmp::max;

pub struct ValueIndex {
    id: FileId,
    offset: FileOffset,
}

const FILE_SIZE_LIMIT: u64 = 234;
const START_ID: i32 = 1;

pub struct FileManager {
    nextId: FileId,
    meta: DBMeta,
    normalFiles: Vec<NormalFileMeta>,
    compactFiles: Option<CompactFileMeta>,
}


impl FileManager {
    pub fn new(workDir: &Path) -> FileManager {

        let dbMeat = DBMeta::new(workDir);
        let metas = dbMeat.listMeta();
        metas.iter().filter(|meta| {
            if let FileMeta::compact(_, _) = meta {
                return true;
            }
            false
        }).map(|meta| {
            //     delete file
        });
        //  find max
        // metas.iter().map(|meta|)

        unimplemented!()
    }
    pub fn nextFile(&mut self) -> NormalFileMeta {
        let res = NormalFileMeta { id: self.nextId };
        self.nextId += 1;
        self.normalFiles.push(res.clone());
        res
    }
    pub fn nextCompacteFile(&mut self, maxId: FileId) -> CompactFileMeta {
        let res = CompactFileMeta { id: self.nextId, maxNormalFileId: maxId };
        self.nextId += 1;
        res
    }
}

#[derive(Serialize, Deserialize)]
pub enum FileMeta {
    normal(NormalFileMeta),
    compact(CompactFileMeta, bool),
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct CompactFileMeta {
    id: FileId,
    maxNormalFileId: FileId,
}

struct DBMeta {
    file: DBFile,
}

struct MetaCommand {}

impl DBMeta {
    fn new(work_dir: &Path) -> DBMeta { unimplemented!() }
    fn listMeta(&self) -> Vec<FileMeta> { unimplemented!() }
    fn modifyMeta(&mut self, c: MetaCommand) { unimplemented!() }
}


#[cfg(test)]
mod test {
    use std::fs::OpenOptions;
    use std::path::Path;
    use std::process::id;

    use tempfile::TempDir;

    use crate::db::file_manager::{CompactFileMeta, FileManager, NormalFileMeta};

// #[test]
// fn testFileId() {
//     let tmpDir = TempDir::new().unwrap();
//     let id = 2;
//     OpenOptions::new().write(true).create(true).open(tmpDir.path().
//         join(id.to_string()).as_path()).unwrap();
//     let mut fm = FileManager::new(tmpDir.path());
//
//     let id1 = fm.nextFile();
//     let id2 = fm.nextFile();
//     let id3 = fm.nextCompacteFile();
//
//     assert_ne!(id1, id2);
//     assert_eq!(id1.id, id + 1);
//     assert_eq!(id2.id, id + 2);
//     assert_eq!(id3.id, id + 3);
// }

    #[test]
    fn debug() {
        use regex::Regex;
        let re = Regex::new(r"compact_(\d+)").unwrap();
        print!("{}", re.captures("compact_3234").unwrap().get(1).unwrap().as_str());
        assert!(re.is_match("compact_3234"));
    }

// #[test]
// fn testDiscardCompactFile() {
//     let n = TempDir::new().unwrap();
//     let tmpDir = n.path().to_owned();
//     let c = CompactFileMeta { id: 9 };
//     let s = c.toStr();
//     let p = tmpDir.join(s).as_path().to_owned();
//
//     let n = NormalFileMeta { id: 34 };
//
//     OpenOptions::new().write(true).create(true).open(&p).unwrap();
//     OpenOptions::new().write(true).create(true).open(
//         tmpDir.join(Path::new(&n.toStr())).as_path()
//     ).unwrap();
//     assert_eq!(p.exists(), true);
//
//     let mut fm = FileManager::new(tmpDir.as_path());
//     assert_ne!(p.exists(), true);
//
//     assert_eq!(fm.normalFiles.len(), 1);
//     assert_eq!(fm.normalFiles[0], n);
// }


// #[test]
// fn testCompactFileMeta() {
//     let a = CompactFileMeta { id: 1 };
//     let s = a.toStr();
//     assert_eq!(s, "compact_1");
//
//     let a2 = CompactFileMeta::new(&s).unwrap();
//     assert_eq!(a2, a);
//
//     let b = CompactFileMeta { id: 5 };
//     let s = b.toStr();
//     assert_eq!(s, "compact_5");
//
//     let mut c = CompactFileMeta::new(&s).unwrap();
//     assert_eq!(b, c);
//     c.finish();
// }

// #[test]
// fn testNormalFileMeta() {
//     let a = NormalFileMeta { id: 123 };
//     let s = a.toStr();
//     assert_eq!(s, "123");
//
//     let b = NormalFileMeta::new(&s).unwrap();
//     assert_eq!(a, b);
// }
}