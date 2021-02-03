use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions, read_to_string};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, Sender};

use clap::Format;
use failure::_core::cmp::max;
use log::{info, warn};
use serde::Deserialize;
use serde::Serialize;
use serde_json::map::Entry::Vacant;

use crate::db::db_file::{DBFile, DBIter};
use crate::db::db_meta::{DBMeta, FileMeta, MetaCommand};
use crate::db::index::BUCKET_SIZE;

use super::common::*;
use super::index;

fn idToPath(id: &FileId, work_dir: &PathBuf) -> PathBuf {
    let res = work_dir.join(Path::new(&id.to_string()));
    res
}

pub type ValueIndex = FileOffset;

// pub struct ValueIndex {
//     pub offset: FileOffset,
// }

const START_ID: FileId = 1;

// 2mb
pub const FILE_SIZE_LIMIT: usize = 1024 * 20;

enum BucketMeta {
    Normal(FileMeta),
    Compacting { writeableFile: FileMeta, readOnly: FileMeta },
}

pub struct FileManager {
    nextId: FileId,
    meta: DBMeta,
    needCompact: Sender<BucketId>,
    buckets: HashMap<BucketId, BucketMeta>,
    bucketSize: HashMap<BucketId, bool>,
    files: HashMap<FileId, DBFile>,
}

impl FileManager {
    pub fn new(workDir: &Path) -> (FileManager, Receiver<BucketId>) {
        let mut dbMeta = DBMeta::new(workDir);
        let metas = dbMeta.listMeta();
        let mut nextFileId = metas.iter().map(|m| m.getId()).
            max().unwrap_or(START_ID);
        let mut current = None;
        if nextFileId != START_ID.clone() {
            current = Some(nextFileId - 1);
        };
        // todo delete unused file
        let (sx, rx) = std::sync::mpsc::channel();
        let mut buckets: HashMap<FileId, BucketMeta> = HashMap::new();
        let mut files: HashMap<FileId, DBFile> = HashMap::new();

        for fileMeta in metas.iter() {
            // ignore snapshot
            if fileMeta.isSnapshot() {
                continue;
            }
            buckets.insert(fileMeta.bucketId, BucketMeta::Normal(fileMeta.clone()));
            files.insert(fileMeta.id, DBFile::new(&idToPath(&fileMeta.id,
                                                            &workDir.to_path_buf())).unwrap());
        }

        let mut bucketSize = HashMap::new();
        for bid in 0..BUCKET_SIZE {
            // init size to zero
            bucketSize.insert(bid, false);

            // init db in bucket
            if !buckets.contains_key(&bid) {
                let fileId = nextFileId;
                nextFileId += 1;
                let f = FileMeta::new(fileId, bid, false);
                buckets.insert(bid, BucketMeta::Normal(f.clone()));
                files.insert(fileId, DBFile::new(&idToPath(&fileId,
                                                           &workDir.to_path_buf())).unwrap());
                dbMeta.update(MetaCommand::Insert(f));
            }
        }

        let res = FileManager {
            nextId: nextFileId,
            meta: dbMeta,
            needCompact: sx,
            buckets: buckets,
            files: files,
            bucketSize: bucketSize,
        };
        (res, rx)
    }

    fn nextId(&mut self) -> FileId {
        let res = self.nextId;
        self.nextId += 1;
        res
    }
    pub fn write(&mut self, bId: BucketId, content: &str) -> Result<FileOffset> {
        // write to current file
        let mut bucketMeta = self.buckets.get(&bId).unwrap();
        let res =
            match bucketMeta {
                BucketMeta::Normal(m) => {
                    let f = self.files.get_mut(&m.id).unwrap();
                    f.write(content)
                }
                BucketMeta::Compacting { writeableFile, readOnly: snapshot } => {
                    let f = self.files.get_mut(&writeableFile.id).unwrap();
                    f.write(content)
                }
            };
        // update bucket write size
        if res.is_ok() {
            let s = self.bucketSize.get(&bId).unwrap();
            let mut size = (*res.as_ref().unwrap() as usize);
            // create new file if read size limit ,and send need compact
            if size > FILE_SIZE_LIMIT && !*s {
                self.needCompact.send(bId).unwrap();
                size = size - FILE_SIZE_LIMIT;
                self.bucketSize.insert(bId, true);
                // self.bucketSize.insert(bId, size);
            }
        }
        res
    }
    pub fn read(&self, bId: BucketId, index: ValueIndex) -> Result<(String, usize)> {
        let bucketMeta = self.buckets.get(&bId).unwrap();
        match bucketMeta {
            BucketMeta::Normal(m) => {
                let file = self.files.get(&m.id).unwrap();
                let res = file.get(index)?;
                res.ok_or(failure::format_err!("get not found"))
            }
            BucketMeta::Compacting { writeableFile, readOnly: snapshot } => {
                // read writeable first
                let a = self.files.get(&writeableFile.id).unwrap();
                let res = a.get(index)?;
                if let Some(s) = res {
                    Ok(s)
                } else {
                    let b = self.files.get(&snapshot.id).unwrap();
                    return b.get(index)?.ok_or(failure::format_err!("get not found"));
                }
            }
        }
    }

    pub fn getDBIter(&self, bId: BucketId) -> DBIter {
        let s = self.buckets.get(&bId).unwrap();
        if let BucketMeta::Normal(n) = s {
            let f = self.files.get(&n.id).unwrap();
            let iter = DBIter::new(f);
            iter
        } else {
            panic!("should be normal")
        }
    }
    pub fn startCompact(&mut self, bId: BucketId) {
        let id = self.nextId();
        let p = idToPath(&id, &self.meta.workDir());
        let dbFile = DBFile::new(&p).unwrap();
        let fileMeta = FileMeta::new(id, bId, false);

        // update meta
        self.meta.update(MetaCommand::Insert(fileMeta.clone()));
        //     set bucket size to zero
        self.bucketSize.insert(bId, false);

        // add db file
        self.files.insert(id, dbFile);

        // update bucket meta
        let bm = self.buckets.remove(&bId).unwrap();
        let newBm = match bm {
            BucketMeta::Normal(mut n) => {
                n.isSnapshot = true;
                let w = FileMeta::new(id, bId, false);
                n.isSnapshot = true;
                let bucketMeta = BucketMeta::Compacting {
                    writeableFile: w,
                    readOnly: n,
                };
                bucketMeta
            }
            BucketMeta::Compacting { writeableFile, readOnly } => {
                panic!();
            }
        };
        self.buckets.insert(bId, newBm);
    }
    // pub fn createSnapshot(&self, bId: BucketId) {}
    pub fn compactFinish(&mut self, bId: BucketId) {
        let f = self.buckets.get(&bId).unwrap();

        if let BucketMeta::Compacting { writeableFile, readOnly: snapshot } = f {
            self.meta.update(MetaCommand::Delete(snapshot.clone()));
            let f = self.files.remove(&snapshot.getId()).unwrap();
            // discard snapshot
            f.delete();
            self.buckets.insert(bId, BucketMeta::Normal(writeableFile.clone()));
        } else {
            panic!("snapshot file not found")
        }
    }

    // need test
    fn deleteUnusedFiles(&self) {
        let paths = std::fs::read_dir(&self.meta.workDir()).unwrap();
        let set = self.meta.listMeta().iter().map(|m| m.id).
            collect::<HashSet<FileId>>();

        for path in paths {
            let name = path.unwrap().file_name().to_str().unwrap().to_owned();
            // try parse name to id
            let res = name.parse::<u64>();
            // parse success
            if let Ok(i) = res {
                // delete if not found
                if !set.contains(&i) {
                    std::fs::remove_file(&name);
                    info!("delete file {}", &name);
                }
            }
        }
    }
}

#[cfg(test)]
mod testFm {
    use tempfile::TempDir;

    use crate::db::file_manager::FileManager;

    #[test]
    fn testNewFileManager() {
        let tmpDir = TempDir::new().unwrap();
        let (mut fm, _) = FileManager::new(tmpDir.path());
        fm.write(3, "234");
        //     todo check file in tmp dir
    }

    #[test]
    fn testReOpenFileManager() {
        let tmpDir = TempDir::new().unwrap();
        let (mut fm, _) = FileManager::new(tmpDir.path());
        drop(fm);
        let (mut fm, _) = FileManager::new(tmpDir.path());
        //     check files and content in dir
    }

    #[test]
    fn testWriteToLimitFileManager() {
        let tmpDir = TempDir::new().unwrap();
        let (mut fm, rx) = FileManager::new(tmpDir.path());
        let s = String::from("2333333333333333333333333333333333333333333333333");
        let mut count = 0;
        loop {
            fm.write(1, &s);
            count += 1;
            if count > 1000 {
                panic!("fail")
            }
            let res = rx.try_recv();
            if res.is_ok() {
                return;
            }
        }
    }

    #[test]
    fn testCompactFileManager() {
        let tmpDir = TempDir::new().unwrap();
        let (mut fm, rx) = FileManager::new(tmpDir.path());
        fm.write(1, "234");
        fm.write(1, "345");
        // start compact
        fm.startCompact(1);
        fm.write(1, "88");
        fm.compactFinish(1);
        // write 456
        // end compact
        //     todo check file is deleted
        //     todo check compact output
        //     todo check meta
        let a = 3;
    }

    #[test]
    fn debug() {
        struct test {}

        impl test {
            fn f(&mut self, i: i32) {
                println!("{}", i)
            }
        }
        let mut a = test {};
        fn tt(t: &mut test, f: fn(&mut test, i32)) {
            f(t, 8)
        }

        tt(&mut a, test::f)
    }
}
