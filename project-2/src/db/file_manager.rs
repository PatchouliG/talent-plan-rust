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
use tempfile::TempDir;

use crate::db::db_file::{DBFile, DBIter};
use crate::db::db_meta::{DBMeta, FileMeta, MetaCommand};

use super::common::*;
use super::index;
use std::sync::{Mutex, Arc};
use std::process::id;

const FILE_SIZE_LIMIT: u64 = 1024 * 64;

#[derive(Copy, Clone)]
pub struct ValueIndex {
    pub offset: FileOffset,
    pub fileId: FileId,
}

pub trait Loader {
    fn load(&mut self, content: &str, index: ValueIndex);
}


impl ValueIndex {
    pub fn new(offset: FileOffset, id: FileId) -> ValueIndex {
        ValueIndex { offset, fileId: id }
    }
}

const START_ID: FileId = 1;

pub type FileManagerLock = Arc<Mutex<FileManager>>;

pub struct FileManager {
    currentFile: DBFile,
    currentFileId: FileId,
    meta: DBMeta,
    readOnlyFiles: HashMap<FileId, DBFile>,
    sizeLimit: u64,
}

impl FileManager {
    pub fn new(workDir: &Path) -> FileManager {
        FileManager::newFmWithSize(workDir, FILE_SIZE_LIMIT)
    }
    pub fn newFmWithSize(workDir: &Path, limit: u64) -> FileManager {
        let mut meta = DBMeta::new(workDir);
        let maxFileId = meta.maxID();
        let currentId = match maxFileId {
            None => { meta.newFileId() }
            Some(f) => { f }
        };
        let currentFile = DBFile::new(&meta.idToPath(currentId)).unwrap();

        let mut readOnlyFiles = HashMap::new();
        meta.listFileIds().iter().
            filter(|id| **id != currentId).
            for_each(|id| {
                readOnlyFiles.insert(*id, meta.idToDBFile(*id));
            });
        FileManager { currentFile, currentFileId: currentId, meta, readOnlyFiles, sizeLimit: limit }
    }

    // write to current db file
    // use new db file when size reach limit
    pub fn writeToCurrent(&mut self, content: &str) -> Result<ValueIndex> {
        let offset = self.currentFile.write(content)?;
        if offset > self.sizeLimit {
            let id = self.meta.newFileId();
            self.readOnlyFiles.insert(self.currentFileId, self.meta.idToDBFile(self.currentFileId));
            self.currentFile = self.meta.idToDBFile(id);
            self.currentFileId = id;
        }
        Ok(ValueIndex::new(offset, self.currentFileId))
    }
    pub fn read(&self, index: ValueIndex) -> Result<Option<(String, usize)>> {
        let p = self.readOnlyFiles.get(&index.fileId);
        if let Some(f) = p
        {
            f.get(index.offset)
        } else if self.currentFileId == index.fileId {
            self.currentFile.get(index.offset)
        } else {
            Err(failure::format_err!("fileId not Found"))
        }
    }

    pub fn getReadOnlyFiles(&self) -> Vec<FileId> {
        self.readOnlyFiles.iter().map(|e| *e.0).collect()
    }

    pub fn isReadOnlyFile(&self, id: FileId) -> bool {
        self.readOnlyFiles.contains_key(&id)
    }

    pub fn idToFile(&self, id: FileId) -> DBFile { self.meta.idToDBFile(id) }
    pub fn idToPath(&self, id: FileId) -> PathBuf { self.meta.idToPath(id) }

    // todo return all db files
    pub fn load(&self, loader: &mut dyn Loader) {
        let mut fIds = self.getReadOnlyFiles();
        fIds.push(self.currentFileId);

        let fs = fIds.iter().map(|i| (*i, self.idToFile(*i))).
            collect::<Vec<(FileId, DBFile)>>();

        fs.iter().for_each(|(id, f)| {
            let it = DBIter::new(f);
            for (content, offset) in it {
                loader.load(&content, ValueIndex::new(offset, *id));
            }
        });
    }
    // delete file, for compact
    pub fn deleteDBFIle(&mut self, id: FileId) {
        // can't delete current file
        assert_ne!(id, self.currentFileId);
        self.meta.update(MetaCommand::Delete(id));
        self.readOnlyFiles.remove(&id);
        std::fs::remove_file(self.meta.idToPath(id)).unwrap();
    }

    // need test
    fn deleteUnusedFiles(&self) {
        unimplemented!()
    }
}

#[cfg(test)]
mod testFm {
    const FILE_SIZE_LIMIT: u64 = 512;

    use tempfile::TempDir;

    use crate::db::common::{Command, FileId};
    use crate::db::db_file::DBFile;
    use crate::db::file_manager::{FileManager, Loader, ValueIndex};
    use std::panic::panic_any;

    fn buildContent() -> String {
        let c = Command::Set("key".to_owned(), "value".to_owned());
        let content = serde_json::to_string(&c).unwrap();
        content
    }

    fn writeToFm(fm: &mut FileManager) {
        let c = buildContent();
        // write until create new file
        for i in 1..50 {
            fm.writeToCurrent(&c).unwrap();
        }
    }

    #[test]
    fn testNewFileManager() {
        let tmpDir = TempDir::new().unwrap();
        let mut fm = FileManager::newFmWithSize(tmpDir.path(), FILE_SIZE_LIMIT);
        let content = buildContent();
        fm.writeToCurrent(&content);
        fm.writeToCurrent(&content);
        fm.writeToCurrent(&content);

        let cf = fm.currentFile.getPath();

        use std::path::Path;
        let size = std::fs::metadata(Path::new(&cf)).unwrap().len();
        assert_eq!(size, 93)
    }

    #[test]
    fn testReOpenFileManager() {
        let tmpDir = TempDir::new().unwrap();
        let mut fm = FileManager::newFmWithSize(tmpDir.path(), FILE_SIZE_LIMIT);
        let content = buildContent();
        let path = fm.meta.workDir();
        let content = buildContent();
        fm.writeToCurrent(&content);
        drop(fm);
        let mut fm = FileManager::new(&path);
    }

    #[test]
    fn testWriteToLimitFileManager() {
        let tmpDir = TempDir::new().unwrap();
        let mut fm = FileManager::newFmWithSize(tmpDir.path(), FILE_SIZE_LIMIT);
        writeToFm(&mut fm);
        let files = fm.getReadOnlyFiles();
        assert_eq!(files.len(), 2)
    }

    #[test]
    fn testDelete() {
        let tmpDir = TempDir::new().unwrap();
        let mut fm = FileManager::newFmWithSize(tmpDir.path(), FILE_SIZE_LIMIT);
        // let fileMeta = fm.newDBFile();
        // let mut dbFile = DBFile::new(&idToPath(&fileMeta.id, &tmpDir.into_path())).unwrap();
        writeToFm(&mut fm);
        let rf = fm.getReadOnlyFiles();
        assert_eq!(rf.len() > 0, true);
        let fId = rf.get(0).unwrap();
        fm.deleteDBFIle(*fId);

        let p = fm.idToPath(*fId);

        //     check if is delete
        assert_eq!(std::path::Path::new(&p).exists(), false);
    }


    struct LoaderTest {
        key: String,
        value: String,
    }

    impl Loader for LoaderTest {
        fn load(&mut self, content: &str, index: ValueIndex) {
            let c = serde_json::from_str::<Command>(content).unwrap();
            if let Command::Set(a, b) = c {
                assert_eq!(a, self.key);
                assert_eq!(b, self.value)
            }
        }
    }

    #[test]
    fn testLoad() {
        let tmpDir = TempDir::new().unwrap();
        let mut fm = FileManager::new(tmpDir.path());
        writeToFm(&mut fm);
        let mut l = LoaderTest { key: "key".to_owned(), value: "value".to_owned() };
        fm.load(&mut l);
    }

    #[test]
    fn testRead() {
        let tmpDir = TempDir::new().unwrap();
        let mut fm = FileManager::newFmWithSize(tmpDir.path(), FILE_SIZE_LIMIT);
        writeToFm(&mut fm);
        let content = buildContent();
        let index = fm.writeToCurrent(&content).unwrap();
        writeToFm(&mut fm);
        let res = fm.read(index).unwrap().unwrap().0;
        let c = serde_json::from_str::<Command>(&res).unwrap();
        if let Command::Set(key, value) = c {
            assert_eq!(key, "key");
            assert_eq!(value, "value");
        } else {
            panic!("match fail")
        }
    }

    // fn buildFM() -> FileManager {
    //     let tmpDir = TempDir::new().unwrap();
    //     let mut fm = FileManager::newFmWithSize(tmpDir.path(), FILE_SIZE_LIMIT);
    //     fm
    // }
}
// }

