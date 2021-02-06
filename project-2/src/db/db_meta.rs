use std::collections::{HashSet, HashMap};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde::Serialize;

use crate::db::db_file::{DBFile, DBIter};

use super::common::*;

const DB_META_FILE_NAME: &str = "meta.db";

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
pub struct FileMeta {
    pub id: FileId
}

impl FileMeta {
    pub fn new(id: FileId) -> FileMeta {
        FileMeta { id }
    }
    pub fn getId(&self) -> FileId {
        self.id
    }
}

const START_ID: u64 = 1;

pub struct DBMeta {
    metaFile: DBFile,
    work_dir: PathBuf,
    fileIds: HashSet<FileId>,
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MetaCommand {
    Insert(FileId),
    Delete(FileId),
}

impl DBMeta {
    pub fn workDir(&self) -> PathBuf {
        self.work_dir.clone()
    }
    pub fn new(work_dir: &Path) -> DBMeta {
        let p = work_dir.join(DB_META_FILE_NAME);
        let dbFile = DBFile::new(p.as_path()).unwrap();
        let it = DBIter::new(&dbFile);

        let mut res = DBMeta { metaFile: dbFile.clone(), work_dir: work_dir.to_path_buf(), fileIds: HashSet::new() };

        for (s, _) in it {
            let c = serde_json::from_str::<MetaCommand>(&s).unwrap();
            res.updateMemory(&c);
        };
        return res;
    }

    pub fn listFileIds(&self) -> &HashSet<FileId> { &self.fileIds }

    pub fn update(&mut self, c: MetaCommand) {
        self.metaFile.write(&serde_json::to_string::<MetaCommand>(&c).unwrap()).unwrap();
        self.updateMemory(&c);
    }

    fn updateMemory(&mut self, c: &MetaCommand) {
        let cl = c.clone();
        match cl {
            MetaCommand::Insert(m) => {
                self.fileIds.insert(m);
            }
            MetaCommand::Delete(m) => {
                self.fileIds.remove(&m);
            }
        }
    }

    pub fn newFileId(&mut self) -> FileId {
        let res = self.maxID().map(|id| id + 1).unwrap_or(START_ID);
        self.update(MetaCommand::Insert(res));
        res
    }

    pub fn idToDBFile(&self, id: FileId) -> DBFile {
        let p = self.idToPath(id);
        DBFile::new(&p.as_path()).unwrap()
    }


    pub fn dbSize(&self) -> u64 {
        let res = self.fileIds.iter().map(|f| { self.fileSize(*f) }).sum();
        res
    }
    pub fn idToPath(&self, id: FileId) -> PathBuf {
        let res = self.work_dir.join(Path::new(&id.to_string()));
        res
    }

    pub fn fileSize(&self, id: FileId) -> u64 {
        let p = self.idToPath(id);
        let m = std::fs::metadata(p).unwrap();
        m.len()
    }

    pub fn maxID(&self) -> Option<FileId> {
        let mut res = None;
        self.fileIds.iter().
            for_each(|a| if res.is_none() || *a > res.unwrap() {
                res = Some(*a);
            });
        res
    }
}

#[cfg(test)]
mod testDBMeta {
    use std::collections::HashSet;
    use std::fs::OpenOptions;
    use std::path::Path;

    use tempfile::TempDir;

    use crate::db::common::FileId;
    use crate::db::db_meta::{DB_META_FILE_NAME, DBMeta, FileMeta, MetaCommand, START_ID};
    use std::io::Write;

    fn new_file_meta(id: FileId) -> FileMeta {
        FileMeta::new(id)
    }

    #[test]
    fn test_create_meta() {
        let tmpDir = TempDir::new().unwrap();
        let p = tmpDir.path().join(Path::new(DB_META_FILE_NAME));
        assert_ne!(p.exists(), true);
        let dbMeta = DBMeta::new(tmpDir.path());
        assert_eq!(p.exists(), true);
    }

    #[test]
    fn test_open_meta() {
        let tmpDir = TempDir::new().unwrap();
        let p = tmpDir.path().join(Path::new(DB_META_FILE_NAME));
        OpenOptions::new().create(true).write(true).open(p.as_path()).unwrap();
        assert_eq!(p.exists(), true);
        DBMeta::new(tmpDir.path());
    }

    #[test]
    fn test_modify_meta() {
        let tmpDir = TempDir::new().unwrap();
        let mut db_meta = DBMeta::new(tmpDir.path());
        // add 1,2,3
        db_meta.update(MetaCommand::Insert(1));
        db_meta.update(MetaCommand::Insert(2));
        db_meta.update(MetaCommand::Insert(3));
        // compact 1,2 to 4
        db_meta.update(MetaCommand::Delete(2));
        drop(db_meta);

        // reopen
        let mut dbMeta = DBMeta::new(tmpDir.path());

        // add 4
        dbMeta.update(MetaCommand::Insert(4));

        // check, should find 1,3,4
        let metas = dbMeta.listFileIds().iter().map(|m| *m).
            collect::<HashSet<FileId>>();
        assert_eq!(metas.contains(&1), true);
        assert_eq!(metas.contains(&2), false);
        assert_eq!(metas.contains(&3), true);
        assert_eq!(metas.contains(&4), true);

        //     test max
        assert_eq!(dbMeta.maxID().unwrap(), 4)
    }

    #[test]
    fn test_file_size() {
        let tmpDir = TempDir::new().unwrap();
        let mut dbMeta = DBMeta::new(tmpDir.path());
        dbMeta.update(MetaCommand::Insert(1));
        let mut f = OpenOptions::new().write(true).create(true).
            open(tmpDir.path().join("1")).unwrap();
        f.write("123".as_bytes());
        let size = dbMeta.fileSize(1);
        assert_eq!(size, 3);
    }

    #[test]
    fn test_new_file_id() {
        let tmpDir = TempDir::new().unwrap();
        let mut db_meta = DBMeta::new(tmpDir.path());
        let res = db_meta.newFileId();
        assert_eq!(res, START_ID);
        let res = db_meta.newFileId();
        assert_eq!(res, START_ID + 1);
        drop(db_meta);
        let mut db_meta = DBMeta::new(tmpDir.path());
        let res = db_meta.newFileId();
        assert_eq!(res, START_ID + 2);
    }
}
