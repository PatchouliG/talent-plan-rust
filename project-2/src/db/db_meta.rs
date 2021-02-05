use std::collections::HashSet;
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

pub struct DBMeta {
    metaFile: DBFile,
    work_dir: PathBuf,
    fileMetas: HashSet<FileMeta>,
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MetaCommand {
    Insert(FileMeta),
    Delete(FileMeta),
}

impl DBMeta {
    pub fn workDir(&self) -> PathBuf {
        self.work_dir.clone()
    }
    pub fn new(work_dir: &Path) -> DBMeta {
        let p = work_dir.join(DB_META_FILE_NAME);
        let dbFile = DBFile::new(p.as_path()).unwrap();
        let it = DBIter::new(&dbFile);

        let mut res = DBMeta { metaFile: dbFile.clone(), work_dir: work_dir.to_path_buf(), fileMetas: HashSet::new() };

        for (s, _) in it {
            let c = serde_json::from_str::<MetaCommand>(&s).unwrap();
            res.updateMemory(&c);
        };
        return res;
    }

    // for test
    fn listMeta(&self) -> &HashSet<FileMeta> { &self.fileMetas }

    pub fn update(&mut self, c: MetaCommand) {
        self.metaFile.write(&serde_json::to_string::<MetaCommand>(&c).unwrap()).unwrap();
        self.updateMemory(&c);
    }

    fn updateMemory(&mut self, c: &MetaCommand) {
        let cl = c.clone();
        match cl {
            MetaCommand::Insert(m) => {
                self.fileMetas.insert(m);
            }
            MetaCommand::Delete(m) => {
                self.fileMetas.remove(&m);
            }
        }
    }

    pub fn dbSize(&self) -> u64 {
        let res = self.fileMetas.iter().map(|f| { self.fileSize(f.id) }).sum();
        res
    }
    pub fn idToPath(&self, id: &FileId) -> PathBuf {
        let res = self.work_dir.join(Path::new(&id.to_string()));
        res
    }

    pub fn fileSize(&self, id: FileId) -> u64 {
        let p = self.idToPath(&id);
        let m = std::fs::metadata(p).unwrap();
        m.len()
    }

    pub fn maxID(&self) -> Option<FileId> {
        self.fileMetas.iter().map(|f| f.id).max()
    }
}

#[cfg(test)]
mod testDBMeta {
    use std::collections::HashSet;
    use std::fs::OpenOptions;
    use std::path::Path;

    use tempfile::TempDir;

    use crate::db::common::FileId;
    use crate::db::db_meta::{DB_META_FILE_NAME, DBMeta, FileMeta, MetaCommand};
    use std::io::Write;

    fn newFileMeta(id: FileId) -> FileMeta {
        FileMeta::new(id)
    }

    #[test]
    fn testCreateMeta() {
        let tmpDir = TempDir::new().unwrap();
        let p = tmpDir.path().join(Path::new(DB_META_FILE_NAME));
        assert_ne!(p.exists(), true);
        let dbMeta = DBMeta::new(tmpDir.path());
        assert_eq!(p.exists(), true);
    }

    #[test]
    fn testOpenMeta() {
        let tmpDir = TempDir::new().unwrap();
        let p = tmpDir.path().join(Path::new(DB_META_FILE_NAME));
        OpenOptions::new().create(true).write(true).open(p.as_path()).unwrap();
        assert_eq!(p.exists(), true);
        DBMeta::new(tmpDir.path());
    }

    #[test]
    fn testModifyMeta() {
        let tmpDir = TempDir::new().unwrap();
        let mut dbMeta = DBMeta::new(tmpDir.path());
        // add 1,2,3
        dbMeta.update(MetaCommand::Insert(newFileMeta(1)));
        dbMeta.update(MetaCommand::Insert(newFileMeta(2)));
        dbMeta.update(MetaCommand::Insert(newFileMeta(3)));
        // compact 1,2 to 4
        dbMeta.update(MetaCommand::Delete(FileMeta::new(2)));
        drop(dbMeta);

        // reopen
        let mut dbMeta = DBMeta::new(tmpDir.path());

        // add 4
        dbMeta.update(MetaCommand::Insert(newFileMeta(4)));

        // check, should find 1,3,4
        let metas = dbMeta.listMeta().iter().map(|m| m.id).
            collect::<HashSet<FileId>>();
        assert_eq!(metas.contains(&1), true);
        assert_eq!(metas.contains(&2), false);
        assert_eq!(metas.contains(&3), true);
        assert_eq!(metas.contains(&4), true);

        //     test max
        assert_eq!(dbMeta.maxID().unwrap(), 4)
    }

    #[test]
    fn testFileSize() {
        let tmpDir = TempDir::new().unwrap();

        let mut dbMeta = DBMeta::new(tmpDir.path());
        dbMeta.update(MetaCommand::Insert(newFileMeta(1)));
        let mut f = OpenOptions::new().write(true).create(true).
            open(tmpDir.path().join("1")).unwrap();
        f.write("123".as_bytes());
        let size = dbMeta.fileSize(1);
        assert_eq!(size, 3);
    }
}
