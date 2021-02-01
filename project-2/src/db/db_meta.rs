use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde::Serialize;

use crate::db::db_file::{DBFile, DBIter};

use super::common::*;

const DB_META_FILE_NAME: &str = "meta.db";

pub struct DBMeta {
    metaFile: DBFile,
    work_dir: PathBuf,
    fileIdS: HashSet<FileId>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MetaCommand {
    Add(FileId),
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

        let mut res = DBMeta { metaFile: dbFile.clone(), work_dir: work_dir.to_path_buf(), fileIdS: HashSet::new() };

        for s in it {
            let c = serde_json::from_str::<MetaCommand>(&s).unwrap();
            res.updateMemory(&c);
        };
        return res;
    }

    pub fn listMeta(&self) -> &HashSet<FileId> { &self.fileIdS }

    pub fn update(&mut self, c: MetaCommand) {
        self.metaFile.write(&serde_json::to_string::<MetaCommand>(&c).unwrap()).unwrap();
        self.updateMemory(&c);
    }

    fn updateMemory(&mut self, c: &MetaCommand) {
        match c {
            MetaCommand::Add(id) => {
                self.fileIdS.insert(*id);
            }
            MetaCommand::Delete(id) => {
                self.fileIdS.remove(&id);
            }
        }
    }
}

#[cfg(test)]
mod testDBMeta {
    use std::fs::OpenOptions;
    use std::path::Path;
    use std::process::id;

    use tempfile::TempDir;

    use crate::db::db_meta::{DB_META_FILE_NAME, DBMeta, MetaCommand};

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
        dbMeta.update(MetaCommand::Add(1));
        dbMeta.update(MetaCommand::Add(2));
        dbMeta.update(MetaCommand::Add(3));
        // delete
        dbMeta.update(MetaCommand::Delete(2));
        drop(dbMeta);

        // reopen
        let mut dbMeta = DBMeta::new(tmpDir.path());

        // add 4
        dbMeta.update(MetaCommand::Add(4));

        // check, should find 1,3,4
        let metas = dbMeta.listMeta();
        assert_eq!(metas.contains(&1), true);
        assert_eq!(metas.contains(&3), true);
        assert_eq!(metas.contains(&4), true);
        assert_eq!(metas.contains(&2), false);
    }
}
