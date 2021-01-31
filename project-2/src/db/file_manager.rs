use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use failure::_core::cmp::max;
use log::{info, warn};
use serde::Deserialize;
use serde::Serialize;
use serde_json::map::Entry::Vacant;

use crate::db::db_file::{DBFile, DBIter};
use crate::db::file_manager::MetaCommand::CompactFinish;

use super::common::*;
use super::index;

pub struct ValueIndex {
    pub id: FileId,
    pub offset: FileOffset,
}

pub const FILE_SIZE_LIMIT: u64 = 234;
const START_ID: FileId = 1;

fn idToPath(id: &FileId, work_dir: &PathBuf) -> PathBuf {
    let res = work_dir.join(Path::new(&id.to_string()));
    res
}

pub struct FileManager {
    nextId: FileId,
    meta: DBMeta,
}

impl FileManager {
    pub fn new(workDir: &Path) -> FileManager {
        let meta = DBMeta::new(workDir);
        let nextId = meta.listMeta().iter().map(|m| m.id).max().unwrap_or(START_ID);
        // todo delete unused file
        FileManager { nextId, meta }
    }
    pub fn nextFile(&mut self) -> NormalFileMeta {
        let res = NormalFileMeta { id: self.nextId };
        let c = MetaCommand::AddNormal(res.clone());
        self.meta.update(c);
        self.nextId += 1;
        res
    }
    pub fn nextCompactFile(&mut self, maxId: FileId) -> CompactFileMeta {
        let res = CompactFileMeta { id: self.nextId, maxNormalFileId: maxId };
        self.nextId += 1;
        res
    }

    pub fn compactFinish(&mut self, c: &CompactFileMeta) {
        self.meta.update(MetaCommand::CompactFinish(c.clone()));
        let p = idToPath(&c.id, &self.meta.work_dir);
        self.deleteUnusedFiles(c.maxNormalFileId);
        // std::fs::remove_file(p);
    }

    fn deleteUnusedFiles(&self, maxId: u64) {
        let paths = std::fs::read_dir(&self.meta.work_dir).unwrap();

        for path in paths {
            let name = path.unwrap().file_name().to_str().unwrap().to_owned();
            let res = name.parse::<u64>();
            if let Ok(i) = res {
                if i <= maxId {
                    std::fs::remove_file(&name);
                    info!("delete file {}", &name);
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum FileMeta {
    normal(NormalFileMeta),
    compact(CompactFileMeta, bool),
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, Hash, Clone)]
pub struct NormalFileMeta {
    id: FileId
}

impl NormalFileMeta {
    pub fn new(id: FileId) -> NormalFileMeta {
        NormalFileMeta { id }
    }
    fn toStr(&self) -> String {
        self.id.to_string()
    }

    pub fn getId(&self) -> FileId {
        self.id
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Eq, Hash, Clone)]
pub struct CompactFileMeta {
    id: FileId,
    maxNormalFileId: FileId,
}

impl CompactFileMeta {
    pub fn fileName(&self) -> String {
        format!("{}", self.id)
    }

    pub fn new(id: FileId, maxId: FileId) -> CompactFileMeta {
        assert_eq!(id > maxId, true);
        CompactFileMeta { id, maxNormalFileId: maxId }
    }
}


const DB_META_FILE_NAME: &str = "meta.db";

struct DBMeta {
    file: DBFile,
    work_dir: PathBuf,
    normalFiles: HashSet<NormalFileMeta>,
    compactFile: Option<CompactFileMeta>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum MetaCommand {
    AddCompact(CompactFileMeta),
    AddNormal(NormalFileMeta),
    CompactFinish(CompactFileMeta),
    // Delete(NormalFileMeta),
}

impl DBMeta {
    // todo delete unused file
    fn new(work_dir: &Path) -> DBMeta {
        let p = work_dir.join(DB_META_FILE_NAME);
        let dbFile = DBFile::new(p.as_path()).unwrap();
        let it = DBIter::new(&dbFile);

        let mut res = DBMeta { file: dbFile.clone(), work_dir: work_dir.to_path_buf(), normalFiles: HashSet::new(), compactFile: None };

        for s in it {
            let c = serde_json::from_str::<MetaCommand>(&s).unwrap();
            res.updateMemory(&c);
        };
        return res;
    }

    fn listMeta(&self) -> &HashSet<NormalFileMeta> { &self.normalFiles }

    fn update(&mut self, c: MetaCommand) {
        self.file.write(&serde_json::to_string::<MetaCommand>(&c).unwrap()).unwrap();
        self.updateMemory(&c);
    }

    fn updateMemory(&mut self, c: &MetaCommand) {
        let normalFiles = &mut self.normalFiles;
        let c = c.clone();
        // let mut compactingFiles = HashSet::new();
        match c {
            MetaCommand::AddCompact(c) => {
                assert_eq!(self.compactFile, None);
                // compactingFiles.insert(c);
                self.compactFile = Some(c);
            }
            MetaCommand::AddNormal(n) => {
                normalFiles.insert(n);
            }
            MetaCommand::CompactFinish(c) => {
                assert_eq!(self.compactFile.as_ref().map_or(false, |f| f.id == c.id), true);
                let maxID = c.maxNormalFileId;
                self.compactFile = None;
                let set: HashSet<NormalFileMeta> = self.normalFiles.iter().filter(|m| m.id > maxID).
                    map(|m| m.clone()).collect();
                self.normalFiles = set;
                // todo delete unused file
            }
        }
    }
}


#[cfg(test)]
mod testFm {
    use std::fs::OpenOptions;
    use std::path::Path;
    use std::process::id;

    use tempfile::TempDir;

    use crate::db::file_manager::{CompactFileMeta, DB_META_FILE_NAME, DBMeta, FileManager, MetaCommand, NormalFileMeta};

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
        let n = NormalFileMeta::new(8);
        dbMeta.update(MetaCommand::AddNormal(n.clone()));
        dbMeta.update(MetaCommand::AddCompact(CompactFileMeta::new(7, 4)));
        drop(dbMeta);

        let mut dbMeta = DBMeta::new(tmpDir.path());

        let metas = dbMeta.listMeta();
        assert_eq!(metas.contains(&n), true);

        assert_eq!(dbMeta.compactFile.as_ref().unwrap().id, 7);
        assert_eq!(dbMeta.compactFile.unwrap().maxNormalFileId, 4);
    }

    //
    // #[test]
    // fn debug() {
    //     use regex::Regex;
    //     let re = Regex::new(r"compact_(\d+)").unwrap();
    //     print!("{}", re.captures("compact_3234").unwrap().get(1).unwrap().as_str());
    //     assert!(re.is_match("compact_3234"));
    // }

}