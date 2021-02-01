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
use crate::db::db_meta::{CompactFileMeta, DBMeta, NormalFileMeta, MetaCommand};

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
        let nextId = meta.listMeta().iter().map(|m| m.getId()).max().unwrap_or(START_ID);
        // todo delete unused file
        FileManager { nextId, meta }
    }
    pub fn nextFile(&mut self) -> NormalFileMeta {
        let res = NormalFileMeta::new(self.nextId);
        let c = MetaCommand::AddNormal(res.clone());
        self.meta.update(c);
        self.nextId += 1;
        res
    }
    pub fn nextCompactFile(&mut self, maxId: FileId) -> CompactFileMeta {
        let res = CompactFileMeta::new(self.nextId, maxId);
        self.nextId += 1;
        res
    }

    // pub fn compactFinish(&mut self, c: &CompactFileMeta) {
    //     self.meta.update(MetaCommand::CompactFinish(c.clone()));
    //     let p = idToPath(&c.id, &self.meta.work_dir);
    //     self.deleteUnusedFiles(c.maxNormalFileId);
    // }

    fn deleteUnusedFiles(&self, maxId: u64) {
        let paths = std::fs::read_dir(&self.meta.workDir()).unwrap();

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
