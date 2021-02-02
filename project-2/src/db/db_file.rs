use failure::_core::cell::RefCell;
use std::fs::{File, OpenOptions};
use std::io::{SeekFrom, Write, Seek, Read};
use std::path::{Path, PathBuf};
use super::common::Result;
use crate::db::common::{FileId, Command, FileOffset};
use crate::db::file_manager::ValueIndex;
use clap::Format;

pub struct DBFile {
    file: RefCell<File>,
    path: String,
    end_position: usize,
}

impl Clone for DBFile {
    fn clone(&self) -> Self {
        DBFile::new_by_file(Path::new(&self.path)).unwrap()
    }
}


impl DBFile {
    fn new_by_file(db_file_path: &Path) -> Result<DBFile> {
        let file = RefCell::new(OpenOptions::new().read(true).append(true).
            create(true).
            open(Path::new(&db_file_path))?);
        let len = file.borrow_mut().seek(SeekFrom::End(0)).unwrap();
        Result::Ok(DBFile { file, path: db_file_path.to_str().unwrap().to_owned(), end_position: len as usize })
    }

    pub fn delete(self) {
        std::fs::remove_file(self.path);
    }

    pub fn new(file: &Path) -> Result<DBFile> {
        DBFile::new_by_file(file)
    }

    pub fn write(&mut self, content: &str) -> Result<FileOffset> {
        let res = self.end_position;

        let b = content.as_bytes();
        let len = b.len().to_be_bytes();
        let file_mut = self.file.get_mut();
        self.end_position += file_mut.write(&len)?;
        self.end_position += file_mut.write(b)?;
        file_mut.flush()?;
        Result::Ok(res as FileOffset)
    }
    pub fn get(&self, offset: FileOffset) -> Result<(String, usize)> {
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
    position: FileOffset,
    db: &'a DBFile,
}

impl<'a> DBIter<'a> {
    pub fn new(db: &DBFile) -> DBIter {
        DBIter { position: 0, db }
    }
}

impl<'a> Iterator for DBIter<'a> {
    type Item = (String, FileOffset);

    fn next(&mut self) -> Option<Self::Item> {
        let p = self.position;
        let (c, size) = self.db.get(self.position).ok()?;
        if size == 0 {
            return None;
        }
        self.position += size as FileOffset;
        Some((c, p))
    }
}