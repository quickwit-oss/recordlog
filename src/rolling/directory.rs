// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};

use tokio::fs::{File, OpenOptions};

use crate::position::FileNumber;

pub struct Directory {
    dir: PathBuf,
    // First position in files.
    file_set: BTreeSet<FileNumber>,
}

fn filename_to_position(file_name: &str) -> Option<FileNumber> {
    if file_name.len() != 24 {
        return None;
    }
    if !file_name.starts_with("wal-") {
        return None;
    }
    let seq_number_str = &file_name[4..];
    if !seq_number_str
        .as_bytes()
        .iter()
        .all(|b| (b'0'..=b'9').contains(b))
    {
        return None;
    }
    let global_pos = file_name[4..].parse::<u32>().ok()?;
    Some(FileNumber::from(global_pos))
}

impl Directory {
    pub async fn open(dir_path: &Path) -> io::Result<Directory> {
        let mut seq_numbers: BTreeSet<FileNumber> = Default::default();
        let mut read_dir = tokio::fs::read_dir(dir_path).await?;
        while let Some(dir_entry) = read_dir.next_entry().await? {
            if !dir_entry.file_type().await?.is_file() {
                continue;
            }
            let file_name = if let Some(file_name) = dir_entry.file_name().to_str() {
                file_name.to_string()
            } else {
                continue;
            };
            if let Some(seq_number) = filename_to_position(&file_name) {
                seq_numbers.insert(seq_number);
            }
        }
        Ok(Directory {
            dir: dir_path.to_path_buf(),
            file_set: seq_numbers,
        })
    }

    pub fn num_files(&self) -> usize {
        self.file_set.len()
    }

    pub async fn truncate(&mut self, file_number: FileNumber) -> io::Result<()> {
        if let Some(&first_file_to_retain) = self.file_set.range(..=file_number).last() {
            let mut removed_files = Vec::new();
            for &position in self.file_set.range(..first_file_to_retain) {
                let filepath = self.filepath(position);
                tokio::fs::remove_file(&filepath).await?;
                removed_files.push(position);
            }
            for position in removed_files {
                self.file_set.remove(&position);
            }
        }
        Ok(())
    }

    pub fn file_numbers<'a>(&'a self) -> impl Iterator<Item = FileNumber> + 'a {
        self.file_set.iter().copied()
    }

    fn filepath(&self, seq_number: FileNumber) -> PathBuf {
        self.dir.join(&format!("wal-{seq_number}"))
    }

    pub fn last_file_number(&self) -> FileNumber {
        self.file_set.iter().last().copied().unwrap_or_default()
    }

    pub async fn new_file(&mut self) -> io::Result<File> {
        let mut file_number = self.last_file_number();
        file_number.inc();
        self.file_set.insert(file_number);
        let new_filepath = self.filepath(file_number);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&new_filepath)
            .await?;
        Ok(file)
    }

    pub async fn open_file(&mut self, file_number: FileNumber) -> io::Result<File> {
        let filepath = self.filepath(file_number);
        let file = OpenOptions::new().read(true).open(&filepath).await?;
        Ok(file)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[test]
    fn test_filename_to_seq_number_invalid_prefix_rejected() {
        assert_eq!(filename_to_position("fil-00000000000000000001"), None);
    }

    #[test]
    fn test_filename_to_seq_number_invalid_padding_rejected() {
        assert_eq!(filename_to_position("wal-0000000000000000001"), None);
    }

    #[test]
    fn test_filename_to_seq_number_invalid_len_rejected() {
        assert_eq!(filename_to_position("wal-000000000000000000011"), None);
    }

    #[test]
    fn test_filename_to_seq_number_simple() {
        assert_eq!(
            filename_to_position("wal-00000000000000000001"),
            Some(FileNumber::from(1))
        );
    }

    #[test]
    fn test_filename_to_seq_number() {
        assert_eq!(
            filename_to_position("wal-00000000000000000001"),
            Some(FileNumber::from(1))
        );
    }

    #[tokio::test]
    async fn test_directory() {
        let tmp_dir = tempfile::tempdir().unwrap();
        {
            let mut directory = Directory::open(tmp_dir.path()).await.unwrap();
            let mut file = directory.new_file().await.unwrap();
            file.write_all(b"hello").await.unwrap();
            file.flush().await.unwrap();
        }
        {
            let mut directory = Directory::open(tmp_dir.path()).await.unwrap();
            let file_numbers: Vec<FileNumber> = directory.file_numbers().collect();
            assert_eq!(&file_numbers, &[1.into()]);
            let mut file = directory.new_file().await.unwrap();
            file.write_all(b"hello2").await.unwrap();
            file.flush().await.unwrap()
        }
        {
            let directory = Directory::open(tmp_dir.path()).await.unwrap();
            let filepaths: Vec<FileNumber> = directory.file_numbers().collect();
            assert_eq!(&filepaths, &[1.into(), 2.into()]);
        }
    }

    #[tokio::test]
    async fn test_directory_truncate() {
        let tmp_dir = tempfile::tempdir().unwrap();
        {
            let mut directory = Directory::open(tmp_dir.path()).await.unwrap();
            let mut file = directory.new_file().await.unwrap();
            file.write_all(b"hello").await.unwrap();
            file.flush().await.unwrap();
        }
        {
            let mut directory = Directory::open(tmp_dir.path()).await.unwrap();
            let file_numbers: Vec<FileNumber> = directory.file_numbers().collect();
            assert_eq!(&file_numbers, &[1.into()]);
            let mut file = directory.new_file().await.unwrap();
            file.write_all(b"hello2").await.unwrap();
            file.flush().await.unwrap();
            file.write_all(b"hello3").await.unwrap();
            file.flush().await.unwrap()
        }
        {
            let directory = Directory::open(tmp_dir.path()).await.unwrap();
            let file_numbers: Vec<FileNumber> = directory.file_numbers().collect();
            assert_eq!(&file_numbers, &[1.into(), 2.into()]);
        }
        {
            let mut directory = Directory::open(tmp_dir.path()).await.unwrap();
            directory.truncate(FileNumber::from(3)).await.unwrap();
            let file_numbers: Vec<FileNumber> = directory.file_numbers().collect();
            assert_eq!(&file_numbers, &[2.into()]);
        }
    }
}
