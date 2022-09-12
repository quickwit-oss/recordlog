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

use std::io;
use std::path::{Path, PathBuf};

use tokio::fs::{File, OpenOptions};

use crate::position::FileNumber;

pub struct Directory {
    dir: PathBuf,
    first_last_files: Option<(FileNumber, FileNumber)>,
}

fn filename_to_position(file_name: &str) -> Option<u32> {
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
    file_name[4..].parse::<u32>().ok()
}

impl Directory {
    pub async fn open(dir_path: &Path) -> io::Result<Directory> {
        let mut file_numbers: Vec<u32> = Default::default();
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
                file_numbers.push(seq_number);
            }
        }
        Ok(Directory {
            dir: dir_path.to_path_buf(),
            first_last_files: FileNumber::from_file_numbers(file_numbers),
        })
    }

    #[cfg(test)]
    pub fn first_last_files(&self) -> Option<(u32, u32)> {
        if let Some((first, last)) = self.first_last_files.as_ref() {
            Some((first.file_number(), last.file_number()))
        } else {
            None
        }
    }

    pub fn first_file_number(&self) -> Option<&FileNumber> {
        self.first_last_files.as_ref().map(|(first, _)| first)
    }

    pub fn last_file_number(&self) -> FileNumber {
        self.first_last_files
            .as_ref()
            .map(|(_first, last)| last.clone())
            .unwrap()
    }

    pub async fn gc(&mut self) -> io::Result<()> {
        let mut file_cursor = if let Some(first_file_number) = self.first_file_number() {
            first_file_number
        } else {
            return Ok(());
        };
        while file_cursor.can_be_deleted() {
            let filepath = self.filepath(&file_cursor);
            tokio::fs::remove_file(&filepath).await?;
            if let Some(next_file) = file_cursor.next() {
                self.set_first_file_number(next_file.clone());
                file_cursor = self.first_file_number().unwrap();
            }
        }
        Ok(())
    }

    fn filepath(&self, file_number: &FileNumber) -> PathBuf {
        self.dir.join(&file_number.filename())
    }

    fn set_first_file_number(&mut self, file_number: FileNumber) {
        let last_file_number = self.last_file_number();
        self.first_last_files = Some((file_number, last_file_number));
    }

    fn set_last_file_number(&mut self, new_file_number: FileNumber) {
        let first_file = self.first_file_number();
        self.first_last_files = if let Some(first) = first_file {
            Some((first.clone(), new_file_number))
        } else {
            Some((new_file_number.clone(), new_file_number))
        };
    }

    pub async fn new_file(&mut self) -> io::Result<File> {
        let next_file_number =
            if let Some((_first_file_number, last_file_number)) = self.first_last_files.as_ref() {
                last_file_number.inc()
            } else {
                FileNumber::default()
            };
        self.set_last_file_number(next_file_number.clone());
        let new_filepath = self.filepath(&next_file_number);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&new_filepath)
            .await?;
        Ok(file)
    }

    pub async fn open_file(&mut self, file_number: FileNumber) -> io::Result<File> {
        let filepath = self.filepath(&file_number);
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
        assert_eq!(filename_to_position("wal-00000000000000000001"), Some(1));
    }

    #[test]
    fn test_filename_to_seq_number() {
        assert_eq!(filename_to_position("wal-00000000000000000001"), Some(1));
    }

    #[tokio::test]
    async fn test_directory_simple() {
        let tmp_dir = tempfile::tempdir().unwrap();
        {
            let mut directory = Directory::open(tmp_dir.path()).await.unwrap();
            let mut file = directory.new_file().await.unwrap();
            file.write_all(b"hello").await.unwrap();
            file.flush().await.unwrap();
        }
        {
            let mut directory = Directory::open(tmp_dir.path()).await.unwrap();
            let first_last: Option<(u32, u32)> = directory.first_last_files();
            assert_eq!(first_last, Some((1, 1)));
            let mut file = directory.new_file().await.unwrap();
            file.write_all(b"hello2").await.unwrap();
            file.flush().await.unwrap()
        }
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
            let first_last: Option<(u32, u32)> = directory.first_last_files();
            assert_eq!(first_last, Some((1, 1)));
            let mut file = directory.new_file().await.unwrap();
            file.write_all(b"hello2").await.unwrap();
            file.flush().await.unwrap()
        }
        {
            let directory = Directory::open(tmp_dir.path()).await.unwrap();
            let first_last: Option<(u32, u32)> = directory.first_last_files();
            assert_eq!(first_last, Some((1, 2)));
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
            let first_last: Option<(u32, u32)> = directory.first_last_files();
            assert_eq!(first_last, Some((1, 1)));
            let mut file = directory.new_file().await.unwrap();
            file.write_all(b"hello2").await.unwrap();
            file.flush().await.unwrap();
            file.write_all(b"hello3").await.unwrap();
            file.flush().await.unwrap()
        }
        {
            let directory = Directory::open(tmp_dir.path()).await.unwrap();
            let first_last: Option<(u32, u32)> = directory.first_last_files();
            assert_eq!(first_last, Some((1, 2)));
        }
        {
            let mut directory = Directory::open(tmp_dir.path()).await.unwrap();
            directory.gc().await.unwrap();
            let first_last: Option<(u32, u32)> = directory.first_last_files();
            assert_eq!(first_last, Some((2, 2)));
        }
    }
}
