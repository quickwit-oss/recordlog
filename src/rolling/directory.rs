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
use std::path::Path;
use std::path::PathBuf;

use tokio::fs::File;
use tokio::fs::OpenOptions;

struct Inner {
    dir: PathBuf,
    seq_numbers: BTreeSet<u64>, //< counter for the next file to be created.
                                // No file with this number should exist.
}

pub struct ReadableDirectory {
    inner: Inner,
}

impl ReadableDirectory {
    pub fn files<'a>(&'a self) -> impl Iterator<Item = PathBuf> + 'a {
        self.inner.files()
    }
}

pub struct WritableDirectory {
    inner: Inner,
}

impl WritableDirectory {
    async fn new_file(&mut self) -> io::Result<File> {
        self.inner.new_file().await
    }
}

impl From<ReadableDirectory> for WritableDirectory {
    fn from(readable_dir: ReadableDirectory) -> Self {
        WritableDirectory {
            inner: readable_dir.inner,
        }
    }
}

fn filename_to_seq_number(file_name: &str) -> Option<u64> {
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
    file_name[4..].parse::<u64>().ok()
}

fn seq_number_to_filename(seq_number: u64) -> String {
    format!("wal-{seq_number:020}")
}

impl Inner {
    fn open(dir: &Path) -> io::Result<Inner> {
        let read_dir = dir.read_dir()?;
        let mut seq_numbers: BTreeSet<u64> = Default::default();
        for dir_entry_res in read_dir {
            let dir_entry = dir_entry_res?;
            if !dir_entry.file_type()?.is_file() {
                continue;
            }
            let file_name = if let Some(file_name) = dir_entry.file_name().to_str() {
                file_name.to_string()
            } else {
                continue;
            };
            if let Some(seq_number) = filename_to_seq_number(&file_name) {
                seq_numbers.insert(seq_number);
            }
        }
        Ok(Inner {
            dir: dir.to_path_buf(),
            seq_numbers,
        })
    }

    fn files<'a>(&'a self) -> impl Iterator<Item = PathBuf> + 'a {
        self.seq_numbers
            .iter()
            .copied()
            .map(move |seq_number| self.filepath(seq_number))
    }

    fn filepath(&self, seq_number: u64) -> PathBuf {
        self.dir.join(&format!("wal-{seq_number:020}"))
    }

    async fn new_file(&mut self) -> io::Result<File> {
        let next_seq_number = self
            .seq_numbers
            .iter()
            .last()
            .copied()
            .map(|seq_number| seq_number + 1u64)
            .unwrap_or(0u64);
        self.seq_numbers.insert(next_seq_number);
        let new_filepath = self.filepath(next_seq_number);
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&new_filepath)
            .await
    }
}

pub fn open_directory(path: &Path) -> io::Result<ReadableDirectory> {
    let inner = Inner::open(path)?;
    Ok(ReadableDirectory { inner })
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[test]
    fn test_seq_number_to_filename() {
        assert_eq!(seq_number_to_filename(1u64), "wal-00000000000000000001");
    }

    #[test]
    fn test_filename_to_seq_number_invalid_prefix_rejected() {
        assert_eq!(filename_to_seq_number("fil-00000000000000000001"), None);
    }

    #[test]
    fn test_filename_to_seq_number_invalid_padding_rejected() {
        assert_eq!(filename_to_seq_number("wal-0000000000000000001"), None);
    }

    #[test]
    fn test_filename_to_seq_number_invalid_len_rejected() {
        assert_eq!(filename_to_seq_number("wal-000000000000000000011"), None);
    }

    #[test]
    fn test_filename_to_seq_number_simple() {
        assert_eq!(
            filename_to_seq_number("wal-00000000000000000001"),
            Some(1u64)
        );
    }

    #[test]
    fn test_filename_to_seq_number() {
        assert_eq!(
            filename_to_seq_number("wal-00000000000000000001"),
            Some(1u64)
        );
    }

    fn test_directory_file_aux(directory: &Inner, dir_path: &Path) -> Vec<String> {
        directory
            .files()
            .map(|filepath| {
                assert_eq!(filepath.parent().unwrap(), dir_path);
                filepath.file_name().unwrap().to_str().unwrap().to_string()
            })
            .collect::<Vec<String>>()
    }

    #[tokio::test]
    async fn test_directory() {
        let tmp_dir = tempfile::tempdir().unwrap();
        {
            let mut directory = Inner::open(tmp_dir.path()).unwrap();
            let mut file = directory.new_file().await.unwrap();
            file.write_all(b"hello").await.unwrap();
            file.flush().await.unwrap();
        }
        {
            let mut directory = Inner::open(tmp_dir.path()).unwrap();
            let filepaths = test_directory_file_aux(&directory, tmp_dir.path());
            assert_eq!(&filepaths, &["wal-00000000000000000000"]);
            let mut file = directory.new_file().await.unwrap();
            file.write_all(b"hello2").await.unwrap();
            file.flush().await.unwrap()
        }
        {
            let directory = Inner::open(tmp_dir.path()).unwrap();
            let filepaths = test_directory_file_aux(&directory, tmp_dir.path());
            assert_eq!(
                &filepaths,
                &["wal-00000000000000000000", "wal-00000000000000000001"]
            );
        }
    }
}
