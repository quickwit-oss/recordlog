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
use std::fs::FileType;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use tokio::fs::File;
use tokio::fs::OpenOptions;

pub struct Directory {
    dir: PathBuf,
    seq_numbers: BTreeSet<u64>, //< counter for the next file to be created.
        // No file with this number should exist.
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
        .all(|b| (b'0'..=b'9').contains(b)) {
        return None;
    }
    file_name[4..].parse::<u64>().ok()
}

fn seq_number_to_filename(seq_number: u64) -> String {
    format!("wal-{seq_number:020}")

}

impl Directory {
    pub fn open(dir: &Path) -> io::Result<Directory> {
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
        Ok(Directory {
            dir: dir.to_path_buf(),
            seq_numbers,
        })
    }

    pub async fn new_file(&mut self,) -> io::Result<File> {
        let next_seq_number = self.seq_numbers.iter()
            .last()
            .copied()
            .map(|seq_number| seq_number + 1u64)
            .unwrap_or(0u64);
        self.seq_numbers.insert(next_seq_number);
        let new_filepath = self.dir
            .join(&format!("wal-{next_seq_number:020}"));
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&new_filepath)
            .await
    }
}

#[cfg(test)]
mod tests {
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
        assert_eq!(filename_to_seq_number("wal-00000000000000000001"), Some(1u64));
    }

    #[test]
    fn test_filename_to_seq_number() {
        assert_eq!(filename_to_seq_number("wal-00000000000000000001"), Some(1u64));
    }
}
