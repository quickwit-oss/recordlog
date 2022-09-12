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

use tokio::fs::File;
use tokio::io::BufWriter;

const LIMIT_NUM_BYTES: u64 = 50_000_000u64;

use crate::position::FileNumber;
use crate::record::RecordWriter;
use crate::rolling::record::Record;
use crate::rolling::Directory;

pub struct RecordLogWriter {
    record_writer: RecordWriter<BufWriter<File>>,
    directory: super::Directory,
}

async fn new_record_writer(directory: &mut Directory) -> io::Result<RecordWriter<BufWriter<File>>> {
    // TODO sync parent dir.
    let new_file = directory.new_file().await?;
    let buf_writer = tokio::io::BufWriter::new(new_file);
    Ok(RecordWriter::open(buf_writer))
}

impl RecordLogWriter {
    async fn open_new_file(&mut self) -> io::Result<()> {
        self.record_writer.flush().await?;
        self.record_writer
            .get_underlying_wrt()
            .get_mut()
            .sync_all()
            .await?;
        self.record_writer = new_record_writer(&mut self.directory).await?;
        Ok(())
    }

    #[cfg(test)]
    pub fn first_last_files(&self) -> Option<(u32, u32)> {
        self.directory.first_last_files()
    }

    pub fn current_file(&self) -> FileNumber {
        self.directory.last_file_number()
    }

    pub async fn open(mut directory: Directory) -> io::Result<Self> {
        let record_writer = new_record_writer(&mut directory).await?;
        Ok(RecordLogWriter {
            directory,
            record_writer,
        })
    }

    fn need_new_file(&self) -> bool {
        self.record_writer.num_bytes_written() >= LIMIT_NUM_BYTES
    }

    pub async fn write_record(&mut self, record: Record<'_>) -> io::Result<()> {
        if self.need_new_file() {
            self.open_new_file().await?;
        }
        self.record_writer.write_record(record).await?;
        Ok(())
    }

    /// Remove files that only contain records <= position.
    pub async fn gc(&mut self) -> io::Result<()> {
        self.directory.gc().await?;
        Ok(())
    }

    /// Flush in-memory buffer to the OS, and may call fsync or not depending on some
    /// policy.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.record_writer.flush().await?;
        // TODO add file-sync according to some sync policy
        Ok(())
    }
}
