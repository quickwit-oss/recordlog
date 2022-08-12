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

use crate::record::RecordWriter;
use crate::rolling::directory::WritableDirectory;

pub struct RecordLogWriter {
    record_writer_opt: Option<RecordWriter<BufWriter<File>>>,
    directory: super::WritableDirectory,
}

async fn new_record_writer(
    directory: &mut WritableDirectory,
) -> io::Result<RecordWriter<BufWriter<File>>> {
    // TODO sync parent dir.
    let new_file = directory.new_file().await?;
    let buf_writer = tokio::io::BufWriter::new(new_file);
    Ok(RecordWriter::open(buf_writer))
}

impl RecordLogWriter {
    async fn open_new_file(&mut self) -> io::Result<&mut RecordWriter<BufWriter<File>>> {
        if let Some(mut record_writer) = self.record_writer_opt.take() {
            record_writer.flush().await?;
            record_writer
                .get_underlying_wrt()
                .get_mut()
                .sync_all()
                .await?;
        }
        self.record_writer_opt = Some(new_record_writer(&mut self.directory).await?);
        Ok(self.record_writer_opt.as_mut().unwrap())
    }

    pub(crate) fn open(directory: WritableDirectory) -> Self {
        RecordLogWriter {
            directory,
            record_writer_opt: None,
        }
    }

    fn need_new_file(&self) -> bool {
        if let Some(record_writer) = self.record_writer_opt.as_ref() {
            record_writer.num_bytes_written() >= LIMIT_NUM_BYTES
        } else {
            true
        }
    }

    pub async fn record_writer(&mut self) -> io::Result<&mut RecordWriter<BufWriter<File>>> {
        if self.need_new_file() {
            self.open_new_file().await
        } else {
            Ok(self.record_writer_opt.as_mut().unwrap())
        }
    }

    pub async fn write_record(&mut self, payload: &[u8]) -> io::Result<()> {
        self.record_writer().await?.write_record(payload).await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        if let Some(record_writer) = self.record_writer_opt.as_mut() {
            record_writer.flush().await?;
        }
        // TODO add file-sync according to some sync policy
        Ok(())
    }
}
