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

use std::fs::OpenOptions;
use std::path::Path;
use std::path::PathBuf;

use tokio::fs::File;

use crate::RecordWriter;

pub struct RollingRecordWriter {
    path: PathBuf,
    record_writer: RecordWriter<File>,
}

impl RollingRecordWriter {
    // pub(crate) fn open(dir_path: &Path) -> RollinRecordWriter {
        // let file = OpenOptions::new()
        //     .create(true)
        //     .append(true)
        //     .open();
        // RollingRecordWriter {
        //     path: path.to_path_buf(),
        //     record_writer: RecordWriter::append_to(buffer),
        // }
    // }
}
