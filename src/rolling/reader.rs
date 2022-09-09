use std::collections::VecDeque;
use std::io;
use std::path::Path;

use tokio::fs::File;

use crate::position::FileNumber;
use crate::record::{ReadRecordError, RecordReader};
use crate::rolling::record::Record;
use crate::rolling::{Directory, RecordLogWriter};

pub struct RecordLogReader {
    directory: Directory,
    file_numbers: VecDeque<FileNumber>,
    reader_opt: Option<(FileNumber, RecordReader<File>)>,
}

impl RecordLogReader {
    pub async fn open(dir_path: &Path) -> io::Result<Self> {
        let directory = Directory::open(dir_path).await?;
        let file_numbers = directory.file_numbers().collect();
        Ok(RecordLogReader {
            file_numbers,
            directory,
            reader_opt: None,
        })
    }

    /// `into_writer` should only be called after the reader has been entirely consumed.
    pub async fn into_writer(mut self) -> Result<RecordLogWriter, ReadRecordError> {
        assert!(
            !self.go_next_record().await?,
            "`into_writer` should only be called after the reader has been entirely consumed"
        );
        Ok(RecordLogWriter::open(self.directory.into()))
    }

    async fn go_next_record_current_reader(&mut self) -> Result<bool, ReadRecordError> {
        if let Some((_file_number, record_reader)) = self.reader_opt.as_mut() {
            record_reader.go_next().await
        } else {
            Ok(false)
        }
    }

    async fn go_next_record(&mut self) -> Result<bool, ReadRecordError> {
        loop {
            if self.go_next_record_current_reader().await? {
                return Ok(true);
            }
            if !self.load_next_file().await? {
                return Ok(false);
            }
        }
    }

    async fn load_next_file(&mut self) -> io::Result<bool> {
        if let Some(next_file_number) = self.file_numbers.pop_front() {
            let next_file = self.directory.open_file(next_file_number).await?;
            let record_reader = RecordReader::open(next_file);
            self.reader_opt = Some((next_file_number, record_reader));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) async fn read_record<'a>(
        &'a mut self,
    ) -> Result<Option<(FileNumber, Record<'a>)>, ReadRecordError> {
        if self.go_next_record().await? {
            let (file_number, record_reader) = self.reader_opt.as_ref().unwrap();
            let record: Record<'a> = record_reader.record().ok_or(ReadRecordError::Corruption)?;
            Ok(Some((*file_number, record)))
        } else {
            Ok(None)
        }
    }
}
