use std::io;
use std::path::Path;

use tokio::fs::File;

use crate::position::FileNumber;
use crate::record::{ReadRecordError, RecordReader};
use crate::rolling::record::Record;
use crate::rolling::{Directory, RecordLogWriter};

pub struct RecordLogReader {
    directory: Directory,
    file_number: Option<FileNumber>,
    reader_opt: Option<(FileNumber, RecordReader<File>)>,
}

impl RecordLogReader {
    pub async fn open(dir_path: &Path) -> io::Result<Self> {
        let directory = Directory::open(dir_path).await?;
        let first_file_number = directory.first_file_number().cloned();
        Ok(RecordLogReader {
            file_number: first_file_number,
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
        Ok(RecordLogWriter::open(self.directory).await?)
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
        if let Some(file_number) = self.file_number.take() {
            let next_file = self.directory.open_file(file_number.clone()).await?;
            let record_reader = RecordReader::open(next_file);
            self.file_number = file_number.next();
            self.reader_opt = Some((file_number, record_reader));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) async fn read_record(
        &mut self,
    ) -> Result<Option<(FileNumber, Record<'_>)>, ReadRecordError> {
        if self.go_next_record().await? {
            let (file_number, record_reader) = self.reader_opt.as_ref().unwrap();
            let record: Record<'_> = record_reader.record().ok_or(ReadRecordError::Corruption)?;
            Ok(Some((file_number.clone(), record)))
        } else {
            Ok(None)
        }
    }
}
