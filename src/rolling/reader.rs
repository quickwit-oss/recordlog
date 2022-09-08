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
    files: VecDeque<FileNumber>,
    reader_opt: Option<RecordReader<File>>,
    current_file_number: FileNumber, //< Global position of the current record.
}

impl RecordLogReader {
    pub async fn open(dir_path: &Path) -> io::Result<Self> {
        let directory = Directory::open(dir_path).await?;
        let files = directory.file_numbers().collect();
        Ok(RecordLogReader {
            files,
            directory,
            reader_opt: None,
            current_file_number: FileNumber::default(),
        })
    }

    /// Returns the file number of current record.
    pub fn global_position(&self) -> FileNumber {
        self.current_file_number
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
        if let Some(record_reader) = self.reader_opt.as_mut() {
            record_reader.go_next().await
        } else {
            Ok(false)
        }
    }

    async fn go_next_record(&mut self) -> Result<bool, ReadRecordError> {
        if self.go_next_record_current_reader().await? {
            return Ok(true);
        }
        // The loop below is to deal with files containing no valid records.
        // We do not increment the global position here, as at this point we will
        // return the first valid record of a file and `load_next_file`
        // will take care of setting `.global_position` accurately.
        loop {
            if !self.load_next_file().await? {
                return Ok(false);
            }
            if self.go_next_record_current_reader().await? {
                return Ok(true);
            }
        }
    }

    async fn load_next_file(&mut self) -> io::Result<bool> {
        if let Some(next_file_number) = self.files.pop_front() {
            assert!(next_file_number > self.current_file_number);
            let next_file = self.directory.open_file(next_file_number).await?;
            self.current_file_number = next_file_number;
            let record_reader = RecordReader::open(next_file);
            self.reader_opt = Some(record_reader);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) async fn read_record<'a>(
        &'a mut self,
    ) -> Result<Option<Record<'a>>, ReadRecordError> {
        if self.go_next_record().await? {
            let record: Record = self
                .reader_opt
                .as_ref()
                .unwrap()
                .record()
                .ok_or(ReadRecordError::Corruption)?;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }
}
