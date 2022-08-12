use crate::frame::{FrameReader, ReadFrameError};
use std::io;
use thiserror::Error;
use tokio::io::AsyncRead;

pub struct RecordReader<R> {
    frame_reader: FrameReader<R>,
    record_buffer: Vec<u8>,
    // true if we are in the middle of reading a multifragment record.
    // This is useful, as it makes it possible to drop a record
    // if one of its fragment was corrupted.
    within_record: bool,
}

#[derive(Error, Debug)]
pub enum ReadRecordError {
    #[error("Io error: {0}")]
    IoError(#[from] io::Error),
    #[error("Corruption")]
    Corruption,
}

impl<R: AsyncRead + Unpin> RecordReader<R> {
    pub fn open(reader: R) -> Self {
        let frame_reader = FrameReader::open(reader);
        RecordReader {
            frame_reader,
            record_buffer: Vec::with_capacity(10_000),
            within_record: false,
        }
    }

    pub fn record(&self) -> &[u8] {
        &self.record_buffer
    }

    pub async fn read_record(&mut self) -> Result<Option<&[u8]>, ReadRecordError> {
        let has_record = self.go_next().await?;
        if has_record {
            Ok(Some(self.record()))
        } else {
            Ok(None)
        }
    }

    // Attempts to position the reader to the next record and return
    // true or false whether such a record is available or not.
    pub async fn go_next(&mut self) -> Result<bool, ReadRecordError> {
        loop {
            match self.frame_reader.read_frame().await {
                Ok((fragment_type, frame_payload)) => {
                    if fragment_type.is_first_frame_of_record() {
                        self.within_record = true;
                        self.record_buffer.clear();
                    }
                    if self.within_record {
                        self.record_buffer.extend_from_slice(frame_payload);
                    }
                    if fragment_type.is_last_frame_of_record() {
                        if self.within_record {
                            self.within_record = false;
                            return Ok(true);
                        }
                    }
                }
                Err(ReadFrameError::Corruption) => {
                    self.within_record = false;
                    return Err(ReadRecordError::Corruption);
                }
                Err(ReadFrameError::IoError(io_err)) => {
                    self.within_record = false;
                    return Err(ReadRecordError::IoError(io_err));
                }
                Err(ReadFrameError::NotAvailable) => {
                    return Ok(false);
                }
            }
        }
    }
}
