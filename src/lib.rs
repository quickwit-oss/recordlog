//! This library defines a `log`.
//!
//! This log is strongly inspired by leveldb and rocksdb's implementation.
//!
//! The log is a sequence of blocks of `2^15 = 32_768 bytes`.
//! Even when resuming writing a log after a failure, the alignment of
//! blocks is guaranteed by the Writer.
//!
//! Record's payload can be of any size (including 0). They may span over
//! several blocks.
//!
//! The integrity of the log is protected by a checksum at the block
//! level. In case of corruption, some punctual record can be lost, while
//! later records are ok.
//!
//! # Usage
//!
//! ```
//! use std::io;
//! # #[tokio::main]
//! # async fn main() -> Result<(), recordlog::ReadRecordError> {
//! // We use a simple `Vec<u8>` to act as our
//! // file in this example.
//! let mut buffer: Vec<u8> = Vec::new();
//!
//! // But the log writer can work with any `io::Write`...
//! {
//! let mut log_writer = recordlog::RecordWriter::open(&mut buffer);
//! log_writer.write_record(b"hello").await?;
//! log_writer.write_record(b"happy").await?;
//! log_writer.write_record(b"tax").await?;
//! log_writer.write_record(b"payer").await?;
//! }
//!
//! // ... and the reader can work with any `io::Read`.
//! let mut reader = recordlog::RecordReader::open(&buffer[..]);
//! assert_eq!(reader.read_record().await.unwrap(), Some(b"hello".as_ref()) );
//! assert_eq!(reader.read_record().await.unwrap(), Some(b"happy".as_ref()) );
//! assert_eq!(reader.read_record().await.unwrap(), Some(b"tax".as_ref()) );
//! assert_eq!(reader.read_record().await.unwrap(), Some(b"payer".as_ref()) );
//! assert_eq!(reader.read_record().await.unwrap(), None);
//! Ok(())
//! # }
//! ```

mod frame;
mod record;

pub use record::{ReadRecordError, RecordReader, RecordWriter};
