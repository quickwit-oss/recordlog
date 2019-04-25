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
//!
//! // We use a simple `Vec<u8>` to act as our
//! // file in this example.
//! let mut buffer: Vec<u8> = Vec::new();
//!
//! // But the log writer can work with any `io::Write`...
//! {
//! let mut log_writer = recordlog::Writer::new(&mut buffer);
//! log_writer.add_record(b"hello")?;
//! log_writer.add_record(b"happy")?;
//! log_writer.add_record(b"tax")?;
//! log_writer.add_record(b"payer")?;
//! }
//!
//! // ... and the reader can work with any `io::Read`.
//! let mut reader = recordlog::Reader::new(&buffer[..]);
//! assert_eq!(reader.read_record().unwrap(), Some(b"hello".as_ref()) );
//! assert_eq!(reader.read_record().unwrap(), Some(b"happy".as_ref()) );
//! assert_eq!(reader.read_record().unwrap(), Some(b"tax".as_ref()) );
//! assert_eq!(reader.read_record().unwrap(), Some(b"payer".as_ref()) );
//! assert_eq!(reader.read_record().unwrap(), None);
//! # let res: io::Result<()> = Ok(());
//! # return res;
//! Ok(())
//! ```
//!
mod reader;
mod writer;

use crc32fast;

pub use reader::Reader;
pub use writer::Writer;

pub const BLOCK_LEN: usize = 32_768;

pub const HEADER_LEN: usize = 4 + 2 + 1;

fn crc32(data: &[u8]) -> u32 {
    let mut hash = crc32fast::Hasher::default();
    hash.update(data);
    hash.finalize()
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum RecordType {
    FULL = 1u8,
    FIRST = 2u8,
    MIDDLE = 3u8,
    LAST = 4u8,
}

impl RecordType {
    fn from_u8(b: u8) -> Result<RecordType, ()> {
        match b {
            1u8 => Ok(RecordType::FULL),
            2u8 => Ok(RecordType::FIRST),
            3u8 => Ok(RecordType::MIDDLE),
            4u8 => Ok(RecordType::LAST),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TypeState {
    Initial,
    Middle,
}

#[derive(Debug)]
struct Header {
    checksum: u32,
    len: u16,
    record_type: RecordType,
}

impl Header {
    pub fn for_payload(record_type: RecordType, payload: &[u8]) -> Header {
        assert!(payload.len() < BLOCK_LEN);
        Header {
            checksum: crc32(payload),
            len: payload.len() as u16,
            record_type,
        }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn check(&self, payload: &[u8]) -> bool {
        crc32(payload) == self.checksum
    }

    fn serialize(&self, dest: &mut [u8]) {
        assert_eq!(dest.len(), HEADER_LEN);
        dest[..4].copy_from_slice(&self.checksum.to_le_bytes()[..]);
        dest[4..6].copy_from_slice(&self.len.to_le_bytes()[..]);
        dest[6] = self.record_type as u8;
    }

    fn deserialize(data: &[u8]) -> Result<Header, ()> {
        assert_eq!(data.len(), HEADER_LEN);
        let checksum = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let len = u16::from_le_bytes([data[4], data[5]]);
        let record_type = RecordType::from_u8(data[6])?;
        Ok(Header {
            checksum,
            len,
            record_type,
        })
    }
}

#[cfg(test)]
mod tests;
