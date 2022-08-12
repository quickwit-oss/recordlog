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
mod frame;
mod record;
mod rolling;

pub use rolling::{RecordLogReader, RecordLogWriter};
