mod queue;
mod queues;

use std::io;

pub use self::queue::MemQueue;
pub use self::queues::MemQueues;
use crate::position::FileNumber;

#[derive(Debug)]
pub enum AppendRecordError {
    Past,
    Future,
    Io(io::Error),
}

impl From<io::Error> for AppendRecordError {
    fn from(io_err: io::Error) -> Self {
        AppendRecordError::Io(io_err)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RecordMeta {
    start_offset: usize,
    file_number: FileNumber,
}
