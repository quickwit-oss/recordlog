mod directory;
mod reader;
mod record;
mod writer;

use serde::{Deserialize, Serialize};

pub use self::directory::Directory;
pub use self::reader::RecordLogReader;
pub use self::record::Record;
pub use self::writer::RecordLogWriter;

#[derive(Serialize, Deserialize)]
enum MultiQueueRecord<'a> {
    /// Appends a record to a given queue.
    Record {
        queue: &'a str,
        seq_number: u64,
        payload: &'a [u8],
    },
    /// Truncation happens at the level of a specific queue.
    /// It is simply added to the record log as an operation...
    /// The client will be in charge of detecting when a file can actually
    /// be deleted and will removing the file accordingly.
    Truncate { queue: &'a str, seq_number: u64 },
    /// After removing a log file, in order to avoid losing the last seq_number,
    /// we can append a `LastPosition` record to the log.
    LastPosition { queue: &'a str, seq_number: u64 },
}

#[cfg(test)]
mod tests;
