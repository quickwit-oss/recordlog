mod reader;
mod writer;
pub use self::reader::{ReadRecordError, RecordReader};
pub use self::writer::{FileTrait, RecordWriter};

#[cfg(test)]
mod tests;
