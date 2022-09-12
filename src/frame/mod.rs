mod header;
mod reader;
mod writer;

use self::header::Header;
pub(crate) use self::header::{FrameType, HEADER_LEN};
pub use self::reader::{FrameReader, ReadFrameError};
pub use self::writer::FrameWriter;
pub(crate) const BLOCK_LEN: usize = 32_768;

#[cfg(test)]
mod tests;
