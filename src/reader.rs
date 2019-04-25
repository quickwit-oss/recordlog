use crate::{Header, RecordType, TypeState, BLOCK_LEN, HEADER_LEN};
use std::io::{self, SeekFrom};

pub struct Reader<R> {
    inner: InnerReader<R>,
    // This buffer is only useful to incrementally build records
    // that did not fit in a single chunk.
    buffer: Vec<u8>,
}

impl<R: io::Read> Reader<R> {
    pub fn new(read: R) -> Reader<R> {
        Reader {
            inner: InnerReader::new(read),
            buffer: Vec::with_capacity(BLOCK_LEN * 10),
        }
    }

    pub fn read_record(&mut self) -> Result<Option<&[u8]>, RecordReadError> {
        self.inner.read_record(&mut self.buffer)
    }
}

impl<R: io::Read + io::Seek> Reader<R> {
    pub fn seek(&mut self, offset: u64) -> io::Result<()> {
        self.inner.seek(offset)
    }
}

struct InnerReader<R> {
    read: R,
    in_block_offset: usize,
    block: Box<[u8; BLOCK_LEN]>,
    current_block_len: usize,
    block_dropped: bool,
}

#[derive(Debug)]
pub enum RecordReadError {
    IO(io::Error),
    Corruption,
    IncompleteRecord,
}

impl From<io::Error> for RecordReadError {
    fn from(io_error: io::Error) -> Self {
        RecordReadError::IO(io_error)
    }
}

impl<R: io::Read + io::Seek> InnerReader<R> {
    pub fn seek(&mut self, offset: u64) -> io::Result<()> {
        let in_block_offset = (offset % (BLOCK_LEN as u64)) as usize;
        if in_block_offset + HEADER_LEN >= BLOCK_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid offset".to_string(),
            ));
        }
        let block_offset = offset - in_block_offset as u64;
        self.read.seek(SeekFrom::Start(block_offset))?;
        self.in_block_offset = in_block_offset;
        self.load_block()?;
        Ok(())
    }
}

impl<R: io::Read> InnerReader<R> {
    pub fn new(read: R) -> InnerReader<R> {
        InnerReader {
            read,
            in_block_offset: 0,
            block: Box::new([0u8; BLOCK_LEN]),
            current_block_len: 0,
            block_dropped: false,
        }
    }

    pub fn drop_block(&mut self) {
        self.block_dropped = true;
    }

    pub fn read_record<'b, 'a: 'b>(
        &'a mut self,
        buffer: &'b mut Vec<u8>,
    ) -> Result<Option<&'b [u8]>, RecordReadError> {
        buffer.clear();
        while self.block_dropped {
            if !self.load_block()? {
                return Ok(None);
            }
        }
        let mut state = TypeState::Initial;
        loop {
            if self.data().len() < HEADER_LEN {
                // handle eof.
                if !self.load_block()? {
                    return Ok(None);
                }
                continue;
            }
            let data = self.data();
            let header = Header::deserialize(&self.data()[..HEADER_LEN])
                .map_err(|_| RecordReadError::Corruption)?;
            let chunk = &data[HEADER_LEN..][..header.len()];
            if !header.check(chunk) {
                // Our record checksum is wrong. We'd better skip the entire block.
                // As far as we know the `len` could have been wrong too.
                self.drop_block();
                return Err(RecordReadError::Corruption);
            }

            let record_start = self.in_block_offset + HEADER_LEN;
            let record_stop = record_start + header.len as usize;

            match (state, header.record_type) {
                (TypeState::Initial, RecordType::FIRST)
                | (TypeState::Middle, RecordType::MIDDLE) => {
                    state = TypeState::Middle;
                    self.in_block_offset = record_stop;
                    buffer.extend_from_slice(&self.block[record_start..record_stop]);
                }
                (TypeState::Initial, RecordType::FULL) => {
                    // Avoiding the extra copy, return from the block buffer directly.
                    self.in_block_offset = record_stop;
                    return Ok(Some(&self.block[record_start..record_stop]));
                }
                (TypeState::Middle, RecordType::LAST) => {
                    // This is the last chunk of our record.
                    // We can emit our result.
                    self.in_block_offset = record_stop;
                    buffer.extend_from_slice(&self.block[record_start..record_stop]);
                    return Ok(Some(&buffer[..]));
                }
                (TypeState::Initial, _) | (_, RecordType::FULL) | (_, RecordType::FIRST) => {
                    return Err(RecordReadError::IncompleteRecord);
                }
            };
        }
    }

    /// Loads a new block.
    ///
    /// Returns true iff new data was available.
    /// The block is not necessarily complete.
    ///
    /// If a writer is appending data to this stream, it is possible, and allowed
    /// for`.load_block()` to return `Ok(false)`, and then later `Ok(true)`.
    fn load_block(&mut self) -> io::Result<bool> {
        let mut block_len;
        if self.current_block_len == BLOCK_LEN {
            // The current block has been entirely loaded.
            // Let's load the next one.
            self.block_dropped = true;
            self.current_block_len = 0;
            self.in_block_offset = 0;
            block_len = 0;
        } else {
            // Our current block is incomplete.
            // Let's try to complete it.
            block_len = self.current_block_len;
        };
        while block_len != BLOCK_LEN {
            let read_len = self.read.read(&mut self.block[block_len..])?;
            if read_len == 0 {
                if block_len == self.current_block_len {
                    // No data was read at all. We reach the end of our stream.
                    return Ok(false);
                } else {
                    self.current_block_len = block_len;
                    // returning a partial block.
                    return Ok(true);
                }
            }
            block_len += read_len;
        }
        self.current_block_len = BLOCK_LEN;
        // Loaded a full block.
        Ok(true)
    }

    fn data(&self) -> &[u8] {
        &self.block[self.in_block_offset..self.current_block_len]
    }
}
