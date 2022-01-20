use thiserror::Error;

use crate::{FrameType, Header, BLOCK_LEN, HEADER_LEN};
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

    pub fn read_record(&mut self) -> Result<Option<&[u8]>, ReadError> {
        self.inner.read_record(&mut self.buffer)
    }
}

impl<R: io::Read + io::Seek> Reader<R> {
    pub fn seek(&mut self, offset: u64) -> io::Result<()> {
        self.inner.seek(offset)
    }

    pub fn set_after_last_valid_record(&mut self) -> io::Result<()> {
        self.inner.set_after_last_valid_record()
    }
}

struct InnerReader<R> {
    read: R,
    in_block_offset: usize,
    block: Box<[u8; BLOCK_LEN]>,
    current_block_len: usize,
    block_dropped: bool,
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
        self.fill_block_buffer()?;
        Ok(())
    }

    // Positions the reader right after the last complete record.
    pub fn set_after_last_valid_record(&mut self) -> io::Result<()> {
        let len = self.read.seek(SeekFrom::End(0))?;
        let num_blocks = len / (BLOCK_LEN as u64);
        for last_block_offset in (0..num_blocks).rev() {
            if self.set_after_last_valid_record_in_block(last_block_offset)? {
                return Ok(());
            }
        }
        Ok(())
    }

    // Attempt to set the reader right after the last complete record in the block.
    // In other words, it positions the reader right after the last frame of type FULL or LAST.
    //
    // If the block does not contain any such block, return `Ok(false)`.
    pub fn set_after_last_valid_record_in_block(&mut self, block_id: u64) -> io::Result<bool> {
        let block_offset = block_id * (BLOCK_LEN as u64);
        self.read.seek(SeekFrom::Start(block_offset))?;
        // let mut last_valid_record_offset = None;
        // let mut record_buffer = Vec::new();
        // self.read_record(&mut record_buffer)?;
        unimplemented!()
    }
}

#[derive(Error, Debug)]
pub enum ReadError {
    #[error("Encounterred an io Error: {0}")]
    Io(#[from] io::Error),
    #[error("Found a corrupted record")]
    Corruption,
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

    /// Attempt to read the next frame.
    ///
    /// Return `Ok(None)` if no frame is available.
    pub fn read_frame(
        &mut self,
        record_buffer: &mut Vec<u8>,
    ) -> Result<Option<FrameType>, ReadError> {
        if self.available_num_bytes() < HEADER_LEN {
            self.fill_block_buffer()?;
            if self.available_num_bytes() < HEADER_LEN {
                return Ok(None);
            }
        }

        let header = Header::deserialize(&self.block[self.in_block_offset..][..HEADER_LEN])
            .map_err(|_| ReadError::Corruption)?;

        let frame_start = self.in_block_offset + HEADER_LEN;
        let frame_end = frame_start + header.len();

        if frame_end > BLOCK_LEN {
            // This is a corruption!
            // Mark this block as dropped and return a corruption.
            self.drop_block();
            return Err(ReadError::Corruption);
        }

        if self.current_block_len < frame_end {
            self.fill_block_buffer()?;
            if self.current_block_len < frame_end {
                return Ok(None);
            }
        }

        let frame_payload = &self.block[frame_start..frame_end];
        self.in_block_offset = frame_end;

        if !header.check(frame_payload) {
            // Our record checksum is wrong. We'd better skip the entire block.
            // As far as we know the `len` could have been wrong too.
            return Err(ReadError::Corruption);
        }

        if header.frame_type.is_first_frame_of_record() {
            record_buffer.clear();
        }
        record_buffer.extend_from_slice(&frame_payload);
        Ok(Some(header.frame_type))
    }

    pub fn read_record<'b, 'a: 'b>(
        &'a mut self,
        record_buffer: &'b mut Vec<u8>,
    ) -> Result<Option<&'b [u8]>, ReadError> {
        if self.block_dropped {
            self.fill_block_buffer()?;
            if self.current_block_len != BLOCK_LEN {
                // We weren't able to read the entire block just yet.
                return Ok(None);
            }
            self.block_dropped = false;
            self.in_block_offset = BLOCK_LEN;
        }

        while let Some(frame_type) = self.read_frame(record_buffer)? {
            // TODO defensively check the frame type state machine?
            // That's not really necessary though.
            if frame_type.is_last_frame_of_record() {
                return Ok(Some(&record_buffer[..]));
            }
        }

        Ok(None)
    }

    /// Attempt to read more data.
    /// This function will fill the current block buffer.
    ///
    /// This method returning Ok(()), even if no data
    /// could be read.
    fn fill_block_buffer(&mut self) -> io::Result<()> {
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
                self.current_block_len = block_len;
                return Ok(());
            }
            block_len += read_len;
        }
        self.current_block_len = BLOCK_LEN;
        // Loaded a full block.
        Ok(())
    }

    fn available_num_bytes(&self) -> usize {
        self.current_block_len - self.in_block_offset
    }
}
