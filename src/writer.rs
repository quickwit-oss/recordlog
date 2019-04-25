use crate::{Header, RecordType, BLOCK_LEN, HEADER_LEN};
use std::io;

const PADDING_ZEROES: [u8; HEADER_LEN] = [0u8; HEADER_LEN];

pub struct Writer<W: io::Write> {
    write: W,
    current_block_len: usize,
    buffer: Box<[u8; BLOCK_LEN]>,
}

impl<W: io::Write> Writer<W> {
    pub fn new(w: W) -> Writer<W> {
        Writer {
            write: w,
            current_block_len: 0,
            buffer: Box::new([0u8; BLOCK_LEN]),
        }
    }
}

impl<W: io::Write> Writer<W> {
    /// Appends a new record to the log.
    ///
    /// Records of length 0 are allowed.
    pub fn add_record(&mut self, mut record_payload: &[u8]) -> io::Result<()> {
        let mut first_chunk = true;

        // The `first_chunk` is here to make sure we handle empty record
        // correctly.
        while first_chunk || !record_payload.is_empty() {
            assert!(BLOCK_LEN >= self.current_block_len);
            let remaining_block_len = BLOCK_LEN - self.current_block_len;
            if remaining_block_len < HEADER_LEN {
                // There isn't enough room in the given block
                // to write even the header of a chunk, we stop reading this
                // block and jump to the next one.
                //
                // If there is exactly the room for a header, we still
                // write this header, even if that chunks payload will be empty.
                self.write
                    .write_all(&PADDING_ZEROES[..remaining_block_len])?;
                self.current_block_len = 0;
                continue;
            }

            // Because of the above, if statement, we are now sure that
            // the current block has enough room remaining.
            assert!(remaining_block_len >= HEADER_LEN);

            // All chunks are prepended by a header. So our usable size is not
            // `remaining_block_len` but `remaining_block_len - HEADER_LEN`.
            let available: usize = remaining_block_len - HEADER_LEN;
            let chunk_payload_len = available.min(record_payload.len());
            let (chunk_payload, remaining_payload) = record_payload.split_at(chunk_payload_len);
            // If last_chunk is true, we have enough room in the current block to
            // put all of our record payload.
            let last_chunk = remaining_payload.is_empty();

            let record_type = match (first_chunk, last_chunk) {
                (true, true) => RecordType::FULL,
                (true, false) => RecordType::FIRST,
                (false, false) => RecordType::MIDDLE,
                (false, true) => RecordType::LAST,
            };

            self.write_record(record_type, chunk_payload)?;
            record_payload = remaining_payload;
            first_chunk = false;
        }
        Ok(())
    }

    pub fn force_flush(&mut self) -> io::Result<()> {
        self.write.flush()
    }

    fn write_record(&mut self, record_type: RecordType, data: &[u8]) -> io::Result<()> {
        Header::for_payload(record_type, data).serialize(&mut self.buffer[..HEADER_LEN]);
        let record_len = HEADER_LEN + data.len();
        self.buffer[HEADER_LEN..record_len].copy_from_slice(data);
        self.current_block_len += record_len;
        self.write.write_all(&self.buffer[..record_len])
    }
}
