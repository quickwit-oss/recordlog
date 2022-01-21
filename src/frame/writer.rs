use std::io::{self, SeekFrom};
use tokio::io::{AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufWriter};

use crate::frame::{FrameType, Header, BLOCK_LEN, HEADER_LEN};

pub(crate) struct FrameWriter<W> {
    wrt: BufWriter<W>,
    buffer: Box<[u8; BLOCK_LEN]>,
    current_block_len: usize,
}

impl<W: AsyncWrite + AsyncSeek + Unpin> FrameWriter<W> {
    pub async fn append_to(mut wrt: W) -> io::Result<Self> {
        let position_from_start: u64 = wrt.seek(SeekFrom::Current(0)).await?;
        let position_within_block = (position_from_start % BLOCK_LEN as u64) as usize;
        if position_within_block != 0 {
            // We are within a block.
            // Let's pad this block. The writer will detect that as
            // corrupted frame, which is fine.
            let pad_num_bytes = BLOCK_LEN - position_within_block;
            let padding_bytes = vec![0u8; pad_num_bytes];
            wrt.write_all(&padding_bytes).await?;
        }
        Ok(Self::create_with_aligned_write(wrt))
    }
}

impl<W: AsyncWrite + Unpin> FrameWriter<W> {
    pub(crate) fn create_with_aligned_write(wrt: W) -> Self {
        FrameWriter {
            wrt: BufWriter::new(wrt),
            buffer: Box::new([0u8; BLOCK_LEN]),
            current_block_len: 0,
        }
    }

    pub async fn write_frame<B: AsRef<[u8]>>(
        &mut self,
        frame_type: FrameType,
        payload: B,
    ) -> io::Result<()> {
        let payload = payload.as_ref();
        if self.available_num_bytes_in_block() < HEADER_LEN {
            self.pad_block().await?;
        }
        assert!(payload.len() <= self.max_writable_frame_length());
        let record_len = HEADER_LEN + payload.len();
        assert!(record_len <= BLOCK_LEN);
        Header::for_payload(frame_type, payload).serialize(&mut self.buffer[..HEADER_LEN]);
        self.buffer[HEADER_LEN..record_len].copy_from_slice(payload);
        self.current_block_len = (self.current_block_len + record_len) % BLOCK_LEN;
        self.wrt.write_all(&self.buffer[..record_len]).await?;
        Ok(())
    }

    /// Flush the buffered writer used in the FrameWriter.
    ///
    /// When writing to a file, this performs a syscall and
    /// the OS will be in charge of eventually writing the data
    /// to disk, but this is not sufficient to ensure durability.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.wrt.flush().await
    }

    async fn pad_block(&mut self) -> io::Result<()> {
        let remaining_num_bytes_in_block = self.available_num_bytes_in_block();
        let b = vec![0u8; remaining_num_bytes_in_block];
        self.wrt.write_all(&b).await?;
        Ok(())
    }

    fn available_num_bytes_in_block(&self) -> usize {
        BLOCK_LEN - self.current_block_len
    }

    /// Returns he
    pub fn max_writable_frame_length(&self) -> usize {
        let available_num_bytes_in_block = self.available_num_bytes_in_block();
        if available_num_bytes_in_block >= HEADER_LEN {
            available_num_bytes_in_block - HEADER_LEN
        } else {
            // That block is finished. We will have to pad it.
            BLOCK_LEN - HEADER_LEN
        }
    }

    pub fn get_underlying_wrt(&mut self) -> &mut W {
        self.wrt.get_mut()
    }
}
