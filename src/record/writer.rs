use std::io::SeekFrom;

use crate::frame::{FrameType, FrameWriter, BLOCK_LEN};
use async_trait::async_trait;
use tokio::fs::File;
use tokio::io::{self, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

pub struct RecordWriter<W> {
    frame_writer: FrameWriter<W>,
}

impl<W: io::AsyncWrite> RecordWriter<W> {}

fn frame_type(is_first_frame: bool, is_last_frame: bool) -> FrameType {
    match (is_first_frame, is_last_frame) {
        (true, true) => FrameType::FULL,
        (true, false) => FrameType::FIRST,
        (false, true) => FrameType::LAST,
        (false, false) => FrameType::MIDDLE,
    }
}

impl<W: io::AsyncWrite + Unpin> RecordWriter<W> {
    pub fn open(wrt: W) -> Self {
        let frame_writer = FrameWriter::create_with_aligned_write(wrt);
        RecordWriter { frame_writer }
    }
}

impl<W: AsyncWrite + Unpin> RecordWriter<W> {
    /// Writes a record.
    ///
    /// Even if this call returns `Ok(())`, at this point the data
    /// is likely to be not durably stored on disk.
    ///
    /// For instance, the data could be stale in a library level buffer,
    /// by a writer level buffer, or an application buffer,
    /// or could not be flushed to disk yet by the OS.
    pub async fn write_record(&mut self, mut payload: &[u8]) -> io::Result<()> {
        let mut is_first_frame = true;
        loop {
            let frame_payload_len = self
                .frame_writer
                .max_writable_frame_length()
                .min(payload.len());
            let frame_payload = &payload[..frame_payload_len];
            payload = &payload[frame_payload_len..];
            let is_last_frame = payload.is_empty();
            let frame_type = frame_type(is_first_frame, is_last_frame);
            self.frame_writer
                .write_frame(frame_type, frame_payload)
                .await?;
            is_first_frame = false;
            if is_last_frame {
                break;
            }
        }
        Ok(())
    }

    /// See `write_record`.
    /// If an io::Error is returned, any number of record may have been written.
    pub async fn write_record_batch(
        &mut self,
        payloads: impl Iterator<Item = &[u8]>,
    ) -> io::Result<()> {
        for record_payload in payloads {
            self.write_record(record_payload).await?;
        }
        Ok(())
    }

    /// Flushes and sync the data to disk.
    pub async fn flush(&mut self) -> io::Result<()> {
        // Empty the application buffer.
        self.frame_writer.flush().await?;
        Ok(())
    }
}
