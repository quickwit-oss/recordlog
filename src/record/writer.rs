use crate::frame::{FrameType, FrameWriter};
use async_trait::async_trait;
use tokio::fs::File;
use tokio::io::{self, AsyncWrite};

pub struct RecordWriter<W> {
    frame_writer: FrameWriter<W>,
    sync_mode: bool,
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

impl<'a> RecordWriter<&'a mut Vec<u8>> {
    pub fn open(buffer: &'a mut Vec<u8>) -> Self {
        let frame_writer = FrameWriter::create_with_aligned_write(buffer);
        RecordWriter {
            frame_writer,
            sync_mode: false,
        }
    }
}

impl<W: Syncable + io::AsyncWrite + io::AsyncSeek + Unpin> RecordWriter<W> {
    /// `sync_mode` controls whether the writer syncs to disk
    /// after each record or not.
    ///
    /// Regardless of whether sync_mode is set to true or not,
    /// application-levels buffer will be flushed to disk after each
    /// call to `.write_record()` or `.write_records()`.
    ///
    /// At this point, however, the OS has not written the data down to disk.
    /// `sync_mode` enforces an extra call to `fsync`.
    ///
    /// It offers better durability guarantees, at the cost of performance.
    pub async fn append_to(wrt: W, sync_mode: bool) -> io::Result<Self> {
        let frame_writer = FrameWriter::append_to(wrt).await?;
        Ok(RecordWriter {
            frame_writer,
            sync_mode,
        })
    }
}

impl<W: Syncable + AsyncWrite + Unpin> RecordWriter<W> {
    async fn write_single_record(&mut self, mut payload: &[u8]) -> io::Result<()> {
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
        self.frame_writer.flush().await?;
        if self.sync_mode {
            self.frame_writer.get_underlying_wrt().sync().await?;
        }
        Ok(())
    }

    /// If Ok is returned, the record has been written correctly to the OS
    /// (and fsync'ed if sync_mode==true).
    /// If an io Error is returned, the record may or may not have been written.
    pub async fn write_record(&mut self, payload: &[u8]) -> io::Result<()> {
        self.write_single_record(payload).await?;
        self.frame_writer.flush().await?;
        if self.sync_mode {
            self.frame_writer.get_underlying_wrt().sync().await?;
        }
        Ok(())
    }

    /// If Ok is returned the record have been written correctly
    /// (and fsync'ed if sync_mode==true).
    /// If an io::Error is returned, any number of record may have been written.
    ///
    /// If sync_mode is set to true, fsync will happen only after the entire batch
    /// has been written.
    pub async fn write_record_batch(
        &mut self,
        payloads: impl Iterator<Item = &[u8]>,
    ) -> io::Result<()> {
        for record_payload in payloads {
            self.write_single_record(record_payload).await?;
        }
        self.frame_writer.flush().await?;
        if self.sync_mode {
            self.frame_writer.get_underlying_wrt().sync().await?;
        }
        Ok(())
    }
}

impl RecordWriter<File> {
    /// Flushes and sync the data to disk.
    pub async fn sync(&mut self) -> io::Result<()> {
        self.frame_writer.get_underlying_wrt().sync().await?;
        Ok(())
    }
}

#[async_trait]
pub trait Syncable {
    async fn sync(&mut self) -> io::Result<()>;
}

#[async_trait]
impl Syncable for File {
    async fn sync(&mut self) -> io::Result<()> {
        self.sync_all().await?;
        Ok(())
    }
}

#[async_trait]
impl Syncable for Vec<u8> {
    async fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl<'a> Syncable for &'a mut Vec<u8> {
    async fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}
