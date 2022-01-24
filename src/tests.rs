use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::frame::BLOCK_LEN;
use crate::record::FileTrait;
use crate::{ReadRecordError, RecordReader, RecordWriter};
use async_trait::async_trait;
use rand::prelude::*;
use tokio::io::AsyncWrite;

pub struct FaultyWriter<W> {
    wrt: W,
}

impl<W> From<W> for FaultyWriter<W> {
    fn from(wrt: W) -> Self {
        FaultyWriter { wrt }
    }
}

#[async_trait]
impl FileTrait for FaultyWriter<File> {
    async fn sync(&mut self) -> io::Result<()> {
        self.wrt.sync_all()
    }
    async fn pad_for_block_alignment(&mut self) -> io::Result<()> {
        let position_from_start: u64 = self.wrt.seek(SeekFrom::End(0))?;
        let position_within_block = (position_from_start % BLOCK_LEN as u64) as usize;
        if position_within_block != 0 {
            // We are within a block.
            // Let's pad this block. The writer will detect that as
            // corrupted frame, which is fine.
            let pad_num_bytes = BLOCK_LEN - position_within_block;
            let padding_bytes = vec![0u8; pad_num_bytes];
            self.wrt.write_all(&padding_bytes)?;
            self.wrt.sync_all()?;
        }
        Ok(())
    }
}

enum BehaviorOnWrite {
    Success {
        written_len: usize,
    },
    IoError {
        io_err: io::Error,
        written_len: usize,
    },
}

impl<W> FaultyWriter<W> {
    fn behavior_on_write(&self, len: usize) -> BehaviorOnWrite {
        // 90% of the time, a write succeeds.
        let is_success = thread_rng().gen::<f32>() < 0.9f32;
        let written_len = thread_rng().gen_range(0..len);
        if is_success {
            BehaviorOnWrite::Success {
                written_len: written_len.max(1),
            }
        } else {
            BehaviorOnWrite::IoError {
                io_err: io::ErrorKind::Other.into(),
                written_len,
            }
        }
    }
}

impl<W> Unpin for FaultyWriter<W> {}

impl<W: io::Write> AsyncWrite for FaultyWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let behavior = self.behavior_on_write(buf.len());
        match behavior {
            BehaviorOnWrite::Success { written_len } => {
                let inner = Pin::into_inner(self);
                inner.wrt.write(&buf[..written_len]).unwrap();
                Poll::Ready(Ok(written_len))
            }
            BehaviorOnWrite::IoError {
                io_err,
                written_len,
            } => {
                std::thread::sleep(Duration::from_millis(1));
                if written_len > 0 {
                    self.wrt.write(&buf[..written_len]).unwrap();
                }
                Poll::Ready(Err(io_err))
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.wrt.flush().unwrap();
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn test_recordlog_truncation_in_file() {
    let mut buf = Vec::new();
    {
        let mut record_writer = RecordWriter::append_to(&mut buf, false).await.unwrap();
        record_writer.write_record(b"abc").await.unwrap();
    }
    buf.truncate(3);
    {
        let mut record_writer = RecordWriter::append_to(&mut buf, false).await.unwrap();
        record_writer.write_record(b"def").await.unwrap();
    }
    let mut record_reader = RecordReader::open(&buf[..]);
    let res = record_reader.read_record().await;
    assert!(matches!(res, Err(ReadRecordError::Corruption)));
    let res = record_reader.read_record().await;
    assert!(matches!(res, Ok(Some(b"def"))));
}

// Our faulty writer blocks a lot because it was easier to write that way.
// There we use the non-default multithread runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_recordlog_in_file() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let temp_path = temp_file.path().to_path_buf();
    let temp_path_clone = temp_path.clone();
    tokio::task::spawn(async move {
        let mut record_id = 0;
        while record_id < 100 {
            let write_file = OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(&temp_path)
                .unwrap();
            let faulty_writer = FaultyWriter::from(write_file);
            let mut record_writer =
                if let Ok(record_writer) = RecordWriter::append_to(faulty_writer, false).await {
                    record_writer
                } else {
                    continue;
                };
            loop {
                tokio::task::yield_now().await;
                let record_payload = format!("{}", record_id);
                if let Err(_) = record_writer.write_record(record_payload.as_bytes()).await {
                    break;
                }
                // We successfuly wrote the record.
                record_id += 1;
                if record_id == 100 {
                    return;
                }
            }
        }
    });
    let reader_task = tokio::task::spawn(async move {
        let read_file = tokio::fs::File::open(&temp_path_clone).await.unwrap();
        let mut record_reader = RecordReader::open(read_file);
        let mut next_record_id = 0;
        while next_record_id < 100 {
            let read_record_res = record_reader.read_record().await;
            let record = if let Ok(Some(record)) = read_record_res {
                record
            } else if let Ok(None) = read_record_res {
                continue;
            } else {
                continue;
            };
            let record_str = std::str::from_utf8(record).unwrap();
            let record_id: usize = record_str.parse().unwrap();
            if record_id < next_record_id {
                continue;
            }
            assert_eq!(record_id, next_record_id);
            next_record_id += 1;
        }
    });
    reader_task.await.unwrap();
}
