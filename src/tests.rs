use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::frame::BLOCK_LEN;
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
