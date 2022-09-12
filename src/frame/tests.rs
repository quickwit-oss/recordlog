use std::io;

use crate::frame::header::{FrameType, HEADER_LEN};
use crate::frame::{FrameReader, FrameWriter, ReadFrameError, BLOCK_LEN};

#[tokio::test]
async fn test_frame_simple() -> io::Result<()> {
    let mut wrt: Vec<u8> = Vec::new();
    {
        let mut frame_writer = FrameWriter::create(&mut wrt);
        frame_writer
            .write_frame(FrameType::First, &b"abc"[..])
            .await?;
        frame_writer
            .write_frame(FrameType::Middle, &b"de"[..])
            .await?;
        frame_writer
            .write_frame(FrameType::Last, &b"fgh"[..])
            .await?;
        frame_writer.flush().await?;
    }
    let mut frame_reader = FrameReader::open(&wrt[..]);
    assert!(matches!(
        frame_reader.read_frame().await,
        Ok((FrameType::First, b"abc"))
    ));
    assert!(matches!(
        frame_reader.read_frame().await,
        Ok((FrameType::Middle, b"de"))
    ));
    assert!(matches!(
        frame_reader.read_frame().await,
        Ok((FrameType::Last, b"fgh"))
    ));
    assert!(matches!(
        frame_reader.read_frame().await,
        Err(ReadFrameError::NotAvailable)
    ));
    Ok(())
}

#[tokio::test]
async fn test_frame_partial() -> io::Result<()> {
    let mut wrt: Vec<u8> = Vec::new();
    {
        let mut frame_writer = FrameWriter::create(&mut wrt);
        frame_writer
            .write_frame(FrameType::First, &b"abc"[..])
            .await?;
        frame_writer.flush().await?;
    }
    assert_eq!(wrt.len(), HEADER_LEN + 3);
    wrt.truncate(HEADER_LEN + 2);
    let mut frame_reader = FrameReader::open(&wrt[..]);
    assert!(matches!(
        frame_reader.read_frame().await,
        Err(ReadFrameError::NotAvailable)
    ));
    Ok(())
}

#[tokio::test]
async fn test_frame_corruption_in_payload() -> io::Result<()> {
    let mut wrt: Vec<u8> = Vec::new();
    {
        let mut frame_writer = FrameWriter::create(&mut wrt);
        frame_writer
            .write_frame(FrameType::First, &b"abc"[..])
            .await?;
        frame_writer.flush().await?;
    }
    {
        let mut frame_writer = FrameWriter::create(&mut wrt);
        frame_writer
            .write_frame(FrameType::Middle, &b"de"[..])
            .await?;
        frame_writer.flush().await?;
    }
    wrt[8] = 0u8;
    let mut frame_reader = FrameReader::open(&wrt[..]);
    assert!(matches!(
        frame_reader.read_frame().await,
        Err(ReadFrameError::Corruption)
    ));
    assert!(matches!(
        frame_reader.read_frame().await,
        Ok((FrameType::Middle, b"de"))
    ));
    Ok(())
}

async fn repeat_empty_frame_util(repeat: usize) -> Vec<u8> {
    let mut wrt: Vec<u8> = Vec::new();
    {
        let mut frame_writer = FrameWriter::create(&mut wrt);
        for _ in 0..repeat {
            frame_writer
                .write_frame(FrameType::Full, &b""[..])
                .await
                .unwrap();
        }
        frame_writer.flush().await.unwrap();
    }
    wrt
}

#[tokio::test]
async fn test_simple_multiple_blocks() -> io::Result<()> {
    let num_frames = 1 + BLOCK_LEN / HEADER_LEN;
    let buffer = repeat_empty_frame_util(num_frames).await;
    assert_eq!(buffer.len(), BLOCK_LEN + HEADER_LEN);
    let mut frame_reader = FrameReader::open(&buffer[..]);
    for _ in 0..num_frames {
        let read_frame_res = frame_reader.read_frame().await;
        assert!(matches!(read_frame_res, Ok((FrameType::Full, &[]))));
    }
    assert!(matches!(
        frame_reader.read_frame().await,
        Err(ReadFrameError::NotAvailable)
    ));
    Ok(())
}

#[tokio::test]
async fn test_multiple_blocks_corruption_on_length() -> io::Result<()> {
    // We end up with 4681 frames on the first block.
    // 1 frame on the second block
    let num_frames = 1 + BLOCK_LEN / HEADER_LEN;
    let mut buffer = repeat_empty_frame_util(num_frames).await;
    buffer[2000 * HEADER_LEN + 5] = 255u8;
    assert_eq!(buffer.len(), BLOCK_LEN + HEADER_LEN);
    let mut frame_reader = FrameReader::open(&buffer[..]);
    for _ in 0..2000 {
        let read_frame_res = frame_reader.read_frame().await;
        assert!(matches!(read_frame_res, Ok((FrameType::Full, &[]))));
    }
    assert!(matches!(
        frame_reader.read_frame().await,
        Err(ReadFrameError::Corruption)
    ));
    assert!(matches!(
        frame_reader.read_frame().await,
        Ok((FrameType::Full, &[]))
    ));
    assert!(matches!(
        frame_reader.read_frame().await,
        Err(ReadFrameError::NotAvailable)
    ));
    Ok(())
}
