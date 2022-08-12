use super::{ReadRecordError, RecordReader, RecordWriter};
use crate::frame::{BLOCK_LEN, HEADER_LEN};

#[tokio::test]
async fn test_no_data() {
    let mut reader = RecordReader::open(&b""[..]);
    assert!(matches!(reader.read_record().await, Ok(None)));
}

#[tokio::test]
async fn test_empty_record() {
    let mut buffer = Vec::new();
    let mut writer = RecordWriter::open(&mut buffer);
    writer.write_record(b"").await.unwrap();
    writer.flush().await.unwrap();
    let mut reader = RecordReader::open(&buffer[..]);
    assert!(matches!(reader.read_record().await, Ok(Some(b""))));
    assert!(matches!(reader.read_record().await, Ok(None)));
}

#[tokio::test]
async fn test_simple_record() {
    let mut buffer = Vec::new();
    let mut writer = RecordWriter::open(&mut buffer);
    writer.write_record(b"hello").await.unwrap();
    writer.flush().await.unwrap();
    let mut reader = RecordReader::open(&buffer[..]);
    assert!(matches!(reader.read_record().await, Ok(Some(b"hello"))));
    assert!(matches!(reader.read_record().await, Ok(None)));
}

fn make_long_entry(len: usize) -> Vec<u8> {
    let mut long_entry = Vec::new();
    for i in 0u32..(1u32 + (len as u32) / 4u32) {
        long_entry.extend_from_slice(&i.to_le_bytes());
    }
    long_entry.resize(len, 0u8);
    long_entry
}

#[tokio::test]
async fn test_spans_over_more_than_one_block() {
    let mut buffer = Vec::new();
    let long_entry: Vec<u8> = make_long_entry(80_000);
    let mut writer = RecordWriter::open(&mut buffer);
    writer.write_record(&long_entry).await.unwrap();
    writer.flush().await.unwrap();
    let mut reader = RecordReader::open(&buffer[..]);
    let record_payload = reader.read_record().await.unwrap().unwrap();
    assert_eq!(record_payload, &long_entry[..]);
    assert!(matches!(reader.read_record().await, Ok(None)));
}

#[tokio::test]
async fn test_block_requires_padding() {
    let mut buffer = Vec::new();
    // We'll miss 1 byte to be able to fit our next chunk header in the
    // first block.
    let long_entry = make_long_entry(BLOCK_LEN - HEADER_LEN - HEADER_LEN - 1);
    let mut writer = RecordWriter::open(&mut buffer);
    writer.write_record(&long_entry).await.unwrap();
    writer.write_record(b"hello").await.unwrap();
    writer.flush().await.unwrap();
    let mut reader = RecordReader::open(&buffer[..]);
    assert_eq!(reader.read_record().await.unwrap(), Some(&long_entry[..]));
    assert_eq!(reader.read_record().await.unwrap(), Some(b"hello".as_ref()));
    assert_eq!(reader.read_record().await.unwrap(), None);
}

#[tokio::test]
async fn test_first_chunk_empty() {
    let mut buffer = Vec::new();
    // We'll miss 1 byte to be able to fit our next chunk header in the
    // first block.
    let long_entry = make_long_entry(BLOCK_LEN - HEADER_LEN - HEADER_LEN);
    let mut writer = RecordWriter::open(&mut buffer);
    writer.write_record(&long_entry).await.unwrap();
    writer.write_record(b"hello").await.unwrap();
    writer.flush().await.unwrap();
    let mut reader = RecordReader::open(&buffer[..]);
    assert_eq!(reader.read_record().await.unwrap(), Some(&long_entry[..]));
    assert_eq!(reader.read_record().await.unwrap(), Some(b"hello".as_ref()));
    assert_eq!(reader.read_record().await.unwrap(), None);
}

#[tokio::test]
async fn test_behavior_upon_corruption() {
    let mut buffer = Vec::new();
    let records: Vec<String> = (0..1_000).map(|i| format!("hello{}", i)).collect();

    {
        let mut writer = RecordWriter::open(&mut buffer);
        for record in &records {
            writer.write_record(record.as_bytes()).await.unwrap();
        }
        writer.flush().await.unwrap();
    }
    {
        let mut reader = RecordReader::open(&buffer[..]);
        for record in &records {
            assert_eq!(reader.read_record().await.unwrap(), Some(record.as_bytes()));
        }
        assert_eq!(reader.read_record().await.unwrap(), None);
    }
    buffer[1_000] = 3;
    {
        let mut reader = RecordReader::open(&buffer[..]);
        for record in &records[0..72] {
            // bug at i=73
            assert_eq!(reader.read_record().await.unwrap(), Some(record.as_bytes()));
        }
        assert!(matches!(
            reader.read_record().await,
            Err(ReadRecordError::Corruption)
        ));
    }
}
