use crate::reader::ReadError;
use crate::{Reader, Writer, HEADER_LEN};

#[test]
fn test_empty() {
    let mut buffer = Vec::new();
    Writer::new(&mut buffer).add_record(b"").unwrap();
    assert_eq!(buffer.len(), HEADER_LEN);
    let mut reader = Reader::new(&buffer[..]);
    assert_eq!(reader.read_record().unwrap().unwrap(), b"");
}

#[test]
fn test_simple() {
    let mut buffer = Vec::new();
    Writer::new(&mut buffer).add_record(b"hello").unwrap();
    assert_eq!(buffer.len(), HEADER_LEN + "hello".len());
    let mut reader = Reader::new(&buffer[..]);
    assert_eq!(reader.read_record().unwrap().unwrap(), b"hello");
}

fn make_long_entry(len: usize) -> Vec<u8> {
    let mut long_entry = Vec::new();
    for i in 0u32..(1u32 + (len as u32) / 4u32) {
        long_entry.extend_from_slice(&i.to_le_bytes());
    }
    long_entry.resize(len, 0u8);
    long_entry
}

#[test]
fn test_spans_over_more_than_one_block() {
    let mut buffer = Vec::new();
    let long_entry = make_long_entry(80_000);
    Writer::new(&mut buffer).add_record(&long_entry).unwrap();
    let mut reader = Reader::new(&buffer[..]);
    assert_eq!(reader.read_record().unwrap().unwrap(), &long_entry[..]);
}

#[test]
fn test_block_requires_padding() {
    let mut buffer = Vec::new();
    // We'll miss 1 byte to be able to fit our next chunk header in the
    // first block.
    let long_entry = make_long_entry(32_768 - crate::HEADER_LEN - crate::HEADER_LEN - 1);
    let mut writer = Writer::new(&mut buffer);
    writer.add_record(&long_entry).unwrap();
    writer.add_record(b"hello").unwrap();
    let mut reader = Reader::new(&buffer[..]);
    assert_eq!(reader.read_record().unwrap(), Some(&long_entry[..]));
    assert_eq!(reader.read_record().unwrap(), Some(b"hello".as_ref()));
    assert_eq!(reader.read_record().unwrap(), None);
}

#[test]
fn test_first_chunk_empty() {
    let mut buffer = Vec::new();
    // We'll miss 1 byte to be able to fit our next chunk header in the
    // first block.
    let long_entry = make_long_entry(32_768 - crate::HEADER_LEN - crate::HEADER_LEN);
    let mut writer = Writer::new(&mut buffer);
    writer.add_record(&long_entry).unwrap();
    writer.add_record(b"hello").unwrap();
    let mut reader = Reader::new(&buffer[..]);
    assert_eq!(reader.read_record().unwrap(), Some(&long_entry[..]));
    assert_eq!(reader.read_record().unwrap(), Some(b"hello".as_ref()));
    assert_eq!(reader.read_record().unwrap(), None);
}

#[test]
fn test_behavior_upon_corruption() {
    let mut buffer = Vec::new();
    let records: Vec<String> = (0..1_000).map(|i| format!("hello{}", i)).collect();

    {
        let mut writer = Writer::new(&mut buffer);
        for record in &records {
            writer.add_record(record.as_bytes()).unwrap();
        }
    }
    {
        let mut reader = Reader::new(&buffer[..]);
        for record in &records {
            assert_eq!(reader.read_record().unwrap(), Some(record.as_bytes()));
        }
        assert_eq!(reader.read_record().unwrap(), None);
    }
    buffer[1_000] = 3;
    {
        let mut reader = Reader::new(&buffer[..]);
        for record in &records[0..72] {
            // bug at i=73
            assert_eq!(reader.read_record().unwrap(), Some(record.as_bytes()));
        }
        if let ReadError::Corruption = reader.read_record().unwrap_err() {
        } else {
            panic!("Excepted a `Corruption` error");
        }
    }
}
