use std::convert::{TryFrom, TryInto};

use crate::record::Serializable;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Record<'a> {
    /// Adds a new record to a specific queue.
    AppendRecord {
        position: u64,
        queue: &'a str,
        payload: &'a [u8],
    },
    /// Records the truncation of a specific queue.
    Truncate { position: u64, queue: &'a str },
    /// Records the next position of a given queue.
    /// If the queue does not exists, creates it.
    ///
    /// `position` is the position of the NEXT message to be appended.
    Touch { position: u64, queue: &'a str },
}

#[repr(u8)]
#[derive(Clone, Copy, Debug)]
enum RecordType {
    AppendRecord = 0,
    Truncate = 1,
    Touch = 2,
}

impl TryFrom<u8> for RecordType {
    type Error = ();

    fn try_from(code: u8) -> Result<Self, Self::Error> {
        match code {
            0 => Ok(RecordType::AppendRecord),
            1 => Ok(RecordType::Truncate),
            2 => Ok(RecordType::Touch),
            _ => Err(()),
        }
    }
}

fn serialize(
    record_type: RecordType,
    position: u64,
    queue: &str,
    payload: &[u8],
    buffer: &mut Vec<u8>,
) {
    assert!(queue.len() <= u16::MAX as usize);
    buffer.push(record_type as u8);
    buffer.extend_from_slice(&position.to_le_bytes());
    buffer.extend_from_slice(&(queue.len() as u16).to_le_bytes());
    buffer.extend_from_slice(queue.as_bytes());
    buffer.extend(payload);
}

impl<'a> Serializable<'a> for Record<'a> {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        buffer.clear();
        match *self {
            Record::AppendRecord {
                position,
                queue,
                payload,
            } => {
                serialize(RecordType::AppendRecord, position, queue, payload, buffer);
            }
            Record::Truncate { queue, position } => {
                serialize(RecordType::Truncate, position, queue, &[], buffer);
            }
            Record::Touch { queue, position } => {
                serialize(RecordType::Touch, position, queue, &[], buffer);
            }
        }
    }

    fn deserialize(buffer: &'a [u8]) -> Option<Record<'a>> {
        let enum_tag = RecordType::try_from(buffer[0]).ok()?;
        if buffer.len() < 8 {
            return None;
        }
        let position = u64::from_le_bytes(buffer[1..9].try_into().unwrap());
        let queue_len = u16::from_le_bytes(buffer[9..11].try_into().unwrap()) as usize;
        let queue = std::str::from_utf8(&buffer[11..][..queue_len]).ok()?;
        let payload = &buffer[11 + queue_len..];
        match enum_tag {
            RecordType::AppendRecord => Some(Record::AppendRecord {
                position,
                queue,
                payload,
            }),
            RecordType::Truncate => Some(Record::Truncate { position, queue }),
            RecordType::Touch => Some(Record::Touch { position, queue }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use crate::rolling::record::RecordType;

    #[test]
    fn test_record_type_serialize() {
        let mut num_record_types = 0;
        for code in 0u8..=255u8 {
            if let Ok(record_type) = RecordType::try_from(code) {
                assert_eq!(record_type as u8, code);
                num_record_types += 1;
            }
        }
        assert_eq!(num_record_types, 3);
    }
}
