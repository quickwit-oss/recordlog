use std::convert::TryInto;

use crate::record::Serializable;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Record<'a> {
    CreateQueue {
        queue: &'a str,
    },
    AppendRecord {
        position: u64,
        queue: &'a str,
        payload: &'a [u8],
    },
    Truncate {
        position: u64,
        queue: &'a str,
    },
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
                buffer.push(0u8);
                buffer.extend_from_slice(&position.to_le_bytes());
                buffer.extend_from_slice(&(queue.len() as u16).to_le_bytes());
                buffer.extend_from_slice(queue.as_bytes());
                buffer.extend(payload);
            }
            Record::Truncate { queue, position } => {
                buffer.push(1u8);
                buffer.extend(&position.to_le_bytes());
                buffer.extend_from_slice(&(queue.len() as u16).to_le_bytes());
                buffer.extend(queue.as_bytes());
            }
            Record::CreateQueue { queue } => {
                buffer.push(2u8);
                buffer.extend_from_slice(queue.as_bytes());
            }
        }
    }

    fn deserialize(buffer: &'a [u8]) -> Option<Record<'a>> {
        let enum_tag = buffer[0];
        match enum_tag {
            0u8 => {
                if buffer.len() < 8 {
                    return None;
                }
                let position = u64::from_le_bytes(buffer[1..9].try_into().unwrap());
                let queue_len = u16::from_le_bytes(buffer[9..11].try_into().unwrap()) as usize;
                let queue = std::str::from_utf8(&buffer[11..][..queue_len]).ok()?;
                let payload = &buffer[11 + queue_len..];
                Some(Record::AppendRecord {
                    position,
                    queue,
                    payload,
                })
            }
            1u8 => {
                if buffer.len() < 8 {
                    return None;
                }
                let position = u64::from_le_bytes(buffer[1..9].try_into().unwrap());
                let queue_len = u16::from_le_bytes(buffer[9..11].try_into().unwrap()) as usize;
                let queue = std::str::from_utf8(&buffer[11..][..queue_len]).ok()?;
                Some(Record::Truncate { position, queue })
            }
            2u8 => Some(Record::CreateQueue {
                queue: std::str::from_utf8(&buffer[1..]).ok()?,
            }),
            _ => None,
        }
    }
}
