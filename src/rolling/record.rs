use std::convert::TryInto;

use crate::position::Position;
use crate::record::Serializable;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Record<'a> {
    AppendRecord {
        position: Position,
        queue: &'a str,
        payload: &'a [u8],
    },
    Truncate {
        position: Position,
        queue: &'a str,
    },
}

impl<'a> Record<'a> {
    pub fn position(&self) -> Position {
        match self {
            Record::AppendRecord { position, .. } => *position,
            Record::Truncate { position, .. } => *position,
        }
    }
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
                buffer.extend_from_slice(&position.0.to_le_bytes());
                buffer.extend_from_slice(&(queue.len() as u16).to_le_bytes());
                buffer.extend(queue.as_bytes());
                buffer.extend(payload);
            }
            Record::Truncate { queue, position } => {
                buffer.push(1u8);
                buffer.extend(&position.0.to_le_bytes());
                buffer.extend_from_slice(&(queue.len() as u16).to_le_bytes());
                buffer.extend(queue.as_bytes());
            }
        }
    }

    fn deserialize(buffer: &'a [u8]) -> Option<Record<'a>> {
        if buffer.len() < 8 {
            return None;
        }
        let enum_tag = buffer[0];
        let position = Position(u64::from_le_bytes(buffer[1..9].try_into().unwrap()));
        let queue_len = u16::from_le_bytes(buffer[9..11].try_into().unwrap()) as usize;
        let queue = std::str::from_utf8(&buffer[11..][..queue_len]).ok()?;
        match enum_tag {
            0u8 => {
                let payload = &buffer[11 + queue_len..];
                Some(Record::AppendRecord {
                    position,
                    queue: queue,
                    payload,
                })
            }
            1u8 => Some(Record::Truncate {
                position,
                queue: queue,
            }),
            _ => None,
        }
    }
}
