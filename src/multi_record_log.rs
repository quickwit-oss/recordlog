use std::io;
use std::path::Path;

use crate::mem::AppendRecordError;
use crate::position::Position;
use crate::record::ReadRecordError;
use crate::rolling::{Record, RecordLogReader};
use crate::{mem, rolling};

pub struct MultiRecordLog {
    record_log_writer: rolling::RecordLogWriter,
    in_mem_queues: mem::MemQueues,
}

impl MultiRecordLog {
    pub async fn open(directory_path: &Path) -> Result<Self, ReadRecordError> {
        let mut record_log_reader = RecordLogReader::open(directory_path).await?;
        let mut in_mem_queues = crate::mem::MemQueues::default();
        loop {
            let file_number = record_log_reader.global_position();
            if let Some(record) = record_log_reader.read_record().await? {
                match record {
                    Record::AppendRecord {
                        position,
                        queue,
                        payload,
                    } => {
                        in_mem_queues
                            .append_record(queue, file_number, Some(position), payload)
                            .map_err(|_| ReadRecordError::Corruption)?;
                    }
                    Record::Truncate { position, queue } => {
                        in_mem_queues.truncate(queue, position);
                    }
                }
            } else {
                break;
            }
        }
        let record_log_writer = record_log_reader.into_writer().await?;
        Ok(MultiRecordLog {
            record_log_writer,
            in_mem_queues,
        })
    }

    pub fn num_files(&self) -> usize {
        self.record_log_writer.num_files()
    }

    /// Appends a record to the log.
    ///
    /// The local_position argument can optionally be passed to enforce nilpotence.
    /// TODO if an io Error is encounterred, the in mem queue and the record log will
    /// be in an inconsistent state.
    pub async fn append_record(
        &mut self,
        queue: &str,
        local_position: Option<Position>,
        payload: &[u8],
    ) -> Result<Option<Position>, AppendRecordError> {
        let file_number = self.record_log_writer.roll_if_needed().await?;
        let append_record_res =
            self.in_mem_queues
                .append_record(queue, file_number, local_position, payload)?;
        let local_position = if let Some(local_position) = append_record_res {
            local_position
        } else {
            return Ok(None);
        };
        let record = Record::AppendRecord {
            position: local_position,
            queue,
            payload,
        };
        self.record_log_writer.write_record(record).await?;
        self.record_log_writer.flush().await?;
        Ok(Some(local_position))
    }

    /// Returns the first record with position greater of equal to position.
    pub fn get_after(&self, queue: &str, position: Position) -> Option<(Position, &[u8])> {
        self.in_mem_queues.get_after(queue, position)
    }

    pub async fn truncate(&mut self, queue: &str, local_position: Position) -> io::Result<()> {
        // self.in_mem_queues.truncate(queue, position)
        // self.record_log_writer
        //     .write_record(Record::Truncate { position, queue })
        //     .await?;
        // if let Some(position) = self.in_mem_queues.truncate(queue, position) {
        //     self.record_log_writer.truncate(position).await?;
        // }
        todo!()
    }
}
