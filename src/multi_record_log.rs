use std::path::Path;

use crate::error::{AppendError, CreateQueueError, MissingQueue, TruncateError};
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
                    Record::CreateQueue { queue } => {
                        in_mem_queues.create_queue(queue);
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

    #[cfg(test)]
    pub fn num_files(&self) -> usize {
        self.record_log_writer.num_files()
    }

    pub async fn create_queue(&mut self, queue: &str) -> Result<(), CreateQueueError> {
        self.record_log_writer.roll_if_needed().await?;
        self.in_mem_queues.create_queue(queue)?;
        let record = Record::CreateQueue { queue };
        self.record_log_writer.write_record(record).await?;
        self.record_log_writer.flush().await?;
        Ok(())
    }

    /// Appends a record to the log.
    ///
    /// The local_position argument can optionally be passed to enforce nilpotence.
    /// TODO if an io Error is encounterred, the in mem queue and the record log will
    /// be in an inconsistent state.
    pub async fn append_record(
        &mut self,
        queue: &str,
        position: Option<u64>,
        payload: &[u8],
    ) -> Result<Option<u64>, AppendError> {
        if !self.in_mem_queues.contains_queue(queue) {
            return Err(AppendError::MissingQueue(queue.to_string()));
        }
        let file_number = self.record_log_writer.roll_if_needed().await?;
        let append_record_res =
            self.in_mem_queues
                .append_record(queue, file_number, position, payload)?;
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
    pub fn iter_from<'a>(
        &'a self,
        queue: &str,
        start_from: u64,
    ) -> Result<impl Iterator<Item = (u64, &'a [u8])> + 'a, MissingQueue> {
        self.in_mem_queues.iter_from(queue, start_from)
    }

    async fn log_positions(&mut self) -> Result<(), TruncateError> {
        todo!();
    }

    pub async fn truncate(&mut self, queue: &str, position: u64) -> Result<(), TruncateError> {
        let file_number_opt = self.in_mem_queues.truncate(queue, position)?;
        self.record_log_writer
            .write_record(Record::Truncate { position, queue })
            .await?;
        if let Some(file_number) = file_number_opt {
            self.log_positions().await?;
            self.record_log_writer.truncate(file_number).await?;
        }
        Ok(())
    }
}
