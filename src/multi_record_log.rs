use std::io;
use std::path::Path;

use crate::mem;
use crate::position::LocalPosition;
use crate::record::ReadRecordError;
use crate::rolling;
use crate::rolling::Record;
use crate::rolling::RecordLogReader;

pub struct MultiRecordLog {
    record_log_writer: rolling::RecordLogWriter,
    in_mem_queues: mem::MemQueues,
    // last_position: u64,
}

impl MultiRecordLog {
    pub async fn open(directory_path: &Path) -> Result<Self, ReadRecordError> {
        let mut record_log_reader = RecordLogReader::open(directory_path).await?;
        let mut in_mem_queues = crate::mem::MemQueues::default();
        while let Some(record) = record_log_reader.read_record().await? {
            match record {
                Record::AddRecord {
                    position,
                    queue,
                    payload,
                } => {
                    in_mem_queues
                        .add_record(queue, Some(position), payload)
                        .map_err(|_| ReadRecordError::Corruption)?;
                }
                Record::Truncate { position, queue } => {
                    in_mem_queues.truncate(queue, position);
                }
            }
        }
        let record_log_writer = record_log_reader.into_writer().await?;
        Ok(MultiRecordLog {
            record_log_writer,
            in_mem_queues,
            // last_position,
        })
    }

    pub fn num_files(&self) -> usize {
        self.record_log_writer.num_files()
    }

    // // Returns a new position.
    // fn inc_position(&mut self) -> u64 {
    //     self.last_position += 1;
    //     self.last_position
    // }

    pub async fn append_record(&mut self, queue: &str, payload: &[u8]) -> io::Result<()> {
        // let position = self.inc_position();
        // let record = Record::AddRecord {
        //     position,
        //     queue,
        //     payload,
        // };
        // self.record_log_writer.write_record(record).await?;
        // self.record_log_writer.flush().await?;
        // self.in_mem_queues.add_record(queue, position, payload);
        todo!()
    }

    /// Returns the first record with position greater of equal to position.
    pub fn get_after(&self, queue_id: &str, position: LocalPosition) -> Option<(LocalPosition, &[u8])> {
        self.in_mem_queues.get_after(queue_id, position)
    }

    pub async fn truncate(&mut self, queue: &str, local_position: LocalPosition) -> io::Result<()> {
        // self.record_log_writer
        //     .write_record(Record::Truncate { position, queue })
        //     .await?;
        // if let Some(position) = self.in_mem_queues.truncate(queue, position) {
        //     self.record_log_writer.truncate(position).await?;
        // }
        todo!()
    }
}
