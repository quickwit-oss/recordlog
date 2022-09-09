use std::ops::{Bound, RangeBounds};

use crate::error::AppendError;
use crate::position::FileNumber;

#[derive(Clone, Copy)]
struct RecordMeta {
    start_offset: usize,
    file_number: FileNumber,
}

#[derive(Default)]
pub struct MemQueue {
    // Concatenated records
    concatenated_records: Vec<u8>,
    start_position: u64,
    record_metas: Vec<RecordMeta>,
}

impl MemQueue {
    pub fn with_next_position(next_position: u64) -> Self {
        MemQueue {
            concatenated_records: Vec::new(),
            start_position: next_position,
            record_metas: Vec::new(),
        }
    }
    pub fn first_retained_position(&self) -> Option<FileNumber> {
        Some(self.record_metas.first()?.file_number)
    }

    pub fn is_empty(&self) -> bool {
        self.record_metas.is_empty()
    }

    /// Returns what should be the next position.
    pub fn next_position(&self) -> u64 {
        self.start_position + self.record_metas.len() as u64
    }

    /// Returns true iff the record was effectively added.
    /// False if the record was added in the previous call.
    ///
    /// AppendError if the record is strangely in the past or is too much in the future.
    pub fn append_record(
        &mut self,
        file_number: FileNumber,
        target_position_opt: Option<u64>,
        payload: &[u8],
    ) -> Result<Option<u64>, AppendError> {
        let target_position = target_position_opt.unwrap_or_else(|| self.next_position());
        if self.start_position == u64::default() && self.record_metas.is_empty() {
            self.start_position = target_position;
        }
        let dist = (self.next_position() as i64) - (target_position as i64);
        match dist {
            i64::MIN..=-1 => Err(AppendError::Future),
            // Happy path. This record is a new record.
            0 => {
                let record_meta = RecordMeta {
                    start_offset: self.concatenated_records.len(),
                    file_number,
                };
                self.record_metas.push(record_meta);
                self.concatenated_records.extend_from_slice(payload);
                Ok(Some(target_position))
            }
            // This record was already added.
            1 => Ok(None),
            2.. => Err(AppendError::Past),
        }
    }

    fn position_to_idx(&self, position: u64) -> Option<usize> {
        if self.start_position > position {
            return Some(0);
        }
        let idx = (position - self.start_position) as usize;
        if idx > self.record_metas.len() {
            return None;
        }
        Some(idx as usize)
    }

    pub fn range<'a, R>(&'a self, range: R) -> impl Iterator<Item = (u64, &'a [u8])> + 'a
    where R: RangeBounds<u64> + 'static {
        let start_idx: usize = match range.start_bound() {
            Bound::Included(&start_from) => self
                .position_to_idx(start_from)
                .unwrap_or(self.record_metas.len()),
            Bound::Excluded(&start_from) => {
                self.position_to_idx(start_from)
                    .unwrap_or(self.record_metas.len())
                    + 1
            }
            Bound::Unbounded => 0,
        };
        (start_idx..self.record_metas.len())
            .take_while(move |&idx| {
                let position = self.start_position + idx as u64;
                range.contains(&position)
            })
            .map(move |idx| {
                let position = self.start_position + idx as u64;
                let start_offset = self.record_metas[idx].start_offset;
                if let Some(next_record_meta) = self.record_metas.get(idx + 1) {
                    let end_offset = next_record_meta.start_offset;
                    (
                        position,
                        &self.concatenated_records[start_offset..end_offset],
                    )
                } else {
                    (position, &self.concatenated_records[start_offset..])
                }
            })
    }

    /// Removes all records coming before position,
    /// and including the record at "position".
    pub fn truncate(&mut self, truncate_up_to_pos: u64) {
        if self.start_position > truncate_up_to_pos {
            return;
        }
        let first_record_to_keep =
            if let Some(first_record_to_keep) = self.position_to_idx(truncate_up_to_pos + 1) {
                first_record_to_keep
            } else {
                // clear the queue.
                self.start_position = self.start_position + self.record_metas.len() as u64;
                self.concatenated_records.clear();
                self.record_metas.clear();
                return;
            };
        let start_offset_to_keep: usize = self.record_metas[first_record_to_keep].start_offset;
        self.record_metas.drain(..first_record_to_keep);
        for record_meta in &mut self.record_metas {
            record_meta.start_offset -= start_offset_to_keep;
        }
        self.concatenated_records.drain(..start_offset_to_keep);
        self.start_position = self.start_position + first_record_to_keep as u64;
    }
}
