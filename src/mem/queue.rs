use super::AppendRecordError;
use crate::mem::RecordMeta;
use crate::position::{FileNumber, Position};

#[derive(Default)]
pub struct MemQueue {
    // Concatenated records
    concatenated_records: Vec<u8>,
    start_position: Position,
    record_metas: Vec<RecordMeta>,
}
impl MemQueue {
    pub fn first_retained_position(&self) -> Option<FileNumber> {
        Some(self.record_metas.first()?.file_number)
    }

    /// Returns what should be the next position.
    fn target_position(&self) -> Position {
        self.start_position + self.record_metas.len() as u64
    }

    /// Returns true iff the record was effectively added.
    /// False if the record was added in the previous call.
    ///
    /// AppendRecordError if the record is strangely in the past or is too much in the future.
    pub fn append_record(
        &mut self,
        file_number: FileNumber,
        target_position_opt: Option<Position>,
        payload: &[u8],
    ) -> Result<Option<Position>, AppendRecordError> {
        let target_position = target_position_opt.unwrap_or_else(|| self.target_position());
        if self.start_position == Position::default() && self.record_metas.is_empty() {
            self.start_position = target_position;
        }
        let dist = self.target_position() - target_position;
        match dist {
            i64::MIN..=-1 => Err(AppendRecordError::Future),
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
            2.. => Err(AppendRecordError::Past),
        }
    }

    fn position_to_idx(&self, position: Position) -> Option<usize> {
        if self.start_position > position {
            return Some(0);
        }
        let idx = (position - self.start_position) as usize;
        if idx > self.record_metas.len() {
            return None;
        }
        Some(idx as usize)
    }

    pub fn iter_from<'a>(
        &'a self,
        start_from: Position,
    ) -> impl Iterator<Item = (Position, &'a [u8])> + 'a {
        let start_idx = self
            .position_to_idx(start_from)
            .unwrap_or(self.record_metas.len());
        (start_idx..self.record_metas.len()).map(move |idx| {
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

    /// Returns the first record with position greater of equal to position.
    pub fn get_after(&self, start_from: Position) -> Option<(Position, &[u8])> {
        self.iter_from(start_from).next()
    }

    /// Removes all records coming before position,
    /// and including the record at "position".
    pub fn truncate(&mut self, truncate_up_to_pos: Position) {
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
