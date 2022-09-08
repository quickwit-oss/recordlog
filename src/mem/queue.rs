use crate::mem::RecordMeta;
use crate::position::{GlobalPosition, LocalPosition};

#[derive(Default)]
pub struct MemQueue {
    // Concatenated records
    concatenated_records: Vec<u8>,
    record_metas: Vec<RecordMeta>,
    last_position_opt: Option<LocalPosition>,
}

#[derive(Debug)]
pub enum AddRecordError {
    Past,
    Future,
}

impl MemQueue {
    pub fn first_retained_position(&self) -> Option<GlobalPosition> {
        Some(self.record_metas.first()?.global_position)
    }

    /// Returns true iff the record was effectively added.
    /// False if the record was added in the previous call.
    ///
    /// AddRecordError if the record is strangely in the past or is too much in the future.
    pub fn add_record(
        &mut self,
        global_position: GlobalPosition,
        target_position_opt: Option<LocalPosition>,
        payload: &[u8],
    ) -> Result<Option<LocalPosition>, AddRecordError> {
        let target_position = target_position_opt.unwrap_or_else(|| {
            if let Some(last_position) = self.last_position_opt {
                last_position.plus_one()
            } else {
                LocalPosition::default()
            }
        });
        let dist = if let Some(last_position) = self.last_position_opt {
            target_position - last_position
        } else {
            1i64
        };
        match dist {
            // This record was already added.
            0 => {
                return Ok(None);
            }
            // Happy path. This record is a new record.
            1 => {
                self.last_position_opt = Some(target_position);
                let record_meta = RecordMeta {
                    start_offset: self.concatenated_records.len(),
                    position: target_position,
                    global_position,
                };
                self.record_metas.push(record_meta);
                self.concatenated_records.extend_from_slice(payload);
                Ok(Some(target_position))
            }
            i64::MIN..=0 => Err(AddRecordError::Past),
            2.. => Err(AddRecordError::Future),
        }
    }

    fn position_to_idx(&self, position: LocalPosition) -> Result<usize, usize> {
        self.record_metas
            .binary_search_by_key(&position, |record_meta| record_meta.position)
    }

    /// Returns the first record with position greater of equal to position.
    pub fn get_after(&self, position: LocalPosition) -> Option<(LocalPosition, &[u8])> {
        let idx = self
            .position_to_idx(position)
            .unwrap_or_else(|first_element_after| first_element_after);
        if idx > self.record_metas.len() {
            return None;
        }
        let record_meta = self.record_metas.get(idx)?;
        let start_offset = record_meta.start_offset;
        if let Some(next_record_meta) = self.record_metas.get(idx + 1) {
            let end_offset = next_record_meta.start_offset;
            Some((
                record_meta.position,
                &self.concatenated_records[start_offset..end_offset],
            ))
        } else {
            Some((
                record_meta.position,
                &self.concatenated_records[start_offset..],
            ))
        }
    }

    /// Removes all records coming before position,
    /// and including the record at "position".
    ///
    /// If a truncation occurs,
    /// this function returns the previous lowest position held.
    pub fn truncate(&mut self, truncate_up_to_pos: LocalPosition) -> Option<GlobalPosition> {
        if self.record_metas.first()?.position > truncate_up_to_pos {
            return None;
        }
        let first_record = self.record_metas.first()?;
        if first_record.position > truncate_up_to_pos {
            return None;
        }
        let previous_lowest_retained_global_position = first_record.global_position;
        let first_idx_to_keep = self
            .position_to_idx(truncate_up_to_pos)
            .map(|idx| idx + 1)
            .unwrap_or_else(|op| op);
        let start_offset_to_keep: usize = self
            .record_metas
            .get(first_idx_to_keep)
            .map(|record_meta| record_meta.start_offset)
            .unwrap_or(self.concatenated_records.len());
        self.record_metas.drain(..first_idx_to_keep);
        for record_meta in &mut self.record_metas {
            record_meta.start_offset -= start_offset_to_keep;
        }
        self.concatenated_records.drain(..start_offset_to_keep);
        Some(previous_lowest_retained_global_position)
    }
}
