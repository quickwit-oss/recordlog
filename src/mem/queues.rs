use std::collections::HashMap;
use std::ops::Range;

use crate::mem::queue::AddRecordError;
use crate::mem::MemQueue;
use crate::position::{GlobalPosition, LocalPosition};

#[derive(Default)]
pub struct MemQueues {
    queues: HashMap<String, MemQueue>,
    // Range of records currently being held in all queues.
    retained_range: Range<GlobalPosition>,
}

impl MemQueues {
    fn get_or_create_queue(&mut self, queue_id: &str) -> &mut MemQueue {
        // We do not rely on `entry` in order to avoid
        // the allocation.
        if !self.queues.contains_key(queue_id) {
            self.queues.insert(queue_id.to_string(), Default::default());
        }
        self.queues.get_mut(queue_id).unwrap()
    }
}

impl MemQueues {
    /// Appends a new record.
    ///
    /// If a new record is successfully added, its global position and its local position
    /// are returned.
    ///
    /// When supplied, a new record is really added, iff the last known record position
    /// is one below the past local_position.
    ///
    /// It is useful to allow for nilpotence.
    /// A client may call add_record a second time with the same local_position to ensure that a
    /// record has been written.
    ///
    /// If no local_position is supplied, the call should always be successful.
    pub(crate) fn add_record(
        &mut self,
        queue_id: &str,
        local_position: Option<LocalPosition>,
        record: &[u8],
    ) -> Result<Option<(GlobalPosition, LocalPosition)>, AddRecordError> {
        let candidate_position = self.retained_range.end;
        let queue = self.get_or_create_queue(queue_id);
        let position_opt = queue.add_record(candidate_position, local_position, record)?;
        if let Some(local_position) = position_opt {
            // We only increment the global position if the record is effectively written.
            self.retained_range.end.inc();
            Ok(Some((candidate_position, local_position)))
        } else {
            Ok(None)
        }
    }

    /// Returns the first record with position greater of equal to position.
    pub(crate) fn get_after(
        &self,
        queue_id: &str,
        after_position: LocalPosition,
    ) -> Option<(LocalPosition, &[u8])> {
        let (position, payload) = self.queues.get(queue_id)?.get_after(after_position)?;
        assert!(position >= after_position);
        Some((position, payload))
    }

    /// Removes records up to the supplied `position`,
    /// including the position itself.
    ///
    /// If the queue `queue_id` does not exist, it
    /// will be created, and the first record appended will be `position + 1`.
    ///
    /// If there are no records `<= position`, the method will
    /// not do anything.
    ///
    /// Returns a position up to which including it is safe to truncate files as well.
    pub fn truncate(&mut self, queue_id: &str, position: LocalPosition) -> Option<GlobalPosition> {
        let previous_lowest_retained_position: GlobalPosition =
            self.get_or_create_queue(queue_id).truncate(position)?;
        // Optimization here. This queue was truncated yes, but it was not the reason
        // why we are retaining the oldest record in the queue.
        if self.retained_range.start != previous_lowest_retained_position {
            return None;
        }
        if let Some(new_lowest) = self
            .queues
            .values_mut()
            .flat_map(|queue| queue.first_retained_position())
            .min()
        {
            self.retained_range.start = new_lowest;
        } else {
            self.retained_range.start = self.retained_range.end;
        }
        self.retained_range.start.previous()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_queues() {
        let mut mem_queues = MemQueues::default();
        assert!(mem_queues
            .add_record("droopy", Some(LocalPosition(0)), b"hello")
            .is_ok());
        assert!(mem_queues
            .add_record("droopy", Some(LocalPosition(1)), b"happy")
            .is_ok());
        assert!(mem_queues
            .add_record("fable", Some(LocalPosition(0)), b"maitre")
            .is_ok());
        assert!(mem_queues
            .add_record("fable", Some(LocalPosition(1)), b"corbeau")
            .is_ok());
        assert!(mem_queues
            .add_record("droopy", Some(LocalPosition(2)), b"tax")
            .is_ok());
        assert!(mem_queues
            .add_record("droopy", Some(LocalPosition(3)), b"payer")
            .is_ok());
        assert_eq!(
            mem_queues.get_after("droopy", LocalPosition(0)),
            Some((LocalPosition(0), &b"hello"[..]))
        );
        assert_eq!(
            mem_queues.get_after("droopy", LocalPosition(1)),
            Some((LocalPosition(1), &b"happy"[..]))
        );
        assert_eq!(
            mem_queues.get_after("droopy", LocalPosition(2)),
            Some((LocalPosition(2), &b"tax"[..]))
        );
        assert_eq!(
            mem_queues.get_after("droopy", LocalPosition(3)),
            Some((LocalPosition(3), &b"payer"[..]))
        );
        assert_eq!(mem_queues.get_after("droopy", LocalPosition(4)), None);
        assert_eq!(
            mem_queues.get_after("fable", LocalPosition(0)),
            Some((LocalPosition(0), &b"maitre"[..]))
        );
        assert_eq!(
            mem_queues.get_after("fable", LocalPosition(1)),
            Some((LocalPosition(1), &b"corbeau"[..]))
        );
        assert_eq!(mem_queues.get_after("fable", LocalPosition(2)), None);
        assert_eq!(mem_queues.get_after("fable", LocalPosition(3)), None);
    }

    #[test]
    fn test_mem_queues_truncate() {
        let mut mem_queues = MemQueues::default();
        assert!(mem_queues
            .add_record("droopy", Some(LocalPosition(0)), b"hello")
            .is_ok());
        assert!(mem_queues
            .add_record("droopy", Some(LocalPosition(1)), b"happy")
            .is_ok());
        assert!(mem_queues
            .add_record("droopy", Some(LocalPosition(2)), b"tax")
            .is_ok());
        assert!(mem_queues
            .add_record("droopy", Some(LocalPosition(3)), b"payer")
            .is_ok());
        assert!(mem_queues
            .add_record("droopy", Some(LocalPosition(4)), b"!")
            .is_ok());
        mem_queues
            .add_record("droopy", Some(LocalPosition(5)), b"payer")
            .unwrap();
        mem_queues.truncate("droopy", LocalPosition(3)).unwrap();
        assert_eq!(
            mem_queues.get_after("droopy", LocalPosition(0)),
            Some((LocalPosition(4), &b"!"[..]))
        );
    }

    #[test]
    fn test_mem_queues_skip_yield_error() {
        let mut mem_queues = MemQueues::default();
        assert!(mem_queues
            .add_record("droopy", Some(LocalPosition(0)), b"hello")
            .is_ok());
        assert!(matches!(
            mem_queues.add_record("droopy", Some(LocalPosition(2)), b"happy"),
            Err(AddRecordError::Future)
        ));
        assert!(matches!(
            mem_queues.add_record("droopy", Some(LocalPosition(3)), b"happy"),
            Err(AddRecordError::Future)
        ));
        assert!(mem_queues
            .add_record("droopy", Some(LocalPosition(1)), b"happy")
            .is_ok());
        assert_eq!(
            mem_queues.get_after("droopy", LocalPosition(0)),
            Some((LocalPosition(0), &b"hello"[..]))
        );
        assert_eq!(
            mem_queues.get_after("droopy", LocalPosition(1)),
            Some((LocalPosition(1), &b"happy"[..]))
        );
        assert_eq!(mem_queues.get_after("droopy", LocalPosition(2)), None);
    }

    #[test]
    fn test_mem_queues_append_in_the_past_yield_error() {
        let mut mem_queues = MemQueues::default();
        assert!(mem_queues
            .add_record("droopy", Some(LocalPosition(0)), b"hello")
            .is_ok());
        assert!(mem_queues
            .add_record("droopy", Some(LocalPosition(1)), b"happy")
            .is_ok());
        assert!(matches!(
            mem_queues.add_record("droopy", Some(LocalPosition(0)), b"happy"),
            Err(AddRecordError::Past)
        ));
    }

    #[test]
    fn test_mem_queues_append_nilpotence() {
        let mut mem_queues = MemQueues::default();
        assert!(mem_queues
            .add_record("droopy", Some(LocalPosition(0)), b"hello")
            .is_ok());
        assert!(mem_queues
            .add_record("droopy", Some(LocalPosition(0)), b"different")
            .is_ok()); //< the string is different
                       // Right now there are no checks, on the string being equal.
        assert_eq!(
            mem_queues.get_after("droopy", LocalPosition(0)),
            Some((LocalPosition(0), &b"hello"[..]))
        );
        assert_eq!(mem_queues.get_after("droopy", LocalPosition(1)), None);
    }

    #[test]
    fn test_mem_queues_non_zero_first_el() {
        let mut mem_queues = MemQueues::default();
        assert!(mem_queues
            .add_record("droopy", Some(LocalPosition(5)), b"hello")
            .is_ok());
        assert_eq!(
            mem_queues.get_after("droopy", LocalPosition(0)),
            Some((LocalPosition(5), &b"hello"[..]))
        );
    }

    #[test]
    fn test_mem_queues_no_target_position() {
        let mut mem_queues = MemQueues::default();
        assert!(mem_queues
            .add_record("droopy", Some(LocalPosition(5)), b"hello")
            .is_ok());
        assert!(mem_queues.add_record("droopy", None, b"happy").is_ok());
        assert!(mem_queues.add_record("droopy", None, b"tax").is_ok());
        assert_eq!(
            mem_queues.get_after("droopy", LocalPosition(5)),
            Some((LocalPosition(5), &b"hello"[..]))
        );
        assert_eq!(
            mem_queues.get_after("droopy", LocalPosition(6)),
            Some((LocalPosition(6), &b"happy"[..]))
        );
        assert_eq!(
            mem_queues.get_after("droopy", LocalPosition(7)),
            Some((LocalPosition(7), &b"tax"[..]))
        );
    }
}
