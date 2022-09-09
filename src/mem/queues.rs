use std::collections::HashMap;

use crate::mem::{AppendRecordError, MemQueue};
use crate::position::FileNumber;

#[derive(Default)]
pub struct MemQueues {
    queues: HashMap<String, MemQueue>,
    // Range of records currently being held in all queues.
    lowest_retained_position: FileNumber,
}

impl MemQueues {
    fn get_or_create_queue(&mut self, queue: &str) -> &mut MemQueue {
        // We do not rely on `entry` in order to avoid
        // the allocation.
        if !self.queues.contains_key(queue) {
            self.queues.insert(queue.to_string(), Default::default());
        }
        self.queues.get_mut(queue).unwrap()
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
    /// A client may call `append_record` a second time with the same local_position to ensure that
    /// a record has been written.
    ///
    /// If no local_position is supplied, the call should always be successful.
    pub(crate) fn append_record(
        &mut self,
        queue: &str,
        global_position: FileNumber,
        local_position: Option<u64>,
        record: &[u8],
    ) -> Result<Option<u64>, AppendRecordError> {
        let queue = self.get_or_create_queue(queue);
        let position_opt = queue.append_record(global_position, local_position, record)?;
        if let Some(local_position) = position_opt {
            // We only increment the global position if the record is effectively written.
            Ok(Some(local_position))
        } else {
            Ok(None)
        }
    }

    /// Returns the first record with position greater of equal to position.
    pub(crate) fn iter_from<'a>(
        &'a self,
        queue: &str,
        after_position: u64,
    ) -> Option<impl Iterator<Item = (u64, &'a [u8])> + 'a> {
        Some(self.queues.get(queue)?.iter_from(after_position))
    }

    /// Removes records up to the supplied `position`,
    /// including the position itself.
    ///
    /// If the queue `queue` does not exist, it
    /// will be created, and the first record appended will be `position + 1`.
    ///
    /// If there are no records `<= position`, the method will
    /// not do anything.
    ///
    /// Returns the lowest file number that should be retained.
    pub fn truncate(&mut self, queue: &str, position: u64) -> Option<FileNumber> {
        self.get_or_create_queue(queue).truncate(position);
        let previous_retained_position = self.lowest_retained_position;
        let mut min_retained_position = previous_retained_position;
        for queue in self.queues.values() {
            let queue_retained_position_opt = queue.first_retained_position();
            if let Some(queue_retained_position) = queue_retained_position_opt {
                if queue_retained_position <= previous_retained_position {
                    return None;
                }
                min_retained_position = min_retained_position.min(queue_retained_position);
            }
        }
        assert!(min_retained_position >= previous_retained_position);
        if min_retained_position == previous_retained_position {
            return None;
        }
        self.lowest_retained_position = min_retained_position;
        Some(min_retained_position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_queues() {
        let mut mem_queues = MemQueues::default();
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(0), b"hello")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(1), b"happy")
            .is_ok());
        assert!(mem_queues
            .append_record("fable", 1.into(), Some(0), b"maitre")
            .is_ok());
        assert!(mem_queues
            .append_record("fable", 1.into(), Some(1), b"corbeau")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(2), b"tax")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(3), b"payer")
            .is_ok());
        assert_eq!(
            mem_queues.iter_from("droopy", 0).unwrap().next(),
            Some((0, &b"hello"[..]))
        );
        let droopy: Vec<(u64, &[u8])> = mem_queues.iter_from("droopy", 1).unwrap().collect();
        assert_eq!(
            &droopy,
            &[(1, &b"happy"[..]), (2, &b"tax"[..]), (3, &b"payer"[..])],
        );
        let fable: Vec<(u64, &[u8])> = mem_queues.iter_from("fable", 1).unwrap().collect();
        assert_eq!(&fable, &[(1, &b"corbeau"[..])]);
    }

    #[test]
    fn test_mem_queues_truncate() {
        let mut mem_queues = MemQueues::default();
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(0), b"hello")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(1), b"happy")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(2), b"tax")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(3), b"payer")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(4), b"!")
            .is_ok());
        mem_queues
            .append_record("droopy", 1.into(), Some(5), b"payer")
            .unwrap();
        assert_eq!(mem_queues.truncate("droopy", 3), None); // TODO fixme
        let droopy: Vec<(u64, &[u8])> = mem_queues.iter_from("droopy", 0).unwrap().collect();
        assert_eq!(&droopy[..], &[(4, &b"!"[..]), (5, &b"payer"[..]),]);
    }

    #[test]
    fn test_mem_queues_skip_yield_error() {
        let mut mem_queues = MemQueues::default();
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(0), b"hello")
            .is_ok());
        assert!(matches!(
            mem_queues.append_record("droopy", 1.into(), Some(2), b"happy"),
            Err(AppendRecordError::Future)
        ));
        assert!(matches!(
            mem_queues.append_record("droopy", 1.into(), Some(3), b"happy"),
            Err(AppendRecordError::Future)
        ));
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(1), b"happy")
            .is_ok());
        let droopy: Vec<(u64, &[u8])> = mem_queues.iter_from("droopy", 0).unwrap().collect();
        assert_eq!(&droopy[..], &[(0, &b"hello"[..]), (1, &b"happy"[..])]);
    }

    #[test]
    fn test_mem_queues_append_in_the_past_yield_error() {
        let mut mem_queues = MemQueues::default();
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(0), b"hello")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(1), b"happy")
            .is_ok());
        assert!(matches!(
            mem_queues.append_record("droopy", 1.into(), Some(0), b"happy"),
            Err(AppendRecordError::Past)
        ));
    }

    #[test]
    fn test_mem_queues_append_nilpotence() {
        let mut mem_queues = MemQueues::default();
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(0), b"hello")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(0), b"different")
            .is_ok()); //< the string is different
                       // Right now there are no checks, on the string being equal.
        let droopy: Vec<(u64, &[u8])> = mem_queues.iter_from("droopy", 0).unwrap().collect();
        assert_eq!(&droopy, &[(0, &b"hello"[..])]);
    }

    #[test]
    fn test_mem_queues_non_zero_first_el() {
        let mut mem_queues = MemQueues::default();
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(5), b"hello")
            .is_ok());
        let droopy: Vec<(u64, &[u8])> = mem_queues.iter_from("droopy", 0).unwrap().collect();
        assert_eq!(droopy, &[(5, &b"hello"[..])]);
    }

    #[test]
    fn test_mem_queues_no_target_position() {
        let mut mem_queues = MemQueues::default();
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(5), b"hello")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", 1.into(), None, b"happy")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", 1.into(), None, b"tax")
            .is_ok());
        let droopy: Vec<(u64, &[u8])> = mem_queues.iter_from("droopy", 5).unwrap().collect();
        assert_eq!(
            &droopy[..],
            &[(5, &b"hello"[..]), (6, &b"happy"[..]), (7, &b"tax"[..])]
        );
    }
}
