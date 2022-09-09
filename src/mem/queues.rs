use std::collections::HashMap;

use crate::error::{AlreadyExists, AppendError, MissingQueue};
use crate::mem::MemQueue;
use crate::position::FileNumber;

#[derive(Default)]
pub struct MemQueues {
    queues: HashMap<String, MemQueue>,
    // Range of records currently being held in all queues.
    lowest_retained_position: FileNumber,
}

impl MemQueues {
    pub fn create_queue(&mut self, queue: &str) -> Result<(), AlreadyExists> {
        if self.queues.contains_key(queue) {
            return Err(AlreadyExists);
        }
        self.queues.insert(queue.to_string(), MemQueue::default());
        Ok(())
    }

    fn get_queue(&self, queue: &str) -> Result<&MemQueue, MissingQueue> {
        // We do not rely on `entry` in order to avoid
        // the allocation.
        self.queues
            .get(queue)
            .ok_or_else(|| MissingQueue(queue.to_string()))
    }

    fn get_queue_mut(&mut self, queue: &str) -> Result<&mut MemQueue, MissingQueue> {
        // We do not rely on `entry` in order to avoid
        // the allocation.
        self.queues
            .get_mut(queue)
            .ok_or_else(|| MissingQueue(queue.to_string()))
    }

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
    ) -> Result<Option<u64>, AppendError> {
        let queue = self.get_queue_mut(queue)?;
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
    ) -> Result<impl Iterator<Item = (u64, &'a [u8])> + 'a, crate::error::MissingQueue> {
        Ok(self.get_queue(queue)?.iter_from(after_position))
    }

    /// Removes records up to the supplied `position`,
    /// including the position itself.
    //
    /// If there are no records `<= position`, the method will
    /// not do anything.
    ///
    /// If one or more files should be removed,
    /// returns the lowest file number that should be retained.
    pub fn truncate(
        &mut self,
        queue: &str,
        position: u64,
    ) -> Result<Option<FileNumber>, crate::error::TruncateError> {
        self.get_queue_mut(queue)?.truncate(position);
        let previous_retained_position = self.lowest_retained_position;
        let mut min_retained_position = previous_retained_position;
        for queue in self.queues.values() {
            let queue_retained_position_opt = queue.first_retained_position();
            if let Some(queue_retained_position) = queue_retained_position_opt {
                if queue_retained_position <= previous_retained_position {
                    return Ok(None);
                }
                min_retained_position = min_retained_position.min(queue_retained_position);
            }
        }
        assert!(min_retained_position >= previous_retained_position);
        if min_retained_position == previous_retained_position {
            return Ok(None);
        }
        self.lowest_retained_position = min_retained_position;
        Ok(Some(min_retained_position))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_queues_already_exists() {
        let mut mem_queues = MemQueues::default();
        mem_queues.create_queue("droopy").unwrap();
        assert!(matches!(
            mem_queues.create_queue("droopy"),
            Err(AlreadyExists)
        ));
    }

    #[test]
    fn test_mem_queues() {
        let mut mem_queues = MemQueues::default();
        mem_queues.create_queue("droopy").unwrap();
        mem_queues.create_queue("fable").unwrap();
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
        mem_queues.create_queue("droopy").unwrap();
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
        assert_eq!(mem_queues.truncate("droopy", 3).unwrap(), None); // TODO fixme
        let droopy: Vec<(u64, &[u8])> = mem_queues.iter_from("droopy", 0).unwrap().collect();
        assert_eq!(&droopy[..], &[(4, &b"!"[..]), (5, &b"payer"[..]),]);
    }

    #[test]
    fn test_mem_queues_skip_yield_error() {
        let mut mem_queues = MemQueues::default();
        mem_queues.create_queue("droopy").unwrap();
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(0), b"hello")
            .is_ok());
        assert!(matches!(
            mem_queues.append_record("droopy", 1.into(), Some(2), b"happy"),
            Err(AppendError::Future)
        ));
        assert!(matches!(
            mem_queues.append_record("droopy", 1.into(), Some(3), b"happy"),
            Err(AppendError::Future)
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
        mem_queues.create_queue("droopy").unwrap();
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(0), b"hello")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(1), b"happy")
            .is_ok());
        assert!(matches!(
            mem_queues.append_record("droopy", 1.into(), Some(0), b"happy"),
            Err(AppendError::Past)
        ));
    }

    #[test]
    fn test_mem_queues_append_nilpotence() {
        let mut mem_queues = MemQueues::default();
        mem_queues.create_queue("droopy").unwrap();
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
        mem_queues.create_queue("droopy").unwrap();
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(5), b"hello")
            .is_ok());
        let droopy: Vec<(u64, &[u8])> = mem_queues.iter_from("droopy", 0).unwrap().collect();
        assert_eq!(droopy, &[(5, &b"hello"[..])]);
    }

    #[test]
    fn test_mem_queues_no_target_position() {
        let mut mem_queues = MemQueues::default();
        mem_queues.create_queue("droopy").unwrap();
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
