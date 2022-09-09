use std::collections::HashMap;
use std::ops::{RangeBounds, RangeTo};

use crate::error::{AlreadyExists, AppendError, MissingQueue, TouchError};
use crate::mem::MemQueue;
use crate::position::FileNumber;

#[derive(Default)]
pub struct MemQueues {
    queues: HashMap<String, MemQueue>,
    // Range of records currently being held in all queues.
    lowest_retained_file_number: Option<FileNumber>,
}

impl MemQueues {
    pub fn create_queue(&mut self, queue: &str) -> Result<(), AlreadyExists> {
        if self.queues.contains_key(queue) {
            return Err(AlreadyExists);
        }
        self.queues.insert(queue.to_string(), MemQueue::default());
        Ok(())
    }

    pub fn empty_queue_positions<'a>(&'a self) -> impl Iterator<Item = (&'a str, u64)> + 'a {
        self.queues.iter().filter_map(|(queue, mem_queue)| {
            if mem_queue.is_empty() {
                Some((queue.as_str(), mem_queue.next_position()))
            } else {
                None
            }
        })
    }

    fn get_queue(&self, queue: &str) -> Result<&MemQueue, MissingQueue> {
        // We do not rely on `entry` in order to avoid
        // the allocation.
        self.queues
            .get(queue)
            .ok_or_else(|| MissingQueue(queue.to_string()))
    }

    pub fn contains_queue(&mut self, queue: &str) -> bool {
        self.queues.contains_key(queue)
    }

    fn get_or_create_queue_mut(&mut self, queue: &str) -> &mut MemQueue {
        // We do not rely on `entry` in order to avoid
        // the allocation.
        if !self.queues.contains_key(queue) {
            self.queues.insert(queue.to_string(), MemQueue::default());
        }
        self.queues.get_mut(queue).unwrap()
    }

    pub fn touch(&mut self, queue: &str, start_position: u64) -> Result<(), TouchError> {
        if self.queues.contains_key(queue) {
            let queue = self.get_queue(queue).unwrap();
            if queue.next_position() == start_position {
                Ok(())
            } else {
                Err(TouchError)
            }
        } else {
            self.queues.insert(
                queue.to_string(),
                MemQueue::with_next_position(start_position),
            );
            Ok(())
        }
    }

    /// Appends a new record.
    ///
    /// If a new record is successfully added, its position
    /// is returned.
    ///
    /// If a position is supplied, a new record is really added, iff
    /// the last known record position is lower than the supplied position.
    ///
    /// If the last position is equal to the supplied position,
    /// no new record is added, and no error is returned.
    /// We just return `Ok(None)`.
    ///
    /// If no local_position is supplied, the new record position will be
    /// 1 more than the last position.
    pub(crate) fn append_record(
        &mut self,
        queue: &str,
        file_number: FileNumber,
        position_opt: Option<u64>,
        record: &[u8],
    ) -> Result<Option<u64>, AppendError> {
        let res =
            self.get_or_create_queue_mut(queue)
                .append_record(file_number, position_opt, record)?;
        if self.lowest_retained_file_number.is_none() {
            self.lowest_retained_file_number = Some(file_number);
        }
        Ok(res)
    }

    /// Returns the first record with position greater of equal to position.
    pub(crate) fn range<'a, R>(
        &'a self,
        queue: &str,
        position_range: R,
    ) -> Result<impl Iterator<Item = (u64, &'a [u8])> + 'a, crate::error::MissingQueue>
    where
        R: RangeBounds<u64> + 'static,
    {
        Ok(self.get_queue(queue)?.range(position_range))
    }

    /// Removes records up to the supplied `position`,
    /// including the position itself.
    //
    /// If there are no records `<= position`, the method will
    /// not do anything.
    ///
    /// If one or more files should be removed,
    /// returns the range of the files that should be removed
    pub fn truncate(&mut self, queue: &str, position: u64) -> Truncation {
        let previous_lowest_retained_file_number =
            if let Some(file_number) = self.lowest_retained_file_number {
                file_number
            } else {
                // There are no file to remove anyway.
                return Truncation::NoTruncation;
            };
        self.get_or_create_queue_mut(queue).truncate(position);
        let mut min_retained_file_number_opt: Option<FileNumber> = None;
        for queue in self.queues.values() {
            let queue_retained_file_opt = queue.first_retained_position();
            if let Some(queue_retained_file) = queue_retained_file_opt {
                assert!(queue_retained_file >= previous_lowest_retained_file_number);
                if queue_retained_file == previous_lowest_retained_file_number {
                    return Truncation::NoTruncation;
                }
                min_retained_file_number_opt = Some(
                    min_retained_file_number_opt
                        .unwrap_or(queue_retained_file)
                        .min(queue_retained_file),
                );
            }
        }
        self.lowest_retained_file_number = min_retained_file_number_opt;
        if let Some(min_retained_file_number) = min_retained_file_number_opt {
            assert!(min_retained_file_number >= previous_lowest_retained_file_number);
            if min_retained_file_number == previous_lowest_retained_file_number {
                // No file to remove.
                return Truncation::NoTruncation;
            }
            Truncation::RemoveFiles(..min_retained_file_number)
        } else {
            // No file is retained.
            // Let's remove everything!
            Truncation::RemoveAllFiles
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Truncation {
    NoTruncation,
    RemoveFiles(RangeTo<FileNumber>),
    RemoveAllFiles,
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
            mem_queues.range("droopy", 0..).unwrap().next(),
            Some((0, &b"hello"[..]))
        );
        let droopy: Vec<(u64, &[u8])> = mem_queues.range("droopy", 1..).unwrap().collect();
        assert_eq!(
            &droopy,
            &[(1, &b"happy"[..]), (2, &b"tax"[..]), (3, &b"payer"[..])],
        );
        let fable: Vec<(u64, &[u8])> = mem_queues.range("fable", 1..).unwrap().collect();
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
        assert_eq!(mem_queues.truncate("droopy", 3), Truncation::NoTruncation); // TODO fixme
        let droopy: Vec<(u64, &[u8])> = mem_queues.range("droopy", 0..).unwrap().collect();
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
        let droopy: Vec<(u64, &[u8])> = mem_queues.range("droopy", 0..).unwrap().collect();
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
        let droopy: Vec<(u64, &[u8])> = mem_queues.range("droopy", 0..).unwrap().collect();
        assert_eq!(&droopy, &[(0, &b"hello"[..])]);
    }

    #[test]
    fn test_mem_queues_non_zero_first_el() {
        let mut mem_queues = MemQueues::default();
        mem_queues.create_queue("droopy").unwrap();
        assert!(mem_queues
            .append_record("droopy", 1.into(), Some(5), b"hello")
            .is_ok());
        let droopy: Vec<(u64, &[u8])> = mem_queues.range("droopy", 0..).unwrap().collect();
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
        let droopy: Vec<(u64, &[u8])> = mem_queues.range("droopy", 5..).unwrap().collect();
        assert_eq!(
            &droopy[..],
            &[(5, &b"hello"[..]), (6, &b"happy"[..]), (7, &b"tax"[..])]
        );
    }
}
