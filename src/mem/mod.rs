mod queue;
mod queues;

pub use self::queue::MemQueue;
pub use self::queues::MemQueues;
use crate::position::{GlobalPosition, LocalPosition};

#[derive(Clone, Copy, Debug)]
pub struct RecordMeta {
    start_offset: usize,
    position: LocalPosition,
    global_position: GlobalPosition,
}
