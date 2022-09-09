# What is it?


That record log uses log rotation.

Each recordlog has its own "local" notion of position.
It is possible to truncate each of the queues individually.

# Goals

- be durable, offer some flexibility on `fsync` strategies.
- offer a way to truncate a queue after a specific position
- handle an arbitrary number of queues
- have limited IO
- be fast
- offer the possibility to implement push back

```rust
pub struct MultiRecordLog {
    pub async fn open(directory_path: &Path) -> Result<Self, ReadRecordError>;
    pub async fn append_record(&mut self,
        queue: &str,
        local_position: Option<Position>,
        payload: &[u8]) -> Result<Option<Position>>;
    pub fn iter_from<'a>(
        &'a self,
        queue: &str,
        position: Position,
    ) -> Option<impl Iterator<Item = (Position, &'a [u8])> + 'a>;
    pub async fn truncate(&mut self, queue: &str, position: Position) -> io::Result<()>
}
```
# Non-goals

This is not Kafka. That recordlog is designed for a "small amount of data".
All retained data can fit in RAM.

In the context of quickwit, that queue is used in the PushAPI and is meant to contain
1 worth of data. (At 60MB/s, means 3.6 GB of RAM)


# Implementation details.

`mrecordlog` is multiplexing several independent queues into the same record log.
This approach has the merit of limiting the number of file descriptor necessary,
and more importantly, to limit the number of `fsync`.

It also offers the possibility to truncate the queue for a given record log.
The actual deletion of the data happens when a file only contains deleted records.
Then, and only then, the entire file is deleted.

That recordlog emits a new file every 1GB.
A recordlog file is deleted, once all queues have been truncated after the
last record of a  of a file.

.

There are no compaction logic.



TO BE DONE

It also makes it possible to know the size of the recordlog, to make backpressure possible.

# Implementation

The implementation works by stacking different level of abstraction.
At the lowest level, the frame reader splits the data into frames.
Frames have a fixed size and a checksum. The combination makes the recordlog resilient to corruption.

The record reader implements a protocol to build records over the frame reader.


