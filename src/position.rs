use std::fmt::Display;
use std::ops::{Add, Sub};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Default)]
pub struct Position(pub u64);

impl Sub<Position> for Position {
    type Output = i64;

    fn sub(self, rhs: Position) -> Self::Output {
        assert!(self.0 <= i64::MAX as u64);
        assert!(rhs.0 < i64::MAX as u64);
        self.0 as i64 - rhs.0 as i64
    }
}

impl Position {
    pub fn inc(&mut self) {
        self.0 += 1;
    }

    pub fn plus_one(self) -> Position {
        Position(self.0 + 1)
    }
}

impl Add<u64> for Position {
    type Output = Position;

    fn add(self, add: u64) -> Position {
        Position(self.0 + add)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Default)]
pub struct FileNumber(u32);

impl From<u32> for FileNumber {
    fn from(val: u32) -> Self {
        FileNumber(val)
    }
}

impl FileNumber {
    /// Increment the position and returns the previous value.
    pub fn inc(&mut self) -> FileNumber {
        let new_pos = self.0;
        self.0 = new_pos + 1;
        FileNumber(new_pos)
    }
}

impl Display for FileNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:020}", self.0)
    }
}
