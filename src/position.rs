use std::fmt::Display;
use std::ops::Sub;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Default)]
pub struct LocalPosition(pub u64);

impl Sub<LocalPosition> for LocalPosition {
    type Output = i64;

    fn sub(self, rhs: LocalPosition) -> Self::Output {
        assert!(self.0 <= i64::MAX as u64);
        assert!(rhs.0 < i64::MAX as u64);
        self.0 as i64 - rhs.0 as i64
    }
}

impl LocalPosition {
    pub fn inc(&mut self) {
        self.0 += 1;
    }

    pub fn plus_one(self) -> LocalPosition {
        LocalPosition(self.0 + 1)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Default)]
pub struct GlobalPosition(u64);

impl From<u64> for GlobalPosition {
    fn from(val: u64) -> Self {
        GlobalPosition(val)
    }
}

impl GlobalPosition {
    /// Increment the position and returns the previous value.
    pub fn inc(&mut self) -> GlobalPosition {
        let new_pos = self.0;
        self.0 = new_pos + 1;
        GlobalPosition(new_pos)
    }

    pub fn previous(&self) -> Option<Self> {
        if self.0 == 0 {
            return None;
        }
        Some(GlobalPosition(self.0 - 1))
    }
}

impl Display for GlobalPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:020}", self.0)
    }
}
