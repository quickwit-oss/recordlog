use std::fmt::Display;

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
