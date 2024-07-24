use std::ops::Add;
use std::ops::AddAssign;
use std::ops::Sub;
use std::ops::SubAssign;

use egui::emath::Numeric;

pub trait Saturatingable:
    Add<Output = Self>
    + AddAssign
    + Sub<Output = Self>
    + SubAssign
    + Ord
    + From<u8>
    + Copy
    + Sized
    + Numeric
{
}

impl<N> Saturatingable for N where
    N: Add<Output = Self>
        + AddAssign
        + Sub<Output = Self>
        + SubAssign
        + Ord
        + From<u8>
        + Copy
        + Sized
        + Numeric
{
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Saturating<N: Saturatingable> {
    val: N,
    lower_bound: N,
    upper_bound: N,
}

impl<N: Saturatingable> Saturating<N> {
    pub fn new(val: N, lower_bound: N, upper_bound: N) -> Self {
        Self {
            val: val.clamp(lower_bound, upper_bound),
            lower_bound,
            upper_bound,
        }
    }

    pub fn inc(&mut self) -> bool {
        if self.val == self.upper_bound {
            false
        } else {
            *self += 1u8.into();
            true
        }
    }

    pub fn dec(&mut self) -> bool {
        if self.val == self.lower_bound {
            false
        } else {
            *self -= 1u8.into();
            true
        }
    }

    pub fn set(&mut self, val: N) -> bool {
        let old_val = self.val;
        self.val = val;
        self.val = self.val.clamp(self.lower_bound, self.upper_bound);

        self.val != old_val
    }

    pub fn val(&self) -> N {
        self.val
    }

    pub fn with_val_mut<T>(&mut self, f: impl FnOnce(&mut N) -> T) -> T {
        let res = f(&mut self.val);
        self.val = self.val.clamp(self.lower_bound, self.upper_bound);
        res
    }

    pub fn lower_bound(&self) -> N {
        self.lower_bound
    }

    pub fn upper_bound(&self) -> N {
        self.upper_bound
    }
}

impl<N: Saturatingable> Add<N> for Saturating<N> {
    type Output = Self;

    fn add(self, other: N) -> Self {
        Self {
            val: (self.val + other).clamp(self.lower_bound, self.upper_bound),
            lower_bound: self.lower_bound,
            upper_bound: self.upper_bound,
        }
    }
}

impl<N: Saturatingable> AddAssign<N> for Saturating<N> {
    fn add_assign(&mut self, other: N) {
        self.val = self.val + other;
    }
}

impl<N: Saturatingable> Sub<N> for Saturating<N> {
    type Output = Self;

    fn sub(self, other: N) -> Self {
        Self {
            val: (self.val - other).clamp(self.lower_bound, self.upper_bound),
            lower_bound: self.lower_bound,
            upper_bound: self.upper_bound,
        }
    }
}

impl<N: Saturatingable> SubAssign<N> for Saturating<N> {
    fn sub_assign(&mut self, other: N) {
        self.val = self.val - other
    }
}
