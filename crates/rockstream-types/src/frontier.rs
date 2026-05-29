//! Frontier / antichain types for progress tracking.
//!
//! A frontier represents the boundary of processed time — the set of
//! timestamps at which new data may still arrive.

use serde::{Deserialize, Serialize};
use std::fmt;

/// An antichain of timestamps representing a progress frontier.
///
/// The antichain is the set of minimal elements — no element in the set
/// is less-than-or-equal-to any other element in the set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Antichain<T> {
    elements: Vec<T>,
}

impl<T: Ord + Clone> Antichain<T> {
    /// Create an empty antichain (representing "no progress").
    pub fn empty() -> Self {
        Self {
            elements: Vec::new(),
        }
    }

    /// Create an antichain from a single element.
    pub fn from_elem(elem: T) -> Self {
        Self {
            elements: vec![elem],
        }
    }

    /// Returns the elements of the antichain.
    pub fn elements(&self) -> &[T] {
        &self.elements
    }

    /// Returns true if the antichain is empty.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Returns the number of elements in the antichain.
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Returns true if `time` is less than or equal to some element in the frontier.
    ///
    /// If this returns true, the time has NOT yet been completed.
    pub fn less_equal(&self, time: &T) -> bool {
        self.elements.iter().any(|e| e <= time)
    }

    /// Insert an element, maintaining the antichain invariant.
    pub fn insert(&mut self, elem: T) {
        // Remove any elements that are >= the new element.
        self.elements.retain(|e| elem > *e);
        // Only insert if no existing element is <= the new one.
        if !self.elements.iter().any(|e| *e <= elem) {
            self.elements.push(elem);
        }
    }
}

impl<T: Ord + Clone + fmt::Display> fmt::Display for Antichain<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, elem) in self.elements.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{elem}")?;
        }
        write!(f, "]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn antichain_from_elem() {
        let ac = Antichain::from_elem(5u64);
        assert_eq!(ac.elements(), &[5]);
    }

    #[test]
    fn antichain_empty() {
        let ac: Antichain<u64> = Antichain::empty();
        assert!(ac.is_empty());
        assert_eq!(ac.len(), 0);
    }

    #[test]
    fn antichain_less_equal() {
        let ac = Antichain::from_elem(5u64);
        assert!(ac.less_equal(&5));
        assert!(ac.less_equal(&6));
        assert!(!ac.less_equal(&4));
    }

    #[test]
    fn antichain_display() {
        let ac = Antichain::from_elem(42u64);
        assert_eq!(ac.to_string(), "[42]");
    }
}
