use itertools::Itertools;
use std::fmt;
use thiserror::Error;

#[derive(Debug, Error)]
pub struct IndexOutOfBoundsError {
    index: usize,
    len: usize,
}

impl IndexOutOfBoundsError {
    pub fn new(index: usize, len: usize) -> Self {
        Self { index, len }
    }
}

impl fmt::Display for IndexOutOfBoundsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Index {} is out of bounds for vector of length {}", self.index, self.len)
    }
}

pub trait VecExtensions<T> {
    /// Pushes the provided value to the container if the container is empty
    fn push_if_empty(self, value: T) -> Self;

    /// Inserts the provided `value` at `index` while swapping the item at index to the end of the container
    fn swap_insert(&mut self, index: usize, value: T);

    /// Retains only the elements at the specified indices of the vector.
    ///
    /// Note: This function was defined to be lenient, in the sense that it will not panic if the provided indices are out of bounds, instead return an [`IndexOutOfBoundsError`], and will deduplicate indices.
    fn retain_indices(&mut self, indices: &[usize]) -> Result<(), IndexOutOfBoundsError>;
}

impl<T> VecExtensions<T> for Vec<T> {
    fn push_if_empty(mut self, value: T) -> Self {
        if self.is_empty() {
            self.push(value);
        }
        self
    }

    fn swap_insert(&mut self, index: usize, value: T) {
        self.push(value);
        let loc = self.len() - 1;
        self.swap(index, loc);
    }

    fn retain_indices(&mut self, indices: &[usize]) -> Result<(), IndexOutOfBoundsError> {
        match indices.len() {
            // if indices is empty, there is nothing to retain
            0 => Ok(()),
            // if indices has one element, we can index into the vector without further action
            1 => {
                let index = indices[0];
                if index >= self.len() {
                    return Err(IndexOutOfBoundsError::new(index, self.len()));
                }
                *self = vec![self.remove(index)];
                Ok(())
            }
            // if indices has more than one element, we need to sort and deduplicate the indices and run the retain function accordingly
            len_of_indices => {
                let indices: Vec<usize> = indices.iter().cloned().sorted_unstable().dedup().collect();

                if indices.last().unwrap() >= &self.len() {
                    return Err(IndexOutOfBoundsError::new(*indices.last().unwrap(), self.len()));
                }

                if indices.len() == self.len() {
                    // if indices are equal to the length of the vector, we can keep the vector as is
                    return Ok(());
                }

                let mut i = 0;
                let mut j = 0;
                self.retain(|_| {
                    if j < len_of_indices && indices[j] == i {
                        j += 1;
                        true
                    } else {
                        i += 1;
                        false
                    }
                });

                Ok(())
            }
        }
    }
}
