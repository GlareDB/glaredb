use anyhow::{anyhow, Result};
use bitvec::vec::BitVec;
use std::fmt::Debug;
use std::marker::PhantomData;

const FANOUT: usize = 8;

pub trait Tuple: Sized + Debug {
    type SortKey: Ord + Debug;
    type Remaining: Debug;

    /// Split self into the sort key and whatever's leftover.
    fn split(self) -> (Self::SortKey, Self::Remaining);

    /// Merge a sort key and remaining parts into a full tuple.
    fn merge(key: Self::SortKey, remaining: Self::Remaining) -> Self;
}

#[derive(Debug)]
pub struct Pdt<T: Tuple> {
    values: ValueSpace<T>,
}

impl<T: Tuple> Pdt<T> {
    pub fn insert(&mut self, sid: u64, tuple: T) {
        unimplemented!()
    }
}

#[derive(Debug)]
enum Update<T: Tuple> {
    Insert(T::SortKey, T::Remaining),
    Delete(T::SortKey),
}

#[derive(Debug)]
struct ValueSpace<T: Tuple> {
    updates: Vec<Update<T>>,
}

#[derive(Debug)]
enum Node {
    Inner(Inner),
    Leaf(Leaf),
}

#[derive(Debug)]
struct Inner {
    sids: [Option<u64>; FANOUT],
    deltas: [Option<i64>; FANOUT],
    children: [Option<Box<Node>>; FANOUT],
}

#[derive(Debug)]
struct Leaf {
    sids: [Option<u64>; FANOUT],
    /// Index into the updates vector in the value space.
    update_idxs: [Option<usize>; FANOUT],
}
