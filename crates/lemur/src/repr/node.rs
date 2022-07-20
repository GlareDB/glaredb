use std::ops::{Deref, DerefMut};

#[derive(Debug, Clone)]
pub struct Node<T, M = ()> {
    inner: T,
    meta: M,
}

impl<T, M> Node<T, M> {
    pub fn new(inner: T, meta: M) -> Self {
        Node { inner, meta }
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    pub fn meta(&self) -> &M {
        &self.meta
    }

    pub fn meta_mut(&mut self) -> &mut M {
        &mut self.meta
    }

    pub fn map<U, F>(self, f: F) -> Node<U, M>
    where
        F: FnOnce(T) -> U,
    {
        Node {
            inner: f(self.inner),
            meta: self.meta,
        }
    }
}

impl<T, M> Node<Box<T>, M> {
    pub fn new_boxed(inner: T, meta: M) -> Self {
        Node {
            inner: Box::new(inner),
            meta,
        }
    }
}

impl<T, M> Deref for Node<T, M> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T, M> DerefMut for Node<T, M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
