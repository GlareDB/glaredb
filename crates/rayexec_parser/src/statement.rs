use rayexec_error::{RayexecError, Result};
use std::fmt;

#[derive(Debug, Clone)]
pub enum Statement<'a> {
    Stub(&'a str),
}
