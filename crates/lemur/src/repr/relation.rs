use crate::repr::value::Value;

pub type PrimaryKeyIndices<'a> = &'a [usize];

pub type PrimaryKey<'a> = &'a [Value];

pub type RelationKey = String;
