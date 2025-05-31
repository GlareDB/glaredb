// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Contains structs and methods to build Parquet schema and schema descriptors.

use std::fmt;
use std::sync::Arc;

use glaredb_error::{DbError, Result};

use crate::basic::{
    ColumnOrder, ConvertedType, LogicalType, Repetition, SortOrder, Type as PhysicalType,
};
use crate::format;

/// Representation of a Parquet type.
///
/// Used to describe primitive leaf fields and structs, including top-level schema.
///
/// Note that the top-level schema is represented using [`Type::GroupType`] whose
/// repetition is `None`.
// TODO: Are these arcs actually required?
#[derive(Clone, Debug, PartialEq)]
pub enum SchemaType {
    PrimitiveType(Arc<PrimitiveType>),
    GroupType(Arc<GroupType>),
}

impl From<Arc<PrimitiveType>> for SchemaType {
    fn from(value: Arc<PrimitiveType>) -> Self {
        SchemaType::PrimitiveType(value)
    }
}

impl From<Arc<GroupType>> for SchemaType {
    fn from(value: Arc<GroupType>) -> Self {
        SchemaType::GroupType(value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PrimitiveType {
    pub basic_info: BasicTypeInfo,
    pub physical_type: PhysicalType,
    pub type_length: i32,
    pub scale: i32,
    pub precision: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupType {
    pub basic_info: BasicTypeInfo,
    pub fields: Vec<SchemaType>,
}

impl SchemaType {
    /// Creates primitive type builder with provided field name and physical
    /// type.
    pub fn primitive_type_builder(name: &str, physical_type: PhysicalType) -> PrimitiveTypeBuilder {
        PrimitiveTypeBuilder::new(name, physical_type)
    }

    /// Creates group type builder with provided column name.
    pub fn group_type_builder(name: &str) -> GroupTypeBuilder {
        GroupTypeBuilder::new(name)
    }

    /// Returns [`BasicTypeInfo`] information about the type.
    pub fn get_basic_info(&self) -> &BasicTypeInfo {
        match self {
            SchemaType::PrimitiveType(prim) => &prim.basic_info,
            SchemaType::GroupType(group) => &group.basic_info,
        }
    }

    /// Returns this type's field name.
    pub fn name(&self) -> &str {
        self.get_basic_info().name()
    }

    /// Gets the fields from this group type.
    /// Note that this will panic if called on a non-group type.
    pub fn get_fields(&self) -> &[SchemaType] {
        match self {
            SchemaType::GroupType(group) => &group.fields,
            _ => panic!("Cannot call get_fields() on a non-group type"),
        }
    }

    /// Returns `true` if this type is a primitive type, `false` otherwise.
    pub fn is_primitive(&self) -> bool {
        matches!(self, SchemaType::PrimitiveType(_))
    }

    /// Returns `true` if this type is a group type, `false` otherwise.
    pub fn is_group(&self) -> bool {
        matches!(*self, SchemaType::GroupType(_))
    }

    /// Returns `true` if this type is the top-level schema type (message type).
    pub fn is_schema(&self) -> bool {
        match self {
            SchemaType::GroupType(group) => !group.basic_info.has_repetition(),
            _ => false,
        }
    }

    /// Returns `true` if this type is repeated or optional.
    /// If this type doesn't have repetition defined, we still treat it as optional.
    pub fn is_optional(&self) -> bool {
        self.get_basic_info().has_repetition()
            && self.get_basic_info().repetition() != Repetition::REQUIRED
    }
}

/// A builder for primitive types. All attributes are optional
/// except the name and physical type.
/// Note that if not specified explicitly, `Repetition::OPTIONAL` is used.
pub struct PrimitiveTypeBuilder<'a> {
    name: &'a str,
    repetition: Repetition,
    physical_type: PhysicalType,
    converted_type: ConvertedType,
    logical_type: Option<LogicalType>,
    length: i32,
    precision: i32,
    scale: i32,
    id: Option<i32>,
}

impl<'a> PrimitiveTypeBuilder<'a> {
    /// Creates new primitive type builder with provided field name and physical type.
    pub fn new(name: &'a str, physical_type: PhysicalType) -> Self {
        Self {
            name,
            repetition: Repetition::OPTIONAL,
            physical_type,
            converted_type: ConvertedType::NONE,
            logical_type: None,
            length: -1,
            precision: -1,
            scale: -1,
            id: None,
        }
    }

    /// Sets [`Repetition`] for this field and returns itself.
    pub fn with_repetition(self, repetition: Repetition) -> Self {
        Self { repetition, ..self }
    }

    /// Sets [`ConvertedType`] for this field and returns itself.
    pub fn with_converted_type(self, converted_type: ConvertedType) -> Self {
        Self {
            converted_type,
            ..self
        }
    }

    /// Sets [`LogicalType`] for this field and returns itself.
    /// If only the logical type is populated for a primitive type, the converted type
    /// will be automatically populated, and can thus be omitted.
    pub fn with_logical_type(self, logical_type: Option<LogicalType>) -> Self {
        Self {
            logical_type,
            ..self
        }
    }

    /// Sets type length and returns itself.
    /// This is only applied to FIXED_LEN_BYTE_ARRAY and INT96 (INTERVAL) types, because
    /// they maintain fixed size underlying byte array.
    /// By default, value is `0`.
    pub fn with_length(self, length: i32) -> Self {
        Self { length, ..self }
    }

    /// Sets precision for Parquet DECIMAL physical type and returns itself.
    /// By default, it equals to `0` and used only for decimal context.
    pub fn with_precision(self, precision: i32) -> Self {
        Self { precision, ..self }
    }

    /// Sets scale for Parquet DECIMAL physical type and returns itself.
    /// By default, it equals to `0` and used only for decimal context.
    pub fn with_scale(self, scale: i32) -> Self {
        Self { scale, ..self }
    }

    /// Sets optional field id and returns itself.
    pub fn with_id(self, id: Option<i32>) -> Self {
        Self { id, ..self }
    }

    /// Creates a new `PrimitiveType` instance from the collected attributes.
    /// Returns `Err` in case of any building conditions are not met.
    pub fn build(self) -> Result<PrimitiveType> {
        let mut basic_info = BasicTypeInfo {
            name: String::from(self.name),
            repetition: Some(self.repetition),
            converted_type: self.converted_type,
            logical_type: self.logical_type.clone(),
            id: self.id,
        };

        // Check length before logical type, since it is used for logical type validation.
        if self.physical_type == PhysicalType::FIXED_LEN_BYTE_ARRAY && self.length < 0 {
            return Err(DbError::new(format!(
                "Invalid FIXED_LEN_BYTE_ARRAY length: {} for field '{}'",
                self.length, self.name
            )));
        }

        if let Some(logical_type) = &self.logical_type {
            // If a converted type is populated, check that it is consistent with
            // its logical type
            if self.converted_type != ConvertedType::NONE {
                if ConvertedType::from(self.logical_type.clone()) != self.converted_type {
                    return Err(DbError::new(format!(
                        "Logical type {:?} is incompatible with converted type {} for field '{}'",
                        logical_type, self.converted_type, self.name
                    )));
                }
            } else {
                // Populate the converted type for backwards compatibility
                basic_info.converted_type = self.logical_type.clone().into();
            }
            // Check that logical type and physical type are compatible
            match (logical_type, self.physical_type) {
                (LogicalType::Map, _) | (LogicalType::List, _) => {
                    return Err(DbError::new(format!(
                        "{:?} cannot be applied to a primitive type for field '{}'",
                        logical_type, self.name
                    )));
                }
                (LogicalType::Enum, PhysicalType::BYTE_ARRAY) => {}
                (LogicalType::Decimal { scale, precision }, _) => {
                    // Check that scale and precision are consistent with legacy values
                    if *scale != self.scale {
                        return Err(DbError::new(format!(
                            "DECIMAL logical type scale {} must match self.scale {} for field '{}'",
                            scale, self.scale, self.name
                        )));
                    }
                    if *precision != self.precision {
                        return Err(DbError::new(format!(
                            "DECIMAL logical type precision {} must match self.precision {} for field '{}'",
                            precision, self.precision, self.name
                        )));
                    }
                    self.check_decimal_precision_scale()?;
                }
                (LogicalType::Date, PhysicalType::INT32) => {}
                (
                    LogicalType::Time {
                        unit: format::TimeUnit::MILLIS(_),
                        ..
                    },
                    PhysicalType::INT32,
                ) => {}
                (LogicalType::Time { unit, .. }, PhysicalType::INT64) => {
                    if *unit == format::TimeUnit::MILLIS(Default::default()) {
                        return Err(DbError::new(format!(
                            "Cannot use millisecond unit on INT64 type for field '{}'",
                            self.name
                        )));
                    }
                }
                (LogicalType::Timestamp { .. }, PhysicalType::INT64) => {}
                (LogicalType::Integer { bit_width, .. }, PhysicalType::INT32)
                    if *bit_width <= 32 => {}
                (LogicalType::Integer { bit_width, .. }, PhysicalType::INT64)
                    if *bit_width == 64 => {}
                // Null type
                (LogicalType::Unknown, PhysicalType::INT32) => {}
                (LogicalType::String, PhysicalType::BYTE_ARRAY) => {}
                (LogicalType::Json, PhysicalType::BYTE_ARRAY) => {}
                (LogicalType::Bson, PhysicalType::BYTE_ARRAY) => {}
                (LogicalType::Uuid, PhysicalType::FIXED_LEN_BYTE_ARRAY) if self.length == 16 => {}
                (LogicalType::Uuid, PhysicalType::FIXED_LEN_BYTE_ARRAY) => {
                    return Err(DbError::new(format!(
                        "UUID cannot annotate field '{}' because it is not a FIXED_LEN_BYTE_ARRAY(16) field",
                        self.name
                    )));
                }
                (LogicalType::Float16, PhysicalType::FIXED_LEN_BYTE_ARRAY) if self.length == 2 => {}
                (LogicalType::Float16, PhysicalType::FIXED_LEN_BYTE_ARRAY) => {
                    return Err(DbError::new(format!(
                        "FLOAT16 cannot annotate field '{}' because it is not a FIXED_LEN_BYTE_ARRAY(2) field",
                        self.name
                    )));
                }
                (a, b) => {
                    return Err(DbError::new(format!(
                        "Cannot annotate {:?} from {} for field '{}'",
                        a, b, self.name
                    )));
                }
            }
        }

        match self.converted_type {
            ConvertedType::NONE => {}
            ConvertedType::UTF8 | ConvertedType::BSON | ConvertedType::JSON => {
                if self.physical_type != PhysicalType::BYTE_ARRAY {
                    return Err(DbError::new(format!(
                        "{} cannot annotate field '{}' because it is not a BYTE_ARRAY field",
                        self.converted_type, self.name
                    )));
                }
            }
            ConvertedType::DECIMAL => {
                self.check_decimal_precision_scale()?;
            }
            ConvertedType::DATE
            | ConvertedType::TIME_MILLIS
            | ConvertedType::UINT_8
            | ConvertedType::UINT_16
            | ConvertedType::UINT_32
            | ConvertedType::INT_8
            | ConvertedType::INT_16
            | ConvertedType::INT_32 => {
                if self.physical_type != PhysicalType::INT32 {
                    return Err(DbError::new(format!(
                        "{} cannot annotate field '{}' because it is not a INT32 field",
                        self.converted_type, self.name
                    )));
                }
            }
            ConvertedType::TIME_MICROS
            | ConvertedType::TIMESTAMP_MILLIS
            | ConvertedType::TIMESTAMP_MICROS
            | ConvertedType::UINT_64
            | ConvertedType::INT_64 => {
                if self.physical_type != PhysicalType::INT64 {
                    return Err(DbError::new(format!(
                        "{} cannot annotate field '{}' because it is not a INT64 field",
                        self.converted_type, self.name
                    )));
                }
            }
            ConvertedType::INTERVAL => {
                if self.physical_type != PhysicalType::FIXED_LEN_BYTE_ARRAY || self.length != 12 {
                    return Err(DbError::new(format!(
                        "INTERVAL cannot annotate field '{}' because it is not a FIXED_LEN_BYTE_ARRAY(12) field",
                        self.name
                    )));
                }
            }
            ConvertedType::ENUM => {
                if self.physical_type != PhysicalType::BYTE_ARRAY {
                    return Err(DbError::new(format!(
                        "ENUM cannot annotate field '{}' because it is not a BYTE_ARRAY field",
                        self.name
                    )));
                }
            }
            _ => {
                return Err(DbError::new(format!(
                    "{} cannot be applied to primitive field '{}'",
                    self.converted_type, self.name
                )));
            }
        }

        Ok(PrimitiveType {
            basic_info,
            physical_type: self.physical_type,
            type_length: self.length,
            scale: self.scale,
            precision: self.precision,
        })
    }

    #[inline]
    fn check_decimal_precision_scale(&self) -> Result<()> {
        match self.physical_type {
            PhysicalType::INT32
            | PhysicalType::INT64
            | PhysicalType::BYTE_ARRAY
            | PhysicalType::FIXED_LEN_BYTE_ARRAY => (),
            _ => {
                return Err(DbError::new(
                    "DECIMAL can only annotate INT32, INT64, BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY",
                ));
            }
        }

        // Precision is required and must be a non-zero positive integer.
        if self.precision < 1 {
            return Err(DbError::new(format!(
                "Invalid DECIMAL precision: {}",
                self.precision
            )));
        }

        // Scale must be zero or a positive integer less than the precision.
        if self.scale < 0 {
            return Err(DbError::new(format!(
                "Invalid DECIMAL scale: {}",
                self.scale
            )));
        }

        if self.scale > self.precision {
            return Err(DbError::new(format!(
                "Invalid DECIMAL: scale ({}) cannot be greater than precision ({})",
                self.scale, self.precision
            )));
        }

        // Check precision and scale based on physical type limitations.
        match self.physical_type {
            PhysicalType::INT32 => {
                if self.precision > 9 {
                    return Err(DbError::new(format!(
                        "Cannot represent INT32 as DECIMAL with precision {}",
                        self.precision
                    )));
                }
            }
            PhysicalType::INT64 => {
                if self.precision > 18 {
                    return Err(DbError::new(format!(
                        "Cannot represent INT64 as DECIMAL with precision {}",
                        self.precision
                    )));
                }
            }
            PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                let max_precision = (2f64.powi(8 * self.length - 1) - 1f64).log10().floor() as i32;

                if self.precision > max_precision {
                    return Err(DbError::new(format!(
                        "Cannot represent FIXED_LEN_BYTE_ARRAY as DECIMAL with length {} and \
                        precision {}. The max precision can only be {}",
                        self.length, self.precision, max_precision
                    )));
                }
            }
            _ => (), // For BYTE_ARRAY precision is not limited
        }

        Ok(())
    }
}

/// A builder for group types. All attributes are optional except the name.
/// Note that if not specified explicitly, `None` is used as the repetition of the group,
/// which means it is a root (message) type.
pub struct GroupTypeBuilder<'a> {
    name: &'a str,
    repetition: Option<Repetition>,
    converted_type: ConvertedType,
    logical_type: Option<LogicalType>,
    fields: Vec<SchemaType>,
    id: Option<i32>,
}

impl<'a> GroupTypeBuilder<'a> {
    /// Creates new group type builder with provided field name.
    pub fn new(name: &'a str) -> Self {
        Self {
            name,
            repetition: None,
            converted_type: ConvertedType::NONE,
            logical_type: None,
            fields: Vec::new(),
            id: None,
        }
    }

    /// Sets [`Repetition`] for this field and returns itself.
    pub fn with_repetition(mut self, repetition: Repetition) -> Self {
        self.repetition = Some(repetition);
        self
    }

    /// Sets [`ConvertedType`] for this field and returns itself.
    pub fn with_converted_type(self, converted_type: ConvertedType) -> Self {
        Self {
            converted_type,
            ..self
        }
    }

    /// Sets [`LogicalType`] for this field and returns itself.
    pub fn with_logical_type(self, logical_type: Option<LogicalType>) -> Self {
        Self {
            logical_type,
            ..self
        }
    }

    /// Sets a list of fields that should be child nodes of this field.
    /// Returns updated self.
    pub fn with_fields(self, fields: Vec<SchemaType>) -> Self {
        Self { fields, ..self }
    }

    /// Sets optional field id and returns itself.
    pub fn with_id(self, id: Option<i32>) -> Self {
        Self { id, ..self }
    }

    /// Creates a new `GroupType` instance from the gathered attributes.
    pub fn build(self) -> Result<GroupType> {
        let mut basic_info = BasicTypeInfo {
            name: String::from(self.name),
            repetition: self.repetition,
            converted_type: self.converted_type,
            logical_type: self.logical_type.clone(),
            id: self.id,
        };
        // Populate the converted type if only the logical type is populated
        if self.logical_type.is_some() && self.converted_type == ConvertedType::NONE {
            basic_info.converted_type = self.logical_type.into();
        }
        Ok(GroupType {
            basic_info,
            fields: self.fields,
        })
    }
}

/// Basic type info. This contains information such as the name of the type,
/// the repetition level, the logical type and the kind of the type (group, primitive).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BasicTypeInfo {
    name: String,
    repetition: Option<Repetition>,
    converted_type: ConvertedType,
    logical_type: Option<LogicalType>,
    id: Option<i32>,
}

impl BasicTypeInfo {
    /// Returns field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns `true` if type has repetition field set, `false` otherwise.
    /// This is mostly applied to group type, because primitive type always has
    /// repetition set.
    pub fn has_repetition(&self) -> bool {
        self.repetition.is_some()
    }

    /// Returns [`Repetition`] value for the type.
    pub fn repetition(&self) -> Repetition {
        assert!(self.repetition.is_some());
        self.repetition.unwrap()
    }

    /// Returns [`ConvertedType`] value for the type.
    pub fn converted_type(&self) -> ConvertedType {
        self.converted_type
    }

    /// Returns [`LogicalType`] value for the type.
    pub fn logical_type(&self) -> Option<LogicalType> {
        // Unlike ConvertedType, LogicalType cannot implement Copy, thus we clone it
        self.logical_type.clone()
    }

    /// Returns `true` if id is set, `false` otherwise.
    pub fn has_id(&self) -> bool {
        self.id.is_some()
    }

    /// Returns id value for the type.
    pub fn id(&self) -> i32 {
        assert!(self.id.is_some());
        self.id.unwrap()
    }
}

/// Represents the location of a column in a Parquet schema
#[derive(Clone, PartialEq, Debug, Eq, Hash)]
pub struct ColumnPath {
    pub parts: Vec<String>,
}

impl ColumnPath {
    /// Creates new column path from vector of field names.
    pub fn new(parts: impl IntoIterator<Item = String>) -> Self {
        ColumnPath {
            parts: parts.into_iter().collect(),
        }
    }
}

impl fmt::Display for ColumnPath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.parts.join("."))
    }
}

impl From<Vec<String>> for ColumnPath {
    fn from(parts: Vec<String>) -> Self {
        ColumnPath { parts }
    }
}

impl From<&str> for ColumnPath {
    fn from(single_path: &str) -> Self {
        let s = String::from(single_path);
        ColumnPath::from(s)
    }
}

impl From<String> for ColumnPath {
    fn from(single_path: String) -> Self {
        let v = vec![single_path];
        ColumnPath { parts: v }
    }
}

impl AsRef<[String]> for ColumnPath {
    fn as_ref(&self) -> &[String] {
        &self.parts
    }
}

/// Physical type for leaf-level primitive columns.
///
/// Also includes the maximum definition and repetition levels required to
/// re-assemble nested data.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDescriptor {
    /// The "leaf" primitive type of this column
    pub primitive_type: Arc<PrimitiveType>,
    /// The maximum definition level for this column
    pub max_def_level: i16,
    /// The maximum repetition level for this column
    pub max_rep_level: i16,
    /// The path of this column. For instance, "a.b.c.d".
    // TODO: Why here?
    pub path: Arc<ColumnPath>,
}

impl ColumnDescriptor {
    /// Creates new descriptor for leaf-level column.
    pub fn new(
        primitive_type: Arc<PrimitiveType>,
        max_def_level: i16,
        max_rep_level: i16,
        path: Arc<ColumnPath>,
    ) -> Self {
        Self {
            primitive_type,
            max_def_level,
            max_rep_level,
            path,
        }
    }

    /// Returns column name.
    pub fn name(&self) -> &str {
        &self.primitive_type.basic_info.name
    }

    /// Returns [`ConvertedType`] for this column.
    pub fn converted_type(&self) -> ConvertedType {
        self.primitive_type.basic_info.converted_type()
    }

    /// Returns [`LogicalType`] for this column.
    pub fn logical_type(&self) -> Option<LogicalType> {
        self.primitive_type.basic_info.logical_type()
    }

    /// Returns physical type for this column.
    /// Note that it will panic if called on a non-primitive type.
    pub fn physical_type(&self) -> PhysicalType {
        self.primitive_type.physical_type
    }

    /// Returns type length for this column.
    /// Note that it will panic if called on a non-primitive type.
    pub fn type_length(&self) -> i32 {
        self.primitive_type.type_length
    }

    /// Returns the sort order for this column
    pub fn sort_order(&self) -> SortOrder {
        ColumnOrder::get_sort_order(
            self.logical_type(),
            self.converted_type(),
            self.physical_type(),
        )
    }
}

/// Schema of a Parquet file.
///
/// Encapsulates the file's schema ([`Type`]) and [`ColumnDescriptor`]s for
/// each primitive (leaf) column.
#[derive(PartialEq)]
pub struct SchemaDescriptor {
    /// The top-level logical schema (the "message" type).
    ///
    /// This must be a [`Type::GroupType`] where each field is a root
    /// column type in the schema.
    pub schema: Arc<GroupType>,

    /// The descriptors for the physical type of each leaf column in this schema
    ///
    /// Constructed from `schema` in DFS order.
    pub leaves: Vec<ColumnDescriptor>,

    /// Mapping from a leaf column's index to the root column index that it
    /// comes from.
    ///
    /// For instance: the leaf `a.b.c.d` would have a link back to `a`:
    /// ```text
    /// -- a  <-----+
    /// -- -- b     |
    /// -- -- -- c  |
    /// -- -- -- -- d
    /// ```
    pub leaf_to_base: Vec<usize>,
}

impl fmt::Debug for SchemaDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Skip leaves and leaf_to_base as they only a cache information already found in `schema`
        f.debug_struct("SchemaDescriptor")
            .field("schema", &self.schema)
            .finish()
    }
}

impl SchemaDescriptor {
    /// Creates new schema descriptor from Parquet schema.
    pub fn new(tp: Arc<GroupType>) -> Self {
        let mut leaves = vec![];
        let mut leaf_to_base = Vec::new();
        for (root_idx, f) in tp.fields.iter().enumerate() {
            let mut path = vec![];
            build_tree(f, root_idx, 0, 0, &mut leaves, &mut leaf_to_base, &mut path);
        }

        Self {
            schema: tp,
            leaves,
            leaf_to_base,
        }
    }

    /// Returns [`ColumnDescriptor`] for a field position.
    ///
    /// Panics if `i` is out of bounds.
    pub fn column(&self, i: usize) -> &ColumnDescriptor {
        assert!(
            i < self.leaves.len(),
            "Index out of bound: {} not in [0, {})",
            i,
            self.leaves.len()
        );
        &self.leaves[i]
    }

    /// Returns slice of [`ColumnDescriptor`].
    pub fn columns(&self) -> &[ColumnDescriptor] {
        &self.leaves
    }

    /// Returns number of leaf-level columns.
    pub fn num_columns(&self) -> usize {
        self.leaves.len()
    }

    /// Returns column root [`Type`] for a leaf position.
    pub fn get_column_root(&self, i: usize) -> &SchemaType {
        self.column_root_of(i)
    }

    /// Returns the index of the root column for a field position
    pub fn get_column_root_idx(&self, leaf: usize) -> usize {
        assert!(
            leaf < self.leaves.len(),
            "Index out of bound: {} not in [0, {})",
            leaf,
            self.leaves.len()
        );

        *self
            .leaf_to_base
            .get(leaf)
            .unwrap_or_else(|| panic!("Expected a value for index {leaf} but found None"))
    }

    fn column_root_of(&self, i: usize) -> &SchemaType {
        &self.schema.fields[self.get_column_root_idx(i)]
    }

    /// Returns schema name.
    pub fn name(&self) -> &str {
        &self.schema.basic_info.name
    }
}

fn build_tree<'a>(
    tp: &'a SchemaType,
    root_idx: usize,
    mut max_rep_level: i16,
    mut max_def_level: i16,
    leaves: &mut Vec<ColumnDescriptor>,
    leaf_to_base: &mut Vec<usize>,
    path_so_far: &mut Vec<&'a str>,
) {
    assert!(tp.get_basic_info().has_repetition());

    path_so_far.push(tp.name());
    match tp.get_basic_info().repetition() {
        Repetition::OPTIONAL => {
            max_def_level += 1;
        }
        Repetition::REPEATED => {
            max_def_level += 1;
            max_rep_level += 1;
        }
        _ => {}
    }

    match tp {
        SchemaType::PrimitiveType(prim) => {
            let mut path: Vec<String> = vec![];
            path.extend(path_so_far.iter().copied().map(String::from));
            leaves.push(ColumnDescriptor::new(
                prim.clone(),
                max_def_level,
                max_rep_level,
                Arc::new(ColumnPath::new(path)),
            ));
            leaf_to_base.push(root_idx);
        }
        SchemaType::GroupType(group) => {
            for field in &group.fields {
                build_tree(
                    field,
                    root_idx,
                    max_rep_level,
                    max_def_level,
                    leaves,
                    leaf_to_base,
                    path_so_far,
                );
                path_so_far.pop();
            }
        }
    }
}

/// Get the root schema type from the given elements.
pub fn schema_from_thrift(elements: &[format::SchemaElement]) -> Result<Arc<GroupType>> {
    let mut index = 0;
    let mut schema_nodes = Vec::new();
    while index < elements.len() {
        let t = from_thrift_helper(elements, index)?;
        index = t.0;
        schema_nodes.push(t.1);
    }
    if schema_nodes.len() != 1 {
        return Err(DbError::new(format!(
            "Expected exactly one root node, but found {}",
            schema_nodes.len()
        )));
    }

    match schema_nodes.remove(0) {
        SchemaType::GroupType(group) => Ok(group),
        SchemaType::PrimitiveType(_) => Err(DbError::new(
            "Expected a group type for the root schema type",
        )),
    }
}

/// Constructs a new Type from the `elements`, starting at index `index`. The
/// first result is the starting index for the next Type after this one. If it
/// is equal to `elements.len()`, then this Type is the last one. The second
/// result is the result Type.
fn from_thrift_helper(
    elements: &[format::SchemaElement],
    index: usize,
) -> Result<(usize, SchemaType)> {
    // Whether or not the current node is root (message type).
    // There is only one message type node in the schema tree.
    let is_root_node = index == 0;

    if index > elements.len() {
        return Err(DbError::new(format!(
            "Index out of bound, index = {}, len = {}",
            index,
            elements.len()
        )));
    }
    let element = &elements[index];
    let converted_type = ConvertedType::try_from(element.converted_type)?;
    // LogicalType is only present in v2 Parquet files. ConvertedType is always
    // populated, regardless of the version of the file (v1 or v2).
    let logical_type = element
        .logical_type
        .as_ref()
        .map(|value| LogicalType::from(value.clone()));
    let field_id = elements[index].field_id;
    match elements[index].num_children {
        // From parquet-format:
        //   The children count is used to construct the nested relationship.
        //   This field is not set when the element is a primitive type
        // Sometimes parquet-cpp sets num_children field to 0 for primitive types, so we
        // have to handle this case too.
        None | Some(0) => {
            // primitive type
            if elements[index].repetition_type.is_none() {
                return Err(DbError::new(
                    "Repetition level must be defined for a primitive type",
                ));
            }
            let repetition = Repetition::try_from(elements[index].repetition_type.unwrap())?;
            if let Some(type_) = elements[index].type_ {
                let physical_type = PhysicalType::try_from(type_)?;
                let length = elements[index].type_length.unwrap_or(-1);
                let scale = elements[index].scale.unwrap_or(-1);
                let precision = elements[index].precision.unwrap_or(-1);
                let name = &elements[index].name;
                let builder = SchemaType::primitive_type_builder(name, physical_type)
                    .with_repetition(repetition)
                    .with_converted_type(converted_type)
                    .with_logical_type(logical_type)
                    .with_length(length)
                    .with_precision(precision)
                    .with_scale(scale)
                    .with_id(field_id);
                Ok((
                    index + 1,
                    SchemaType::PrimitiveType(Arc::new(builder.build()?)),
                ))
            } else {
                let mut builder = SchemaType::group_type_builder(&elements[index].name)
                    .with_converted_type(converted_type)
                    .with_logical_type(logical_type)
                    .with_id(field_id);
                if !is_root_node {
                    // Sometimes parquet-cpp and parquet-mr set repetition level REQUIRED or
                    // REPEATED for root node.
                    //
                    // We only set repetition for group types that are not top-level message
                    // type. According to parquet-format:
                    //   Root of the schema does not have a repetition_type.
                    //   All other types must have one.
                    builder = builder.with_repetition(repetition);
                }
                Ok((
                    index + 1,
                    SchemaType::GroupType(Arc::new(builder.build().unwrap())),
                ))
            }
        }
        Some(n) => {
            let repetition = elements[index]
                .repetition_type
                .map(Repetition::try_from)
                .transpose()?;

            let mut fields = vec![];
            let mut next_index = index + 1;
            for _ in 0..n {
                let child_result = from_thrift_helper(elements, next_index)?;
                next_index = child_result.0;
                fields.push(child_result.1);
            }

            let mut builder = SchemaType::group_type_builder(&elements[index].name)
                .with_converted_type(converted_type)
                .with_logical_type(logical_type)
                .with_fields(fields)
                .with_id(field_id);
            if let Some(rep) = repetition {
                // Sometimes parquet-cpp and parquet-mr set repetition level REQUIRED or
                // REPEATED for root node.
                //
                // We only set repetition for group types that are not top-level message
                // type. According to parquet-format:
                //   Root of the schema does not have a repetition_type.
                //   All other types must have one.
                if !is_root_node {
                    builder = builder.with_repetition(rep);
                }
            }
            Ok((
                next_index,
                SchemaType::GroupType(Arc::new(builder.build().unwrap())),
            ))
        }
    }
}

/// Method to convert to Thrift.
pub fn schema_to_thrift(schema: &GroupType) -> Result<Vec<format::SchemaElement>> {
    let mut elements: Vec<format::SchemaElement> = Vec::new();
    group_to_thrift(schema, &mut elements);
    Ok(elements)
}

fn type_to_thrift(typ: &SchemaType, elements: &mut Vec<format::SchemaElement>) {
    match typ {
        SchemaType::GroupType(group) => group_to_thrift(group, elements),
        SchemaType::PrimitiveType(prim) => primitive_to_thrift(prim, elements),
    }
}

fn group_to_thrift(group: &GroupType, elements: &mut Vec<format::SchemaElement>) {
    let repetition = if group.basic_info.has_repetition() {
        Some(group.basic_info.repetition().into())
    } else {
        None
    };

    let element = format::SchemaElement {
        type_: None,
        type_length: None,
        repetition_type: repetition,
        name: group.basic_info.name().to_owned(),
        num_children: Some(group.fields.len() as i32),
        converted_type: group.basic_info.converted_type().into(),
        scale: None,
        precision: None,
        field_id: if group.basic_info.has_id() {
            Some(group.basic_info.id())
        } else {
            None
        },
        logical_type: group.basic_info.logical_type().map(|value| value.into()),
    };

    elements.push(element);

    // Add child elements for a group
    for field in &group.fields {
        type_to_thrift(field, elements);
    }
}

fn primitive_to_thrift(prim: &PrimitiveType, elements: &mut Vec<format::SchemaElement>) {
    let element = format::SchemaElement {
        type_: Some(prim.physical_type.into()),
        type_length: if prim.type_length >= 0 {
            Some(prim.type_length)
        } else {
            None
        },
        repetition_type: Some(prim.basic_info.repetition().into()),
        name: prim.basic_info.name().to_owned(),
        num_children: None,
        converted_type: prim.basic_info.converted_type().into(),
        scale: if prim.scale >= 0 {
            Some(prim.scale)
        } else {
            None
        },
        precision: if prim.precision >= 0 {
            Some(prim.precision)
        } else {
            None
        },
        field_id: if prim.basic_info.has_id() {
            Some(prim.basic_info.id())
        } else {
            None
        },
        logical_type: prim.basic_info.logical_type().map(|value| value.into()),
    };

    elements.push(element);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parser::parse_message_type;

    // TODO: add tests for v2 types

    #[test]
    fn test_primitive_type() {
        let prim = SchemaType::primitive_type_builder("foo", PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 32,
                is_signed: true,
            }))
            .with_id(Some(0))
            .build()
            .unwrap();

        assert_eq!(prim.basic_info.repetition(), Repetition::OPTIONAL);
        assert_eq!(
            prim.basic_info.logical_type(),
            Some(LogicalType::Integer {
                bit_width: 32,
                is_signed: true
            })
        );
        assert_eq!(prim.basic_info.converted_type(), ConvertedType::INT_32);
        assert_eq!(prim.basic_info.id(), 0);
        assert_eq!(prim.physical_type, PhysicalType::INT32);

        // Test illegal inputs with logical type
        let err = SchemaType::primitive_type_builder("foo", PhysicalType::INT64)
            .with_repetition(Repetition::REPEATED)
            .with_logical_type(Some(LogicalType::Integer {
                is_signed: true,
                bit_width: 8,
            }))
            .build()
            .unwrap_err();
        assert!(err.to_string().contains(
            "Cannot annotate Integer { bit_width: 8, is_signed: true } from INT64 for field 'foo'"
        ));

        // Test illegal inputs with converted type
        let err = SchemaType::primitive_type_builder("foo", PhysicalType::INT64)
            .with_repetition(Repetition::REPEATED)
            .with_converted_type(ConvertedType::BSON)
            .build()
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("BSON cannot annotate field 'foo' because it is not a BYTE_ARRAY field")
        );

        let err = SchemaType::primitive_type_builder("foo", PhysicalType::INT96)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(-1)
            .with_scale(-1)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains(
            "DECIMAL can only annotate INT32, INT64, BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY"
        ));

        let err = SchemaType::primitive_type_builder("foo", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::Decimal {
                scale: 32,
                precision: 12,
            }))
            .with_precision(-1)
            .with_scale(-1)
            .build()
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("DECIMAL logical type scale 32 must match self.scale -1 for field 'foo'")
        );

        let err = SchemaType::primitive_type_builder("foo", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(-1)
            .with_scale(-1)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("Invalid DECIMAL precision: -1"));

        let err = SchemaType::primitive_type_builder("foo", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(0)
            .with_scale(-1)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("Invalid DECIMAL precision: 0"));

        let err = SchemaType::primitive_type_builder("foo", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(1)
            .with_scale(-1)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("Invalid DECIMAL scale: -1"));

        let err = SchemaType::primitive_type_builder("foo", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(1)
            .with_scale(2)
            .build()
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid DECIMAL: scale (2) cannot be greater than precision (1)")
        );

        // It is OK if precision == scale
        let _prim = SchemaType::primitive_type_builder("foo", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(1)
            .with_scale(1)
            .build()
            .unwrap();

        let err = SchemaType::primitive_type_builder("foo", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(18)
            .with_scale(2)
            .build()
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot represent INT32 as DECIMAL with precision 18")
        );

        let err = SchemaType::primitive_type_builder("foo", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(32)
            .with_scale(2)
            .build()
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot represent INT64 as DECIMAL with precision 32")
        );

        let err = SchemaType::primitive_type_builder("foo", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_length(5)
            .with_precision(12)
            .with_scale(2)
            .build()
            .unwrap_err();
        assert!(
                err.to_string().contains(
                    "Cannot represent FIXED_LEN_BYTE_ARRAY as DECIMAL with length 5 and precision 12. The max precision can only be 11")
            );

        let err = SchemaType::primitive_type_builder("foo", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::UINT_8)
            .build()
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("UINT_8 cannot annotate field 'foo' because it is not a INT32 field")
        );

        let err = SchemaType::primitive_type_builder("foo", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::TIME_MICROS)
            .build()
            .unwrap_err();
        assert!(
            err.to_string().contains(
                "TIME_MICROS cannot annotate field 'foo' because it is not a INT64 field"
            )
        );

        let err = SchemaType::primitive_type_builder("foo", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::INTERVAL)
            .build()
            .unwrap_err();
        assert!(
                err.to_string().contains(
                    "INTERVAL cannot annotate field 'foo' because it is not a FIXED_LEN_BYTE_ARRAY(12) field")
            );

        let err = SchemaType::primitive_type_builder("foo", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::INTERVAL)
            .with_length(1)
            .build()
            .unwrap_err();
        assert!(
                err.to_string().contains(
                    "INTERVAL cannot annotate field 'foo' because it is not a FIXED_LEN_BYTE_ARRAY(12) field")
            );

        let err = SchemaType::primitive_type_builder("foo", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::ENUM)
            .build()
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("ENUM cannot annotate field 'foo' because it is not a BYTE_ARRAY field")
        );

        let err = SchemaType::primitive_type_builder("foo", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::MAP)
            .build()
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("MAP cannot be applied to primitive field 'foo'")
        );

        let err = SchemaType::primitive_type_builder("foo", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_length(-1)
            .build()
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid FIXED_LEN_BYTE_ARRAY length: -1 for field 'foo'")
        );

        let _prim = SchemaType::primitive_type_builder("foo", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::Float16))
            .with_length(2)
            .build()
            .unwrap();

        // Can't be other than FIXED_LEN_BYTE_ARRAY for physical type
        let err = SchemaType::primitive_type_builder("foo", PhysicalType::FLOAT)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::Float16))
            .with_length(2)
            .build()
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot annotate Float16 from FLOAT for field 'foo'")
        );

        // Must have length 2
        let err = SchemaType::primitive_type_builder("foo", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::Float16))
            .with_length(4)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains(
            "FLOAT16 cannot annotate field 'foo' because it is not a FIXED_LEN_BYTE_ARRAY(2) field"
        ));

        // Must have length 16
        let err = SchemaType::primitive_type_builder("foo", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::Uuid))
            .with_length(15)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains(
            "UUID cannot annotate field 'foo' because it is not a FIXED_LEN_BYTE_ARRAY(16) field"
        ));
    }

    #[test]
    fn test_group_type() {
        let f1 = SchemaType::primitive_type_builder("f1", PhysicalType::INT32)
            .with_converted_type(ConvertedType::INT_32)
            .with_id(Some(0))
            .build();
        assert!(f1.is_ok());
        let f2 = SchemaType::primitive_type_builder("f2", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_id(Some(1))
            .build();
        assert!(f2.is_ok());

        let fields = vec![Arc::new(f1.unwrap()).into(), Arc::new(f2.unwrap()).into()];

        let group = SchemaType::group_type_builder("foo")
            .with_repetition(Repetition::REPEATED)
            .with_logical_type(Some(LogicalType::List))
            .with_fields(fields)
            .with_id(Some(1))
            .build()
            .unwrap();

        assert_eq!(group.basic_info.repetition(), Repetition::REPEATED);
        assert_eq!(group.basic_info.logical_type(), Some(LogicalType::List));
        assert_eq!(group.basic_info.converted_type(), ConvertedType::LIST);
        assert_eq!(group.basic_info.id(), 1);
        assert_eq!(group.fields.len(), 2);
        assert_eq!(group.fields[0].name(), "f1");
        assert_eq!(group.fields[1].name(), "f2");
    }

    #[test]
    fn test_column_descriptor() {
        let result = test_column_descriptor_helper();
        assert!(
            result.is_ok(),
            "Expected result to be OK but got err:\n {}",
            result.unwrap_err()
        );
    }

    fn test_column_descriptor_helper() -> Result<()> {
        let tp = SchemaType::primitive_type_builder("name", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .build()?;

        let descr = ColumnDescriptor::new(Arc::new(tp), 4, 1, Arc::new(ColumnPath::from("name")));

        assert_eq!(descr.path.as_ref(), &ColumnPath::from("name"));
        assert_eq!(descr.converted_type(), ConvertedType::UTF8);
        assert_eq!(descr.physical_type(), PhysicalType::BYTE_ARRAY);
        assert_eq!(descr.max_def_level, 4);
        assert_eq!(descr.max_rep_level, 1);
        assert_eq!(descr.name(), "name");
        assert_eq!(descr.type_length(), -1);
        assert_eq!(descr.primitive_type.precision, -1);
        assert_eq!(descr.primitive_type.scale, -1);

        Ok(())
    }

    #[test]
    fn test_schema_descriptor() {
        let result = test_schema_descriptor_helper();
        assert!(
            result.is_ok(),
            "Expected result to be OK but got err:\n {}",
            result.unwrap_err()
        );
    }

    // A helper fn to avoid handling the results from type creation
    fn test_schema_descriptor_helper() -> Result<()> {
        let mut fields = vec![];

        let inta = SchemaType::primitive_type_builder("a", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::INT_32)
            .build()?;
        fields.push(Arc::new(inta).into());
        let intb = SchemaType::primitive_type_builder("b", PhysicalType::INT64)
            .with_converted_type(ConvertedType::INT_64)
            .build()?;
        fields.push(Arc::new(intb).into());
        let intc = SchemaType::primitive_type_builder("c", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REPEATED)
            .with_converted_type(ConvertedType::UTF8)
            .build()?;
        fields.push(Arc::new(intc).into());

        // 3-level list encoding
        let item1 = SchemaType::primitive_type_builder("item1", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::INT_64)
            .build()?;
        let item2 = SchemaType::primitive_type_builder("item2", PhysicalType::BOOLEAN).build()?;
        let item3 = SchemaType::primitive_type_builder("item3", PhysicalType::INT32)
            .with_repetition(Repetition::REPEATED)
            .with_converted_type(ConvertedType::INT_32)
            .build()?;
        let list = SchemaType::group_type_builder("records")
            .with_repetition(Repetition::REPEATED)
            .with_converted_type(ConvertedType::LIST)
            .with_fields(vec![
                Arc::new(item1).into(),
                Arc::new(item2).into(),
                Arc::new(item3).into(),
            ])
            .build()?;
        let bag = SchemaType::group_type_builder("bag")
            .with_repetition(Repetition::OPTIONAL)
            .with_fields(vec![Arc::new(list).into()])
            .build()?;
        fields.push(Arc::new(bag).into());

        let schema = SchemaType::group_type_builder("schema")
            .with_repetition(Repetition::REPEATED)
            .with_fields(fields)
            .build()?;
        let descr = SchemaDescriptor::new(Arc::new(schema));

        let nleaves = 6;
        assert_eq!(descr.num_columns(), nleaves);

        //                             mdef mrep
        // required int32 a            0    0
        // optional int64 b            1    0
        // repeated byte_array c       1    1
        // optional group bag          1    0
        //   repeated group records    2    1
        //     required int64 item1    2    1
        //     optional boolean item2  3    1
        //     repeated int32 item3    3    2
        let ex_max_def_levels = [0, 1, 1, 2, 3, 3];
        let ex_max_rep_levels = [0, 0, 1, 1, 1, 2];

        for i in 0..nleaves {
            let col = descr.column(i);
            assert_eq!(col.max_def_level, ex_max_def_levels[i], "{i}");
            assert_eq!(col.max_rep_level, ex_max_rep_levels[i], "{i}");
        }

        assert_eq!(descr.column(0).path.to_string(), "a");
        assert_eq!(descr.column(1).path.to_string(), "b");
        assert_eq!(descr.column(2).path.to_string(), "c");
        assert_eq!(descr.column(3).path.to_string(), "bag.records.item1");
        assert_eq!(descr.column(4).path.to_string(), "bag.records.item2");
        assert_eq!(descr.column(5).path.to_string(), "bag.records.item3");

        assert_eq!(descr.get_column_root(0).name(), "a");
        assert_eq!(descr.get_column_root(3).name(), "bag");
        assert_eq!(descr.get_column_root(4).name(), "bag");
        assert_eq!(descr.get_column_root(5).name(), "bag");

        Ok(())
    }

    #[test]
    fn test_schema_build_tree_def_rep_levels() {
        let message_type = "
    message spark_schema {
      REQUIRED INT32 a;
      OPTIONAL group b {
        OPTIONAL INT32 _1;
        OPTIONAL INT32 _2;
      }
      OPTIONAL group c (LIST) {
        REPEATED group list {
          OPTIONAL INT32 element;
        }
      }
    }
    ";
        let schema = parse_message_type(message_type).expect("should parse schema");
        let descr = SchemaDescriptor::new(Arc::new(schema));
        // required int32 a
        assert_eq!(descr.column(0).max_def_level, 0);
        assert_eq!(descr.column(0).max_rep_level, 0);
        // optional int32 b._1
        assert_eq!(descr.column(1).max_def_level, 2);
        assert_eq!(descr.column(1).max_rep_level, 0);
        // optional int32 b._2
        assert_eq!(descr.column(2).max_def_level, 2);
        assert_eq!(descr.column(2).max_rep_level, 0);
        // repeated optional int32 c.list.element
        assert_eq!(descr.column(3).max_def_level, 3);
        assert_eq!(descr.column(3).max_rep_level, 1);
    }

    #[test]
    fn test_get_physical_type_primitive() {
        let f = SchemaType::primitive_type_builder("f", PhysicalType::INT64)
            .build()
            .unwrap();
        assert_eq!(f.physical_type, PhysicalType::INT64);

        let f = SchemaType::primitive_type_builder("f", PhysicalType::BYTE_ARRAY)
            .build()
            .unwrap();
        assert_eq!(f.physical_type, PhysicalType::BYTE_ARRAY);
    }

    #[test]
    fn test_schema_type_thrift_conversion() {
        let message_type = "
    message conversions {
      REQUIRED INT64 id;
      OPTIONAL FIXED_LEN_BYTE_ARRAY (2) f16 (FLOAT16);
      OPTIONAL group int_array_Array (LIST) {
        REPEATED group list {
          OPTIONAL group element (LIST) {
            REPEATED group list {
              OPTIONAL INT32 element;
            }
          }
        }
      }
      OPTIONAL group int_map (MAP) {
        REPEATED group map (MAP_KEY_VALUE) {
          REQUIRED BYTE_ARRAY key (UTF8);
          OPTIONAL INT32 value;
        }
      }
      OPTIONAL group int_Map_Array (LIST) {
        REPEATED group list {
          OPTIONAL group g (MAP) {
            REPEATED group map (MAP_KEY_VALUE) {
              REQUIRED BYTE_ARRAY key (UTF8);
              OPTIONAL group value {
                OPTIONAL group H {
                  OPTIONAL group i (LIST) {
                    REPEATED group list {
                      OPTIONAL DOUBLE element;
                    }
                  }
                }
              }
            }
          }
        }
      }
      OPTIONAL group nested_struct {
        OPTIONAL INT32 A;
        OPTIONAL group b (LIST) {
          REPEATED group list {
            REQUIRED FIXED_LEN_BYTE_ARRAY (16) element;
          }
        }
      }
    }
    ";
        let expected_schema = parse_message_type(message_type).unwrap();
        let thrift_schema = schema_to_thrift(&expected_schema).unwrap();
        let result_schema = schema_from_thrift(&thrift_schema).unwrap();
        assert_eq!(result_schema.as_ref(), &expected_schema);
    }

    #[test]
    fn test_schema_type_thrift_conversion_decimal() {
        let message_type = "
    message decimals {
      OPTIONAL INT32 field0;
      OPTIONAL INT64 field1 (DECIMAL (18, 2));
      OPTIONAL FIXED_LEN_BYTE_ARRAY (16) field2 (DECIMAL (38, 18));
      OPTIONAL BYTE_ARRAY field3 (DECIMAL (9));
    }
    ";
        let expected_schema = parse_message_type(message_type).unwrap();
        let thrift_schema = schema_to_thrift(&expected_schema).unwrap();
        let result_schema = schema_from_thrift(&thrift_schema).unwrap();
        assert_eq!(result_schema.as_ref(), &expected_schema);
    }

    // Tests schema conversion from thrift, when num_children is set to Some(0) for a
    // primitive type.
    #[test]
    fn test_schema_from_thrift_with_num_children_set() {
        // schema definition written by parquet-cpp version 1.3.2-SNAPSHOT
        let message_type = "
    message schema {
      OPTIONAL BYTE_ARRAY id (UTF8);
      OPTIONAL BYTE_ARRAY name (UTF8);
      OPTIONAL BYTE_ARRAY message (UTF8);
      OPTIONAL INT32 type (UINT_8);
      OPTIONAL INT64 author_time (TIMESTAMP_MILLIS);
      OPTIONAL INT64 __index_level_0__;
    }
    ";

        let expected_schema = parse_message_type(message_type).unwrap();
        let mut thrift_schema = schema_to_thrift(&expected_schema).unwrap();
        // Change all of None to Some(0)
        for elem in &mut thrift_schema[..] {
            if elem.num_children.is_none() {
                elem.num_children = Some(0);
            }
        }

        let result_schema = schema_from_thrift(&thrift_schema).unwrap();
        assert_eq!(result_schema.as_ref(), &expected_schema);
    }

    // Sometimes parquet-cpp sets repetition level for the root node, which is against
    // the format definition, but we need to handle it by setting it back to None.
    #[test]
    fn test_schema_from_thrift_root_has_repetition() {
        // schema definition written by parquet-cpp version 1.3.2-SNAPSHOT
        let message_type = "
    message schema {
      OPTIONAL BYTE_ARRAY a (UTF8);
      OPTIONAL INT32 b (UINT_8);
    }
    ";

        let expected_schema = parse_message_type(message_type).unwrap();
        let mut thrift_schema = schema_to_thrift(&expected_schema).unwrap();
        thrift_schema[0].repetition_type = Some(Repetition::REQUIRED.into());

        let result_schema = schema_from_thrift(&thrift_schema).unwrap();
        assert_eq!(result_schema.as_ref(), &expected_schema);
    }

    #[test]
    fn test_schema_from_thrift_group_has_no_child() {
        let message_type = "message schema {}";

        let expected_schema = parse_message_type(message_type).unwrap();
        let mut thrift_schema = schema_to_thrift(&expected_schema).unwrap();
        thrift_schema[0].repetition_type = Some(Repetition::REQUIRED.into());

        let result_schema = schema_from_thrift(&thrift_schema).unwrap();
        assert_eq!(result_schema.as_ref(), &expected_schema);
    }
}
