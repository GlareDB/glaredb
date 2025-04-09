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

use glaredb_error::{DbError, Result};

use super::types::{GroupType, PrimitiveType};
use crate::basic::{ConvertedType, Repetition};
use crate::schema::types::Type;

/// A utility trait to help user to traverse against parquet type.
pub trait TypeVisitor<R, C> {
    /// Called when a primitive type hit.
    fn visit_primitive(&mut self, primitive_type: &PrimitiveType, context: C) -> Result<R>;

    /// Default implementation when visiting a list.
    ///
    /// It checks list type definition and calls [`Self::visit_list_with_item`] with extracted
    /// item type.
    ///
    /// To fully understand this algorithm, please refer to
    /// [parquet doc](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md).
    ///
    /// For example, a standard list type looks like:
    ///
    /// ```text
    /// required/optional group my_list (LIST) {
    ///    repeated group list {
    ///      required/optional binary element (UTF8);
    ///    }
    ///  }
    /// ```
    ///
    /// In such a case, [`Self::visit_list_with_item`] will be called with `my_list` as the list
    /// type, and `element` as the `item_type`
    fn visit_list(&mut self, list_type: &GroupType, context: C) -> Result<R> {
        if list_type.fields.len() != 1 {
            return Err(DbError::new(
                "Group element type of list can only contain one field.",
            ));
        }

        let list_item = list_type.fields.first().unwrap();
        match list_item {
            Type::PrimitiveType(_) => {
                if list_item.get_basic_info().repetition() == Repetition::REPEATED {
                    self.visit_list_with_item(list_type, list_item, context)
                } else {
                    Err(DbError::new(
                        "Primitive element type of list must be repeated.",
                    ))
                }
            }
            Type::GroupType(group) => {
                if group.fields.len() == 1
                    && list_item.name() != "array"
                    && list_item.name() != format!("{}_tuple", list_type.basic_info.name())
                {
                    self.visit_list_with_item(list_type, group.fields.first().unwrap(), context)
                } else {
                    self.visit_list_with_item(list_type, list_item, context)
                }
            }
        }
    }

    /// Called when a struct type hit.
    fn visit_struct(&mut self, struct_type: &GroupType, context: C) -> Result<R>;

    /// Called when a map type hit.
    fn visit_map(&mut self, map_type: &GroupType, context: C) -> Result<R>;

    /// A utility method which detects input type and calls corresponding method.
    fn dispatch(&mut self, cur_type: &Type, context: C) -> Result<R> {
        match cur_type {
            Type::PrimitiveType(prim) => self.visit_primitive(prim, context),
            Type::GroupType(group) => match group.basic_info.converted_type() {
                ConvertedType::LIST => self.visit_list(group, context),
                ConvertedType::MAP | ConvertedType::MAP_KEY_VALUE => self.visit_map(group, context),
                _ => self.visit_struct(group, context),
            },
        }
    }

    /// Called by `visit_list`.
    // TODO: Change `item_type` to `PrimitiveType`?
    fn visit_list_with_item(
        &mut self,
        list_type: &GroupType,
        item_type: &Type,
        context: C,
    ) -> Result<R>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::basic::Type as PhysicalType;
    use crate::schema::parser::parse_message_type;

    struct TestVisitorContext {}
    struct TestVisitor<'a> {
        primitive_visited: bool,
        struct_visited: bool,
        list_visited: bool,
        root_type: &'a GroupType,
    }

    impl TypeVisitor<bool, TestVisitorContext> for TestVisitor<'_> {
        fn visit_primitive(
            &mut self,
            primitive_type: &PrimitiveType,
            _context: TestVisitorContext,
        ) -> Result<bool> {
            match self.get_field_by_name(primitive_type.basic_info.name()) {
                Type::PrimitiveType(field) => assert_eq!(field.as_ref(), primitive_type),
                other => panic!("other: {other:?}"),
            };
            self.primitive_visited = true;
            Ok(true)
        }

        fn visit_struct(
            &mut self,
            struct_type: &GroupType,
            _context: TestVisitorContext,
        ) -> Result<bool> {
            match self.get_field_by_name(struct_type.basic_info.name()) {
                Type::GroupType(group) => assert_eq!(group.as_ref(), struct_type),
                other => panic!("other: {other:?}"),
            };
            self.struct_visited = true;
            Ok(true)
        }

        fn visit_map(
            &mut self,
            _map_type: &GroupType,
            _context: TestVisitorContext,
        ) -> Result<bool> {
            unimplemented!()
        }

        fn visit_list_with_item(
            &mut self,
            list_type: &GroupType,
            item_type: &Type,
            _context: TestVisitorContext,
        ) -> Result<bool> {
            match self.get_field_by_name(list_type.basic_info.name()) {
                Type::GroupType(group) => assert_eq!(group.as_ref(), list_type),
                other => panic!("other: {other:?}"),
            };

            assert_eq!("element", item_type.name());
            assert_eq!(PhysicalType::INT32, item_type.get_physical_type());
            self.list_visited = true;
            Ok(true)
        }
    }

    impl<'a> TestVisitor<'a> {
        fn new(root: &'a GroupType) -> Self {
            Self {
                primitive_visited: false,
                struct_visited: false,
                list_visited: false,
                root_type: root,
            }
        }

        fn get_field_by_name(&self, name: &str) -> &Type {
            self.root_type
                .fields
                .iter()
                .find(|t| t.name() == name)
                .unwrap()
        }
    }

    #[test]
    fn test_visitor() {
        let message_type = "
          message spark_schema {
            REQUIRED INT32 a;
            OPTIONAL group inner_schema {
              REQUIRED INT32 b;
              REQUIRED DOUBLE c;
            }

            OPTIONAL group e (LIST) {
              REPEATED group list {
                REQUIRED INT32 element;
              }
            }
        ";

        let parquet_type = Arc::new(parse_message_type(message_type).unwrap());

        let mut visitor = TestVisitor::new(&parquet_type);
        for f in &parquet_type.fields {
            let c = TestVisitorContext {};
            assert!(visitor.dispatch(f, c).unwrap());
        }

        assert!(visitor.struct_visited);
        assert!(visitor.primitive_visited);
        assert!(visitor.list_visited);
    }
}
