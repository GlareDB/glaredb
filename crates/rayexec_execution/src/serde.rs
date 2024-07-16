//! Module containing the more tricky bits of database object (de)serialization.
//!
//! Specifically this implements the relevant serde traits for functions (scalar,
//! aggregate, and table functions). Since a point of extension are functions, they
//! need remain as boxed traits to enable that.
//!
//! The strategy we take here is since every function is registered into the system
//! catalog, we can use that as a central registry for reconstructing functions from
//! serialized objects. For unplanned functions, we're able to serialize just the
//! name, which we then look up in the catalog when deserializing. For planned
//! functions with potentially some state, we serialized the name of the function
//! along with its state. When deserializing, we look up the function from the
//! catalog (which will return and unplanned function) which then is able continue
//! deserialization of the planned function state.
//!
//! Currently this assumes that all functions are registered in the
//! 'glare_catalog' schema.
//!
//! This implementation takes inspiration from the `typetag` crate (specifically the
//! externally tagged variant). See <https://github.com/dtolnay/typetag/blob/dc3f6d4ec6672945a024c105c91958c1e8fcacd5/src/externally.rs>
use rayexec_error::{RayexecError, Result};
use serde::{
    de::{self, DeserializeSeed, Deserializer, Visitor},
    ser::SerializeMap,
    Serialize, Serializer,
};
use std::fmt;

use crate::{
    database::{catalog::CatalogTx, DatabaseContext},
    functions::{
        aggregate::{AggregateFunction, PlannedAggregateFunction},
        scalar::{PlannedScalarFunction, ScalarFunction},
        table::{PlannedTableFunction, TableFunction},
    },
};

trait ObjectLookup: Copy + 'static {
    /// Object we're looking up that exists in the catalog.
    type Object: ?Sized + 'static;
    /// The stateful version of the object. This should be the "planned" version
    /// of the object.
    type StatefulObject: ?Sized + 'static;

    /// Lookup an object by name in the catalog.
    fn lookup(&self, context: &DatabaseContext, name: &str) -> Result<Box<Self::Object>>;

    /// Deserialize into a stateful object based on the object we provide.
    fn state_deserialize(
        &self,
        object: Box<Self::Object>,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<Self::StatefulObject>>;
}

#[derive(Debug, Clone, Copy)]
struct ScalarFunctionLookup;

impl ObjectLookup for ScalarFunctionLookup {
    type Object = dyn ScalarFunction;
    type StatefulObject = dyn PlannedScalarFunction;

    fn lookup(&self, context: &DatabaseContext, name: &str) -> Result<Box<Self::Object>> {
        let tx = CatalogTx::new();
        let func = context
            .system_catalog()?
            .get_scalar_fn(&tx, "glare_catalog", name)?
            .ok_or_else(|| RayexecError::new(format!("Missing function for '{name}'")))?;

        Ok(func)
    }

    fn state_deserialize(
        &self,
        object: Box<Self::Object>,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<Self::StatefulObject>> {
        object.state_deserialize(deserializer)
    }
}

#[derive(Debug, Clone, Copy)]
struct AggregateFunctionLookup;

impl ObjectLookup for AggregateFunctionLookup {
    type Object = dyn AggregateFunction;
    type StatefulObject = dyn PlannedAggregateFunction;

    fn lookup(&self, context: &DatabaseContext, name: &str) -> Result<Box<Self::Object>> {
        let tx = CatalogTx::new();
        let func = context
            .system_catalog()?
            .get_aggregate_fn(&tx, "glare_catalog", name)?
            .ok_or_else(|| RayexecError::new(format!("Missing function for '{name}'")))?;

        Ok(func)
    }

    fn state_deserialize(
        &self,
        object: Box<Self::Object>,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<Self::StatefulObject>> {
        object.state_deserialize(deserializer)
    }
}

#[derive(Debug, Clone, Copy)]
struct TableFunctionLookup;

impl ObjectLookup for TableFunctionLookup {
    type Object = dyn TableFunction;
    type StatefulObject = dyn PlannedTableFunction;

    fn lookup(&self, context: &DatabaseContext, name: &str) -> Result<Box<Self::Object>> {
        let tx = CatalogTx::new();
        let func = context
            .system_catalog()?
            .get_table_fn(&tx, "glare_catalog", name)?
            .ok_or_else(|| RayexecError::new(format!("Missing function for '{name}'")))?;

        Ok(func)
    }

    fn state_deserialize(
        &self,
        object: Box<Self::Object>,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<Self::StatefulObject>> {
        object.state_deserialize(deserializer)
    }
}

/// A serde visitor implementation that visits a string and returns a database
/// object.
struct DatabaseObjectVisitor<'a, T: ObjectLookup> {
    context: &'a DatabaseContext,
    lookup: T,
}

impl<'de, 'a, T: ObjectLookup> Visitor<'de> for DatabaseObjectVisitor<'a, T> {
    type Value = Box<T::Object>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        // This Visitor expects to receive ...
        write!(formatter, "a database object name")
    }

    fn visit_str<E>(self, name: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.lookup
            .lookup(self.context, name)
            .map_err(de::Error::custom)
    }
}

impl<'de, 'a, T: ObjectLookup> DeserializeSeed<'de> for DatabaseObjectVisitor<'a, T> {
    type Value = Box<T::Object>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(self)
    }
}

struct TaggedVisitor<'a, T: ObjectLookup> {
    context: &'a DatabaseContext,
    lookup: T,
}

impl<'de, 'a, T: ObjectLookup> Visitor<'de> for TaggedVisitor<'a, T> {
    type Value = Box<T::StatefulObject>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a tagged object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let visitor = DatabaseObjectVisitor {
            context: self.context,
            lookup: self.lookup,
        };

        let object = match map.next_key_seed(visitor)? {
            Some(scalar) => scalar,
            None => {
                return Err(de::Error::custom("missing key"));
            }
        };

        map.next_value_seed(StateDeserializer {
            object,
            lookup: self.lookup,
        })
    }
}

struct StateDeserializer<T: ObjectLookup> {
    object: Box<T::Object>,
    lookup: T,
}

impl<'de, T: ObjectLookup> DeserializeSeed<'de> for StateDeserializer<T> {
    type Value = Box<T::StatefulObject>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut erased = <dyn erased_serde::Deserializer>::erase(deserializer);
        self.lookup
            .state_deserialize(self.object, &mut erased)
            .map_err(de::Error::custom)
    }
}

impl Serialize for Box<dyn ScalarFunction + '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.name().serialize(serializer)
    }
}

pub struct ScalarFunctionDeserializer<'a> {
    context: &'a DatabaseContext,
}

impl<'de, 'a> DeserializeSeed<'de> for ScalarFunctionDeserializer<'a> {
    type Value = Box<dyn ScalarFunction>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(DatabaseObjectVisitor {
            context: self.context,
            lookup: ScalarFunctionLookup,
        })
    }
}

impl Serialize for Box<dyn PlannedScalarFunction + '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut ser = serializer.serialize_map(Some(1))?;
        ser.serialize_entry(self.scalar_function().name(), self.serializable_state())?;
        ser.end()
    }
}

pub struct PlannedScalarFunctionDeserializer<'a> {
    context: &'a DatabaseContext,
}

impl<'de, 'a> DeserializeSeed<'de> for PlannedScalarFunctionDeserializer<'a> {
    type Value = Box<dyn PlannedScalarFunction>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(TaggedVisitor {
            context: self.context,
            lookup: ScalarFunctionLookup,
        })
    }
}

impl Serialize for Box<dyn AggregateFunction + '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.name().serialize(serializer)
    }
}

pub struct AggregateFunctionDeserializer<'a> {
    context: &'a DatabaseContext,
}

impl<'de, 'a> DeserializeSeed<'de> for AggregateFunctionDeserializer<'a> {
    type Value = Box<dyn AggregateFunction>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(DatabaseObjectVisitor {
            context: self.context,
            lookup: AggregateFunctionLookup,
        })
    }
}

impl Serialize for Box<dyn PlannedAggregateFunction + '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut ser = serializer.serialize_map(Some(1))?;
        ser.serialize_entry(self.aggregate_function().name(), self.serializable_state())?;
        ser.end()
    }
}

pub struct PlannedAggregateFunctionDeserializer<'a> {
    context: &'a DatabaseContext,
}

impl<'de, 'a> DeserializeSeed<'de> for PlannedAggregateFunctionDeserializer<'a> {
    type Value = Box<dyn PlannedAggregateFunction>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(TaggedVisitor {
            context: self.context,
            lookup: AggregateFunctionLookup,
        })
    }
}

impl Serialize for Box<dyn TableFunction + '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.name().serialize(serializer)
    }
}

pub struct TableFunctionDeserializer<'a> {
    context: &'a DatabaseContext,
}

impl<'de, 'a> DeserializeSeed<'de> for TableFunctionDeserializer<'a> {
    type Value = Box<dyn TableFunction>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(DatabaseObjectVisitor {
            context: self.context,
            lookup: TableFunctionLookup,
        })
    }
}

impl Serialize for Box<dyn PlannedTableFunction + '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut ser = serializer.serialize_map(Some(1))?;
        ser.serialize_entry(self.table_function().name(), self.serializable_state())?;
        ser.end()
    }
}

pub struct PlannedTableFunctionDeserializer<'a> {
    context: &'a DatabaseContext,
}

impl<'de, 'a> DeserializeSeed<'de> for PlannedTableFunctionDeserializer<'a> {
    type Value = Box<dyn PlannedTableFunction>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(TaggedVisitor {
            context: self.context,
            lookup: TableFunctionLookup,
        })
    }
}
