use crate::errors::{internal, CatalogError, Result};
use crate::filter::filter_scan;
use crate::system::constants::{ATTRIBUTES_TABLE_ID, ATTRIBUTES_TABLE_NAME};
use crate::system::{SystemSchema, SystemTable, SystemTableAccessor, SYSTEM_SCHEMA_ID};
use access::runtime::AccessRuntime;
use access::strategy::SinglePartitionStrategy;
use access::table::PartitionedTable;
use catalog_types::context::SessionContext;
use catalog_types::datatypes::{arrow_type_for_type_id, type_id_for_arrow_type, TypeId};
use catalog_types::interfaces::MutableTableProvider;
use catalog_types::keys::{SchemaId, TableId, TableKey};
use datafusion::arrow::array::{
    BooleanArray, BooleanBuilder, Int8Array, Int8Builder, StringArray, StringBuilder, UInt32Array,
    UInt32Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::Expr;
use futures::TryStreamExt;
use itertools::izip;
use std::sync::Arc;

pub struct AttributesTable {
    schema: SchemaRef,
}

impl AttributesTable {
    pub fn new() -> AttributesTable {
        AttributesTable {
            schema: Arc::new(Schema::new(vec![
                Field::new("schema_id", DataType::UInt32, false),
                Field::new("table_id", DataType::UInt32, false),
                Field::new("attr_name", DataType::Utf8, false),
                Field::new("attr_num", DataType::Int8, false),
                Field::new("datatype", DataType::UInt32, false),
                Field::new("nullable", DataType::Boolean, false),
            ])),
        }
    }
}

impl SystemTableAccessor for AttributesTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn name(&self) -> &'static str {
        ATTRIBUTES_TABLE_NAME
    }

    fn is_readonly(&self) -> bool {
        false
    }

    fn get_table(&self, runtime: Arc<AccessRuntime>) -> SystemTable {
        let key = TableKey {
            schema_id: SYSTEM_SCHEMA_ID,
            table_id: ATTRIBUTES_TABLE_ID,
        };

        SystemTable::Base(PartitionedTable::new(
            key,
            Box::new(SinglePartitionStrategy),
            runtime,
            self.schema.clone(),
        ))
    }
}

#[derive(Debug)]
pub struct AttributeRow {
    pub schema_id: SchemaId,
    pub table_id: TableId,
    pub attr_name: String,
    pub attr_num: i8,
    pub datatype: TypeId,
    pub nullable: bool,
}

/// Wrapper around a vector of attributes.
#[derive(Debug)]
pub struct AttributeRows(pub Vec<AttributeRow>);

impl AttributeRows {
    /// Scan attributes for a table.
    pub async fn scan_for_table(
        ctx: &SessionContext,
        runtime: &Arc<AccessRuntime>,
        system: &SystemSchema,
        schema: SchemaId,
        table: TableId,
    ) -> Result<Self> {
        let schemas_table = system
            .get_system_table_accessor(ATTRIBUTES_TABLE_NAME)
            .ok_or_else(|| CatalogError::MissingSystemTable(ATTRIBUTES_TABLE_NAME.to_string()))?
            .get_table(runtime.clone());
        let partitioned_table = schemas_table.into_table_provider_ref();

        // Filter for this table.
        let filter = Expr::Column(Column::from_name("schema_id"))
            .eq(Expr::Literal(ScalarValue::UInt32(Some(schema))))
            .and(
                Expr::Column(Column::from_name("table_id"))
                    .eq(Expr::Literal(ScalarValue::UInt32(Some(table)))),
            );

        let plan = filter_scan(partitioned_table, ctx.get_df_state(), &[filter], None).await?;
        let stream = plan.execute(0, ctx.task_context())?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        let mut attrs: Vec<AttributeRow> =
            Vec::with_capacity(batches.iter().fold(0, |acc, batch| acc + batch.num_rows()));
        for batch in batches {
            let schema_ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| internal!("failed to downcast schema ids to uint32 array"))?;
            let table_ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| internal!("failed to downcast table ids to uint32 array"))?;
            let attr_names = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| internal!("failed to downcast attr names to string array"))?;
            let attr_nums = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| internal!("failed to downcast attr nums to int8 array"))?;
            let datatypes = batch
                .column(4)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| internal!("failed to downcast datatype ids to uint32 array"))?;
            let nullables = batch
                .column(5)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| internal!("failed to downcast nullables to bool array"))?;

            for (schema_id, table_id, attr_name, attr_num, datatype, nullable) in
                izip!(schema_ids, table_ids, attr_names, attr_nums, datatypes, nullables,)
            {
                let schema_id = match schema_id {
                    Some(schema_id) => schema_id,
                    None => return Err(internal!("unexpected null value for schema id")),
                };
                let table_id = match table_id {
                    Some(table_id) => table_id,
                    None => return Err(internal!("unexpected null value for table id")),
                };
                let attr_name = match attr_name {
                    Some(attr_name) => attr_name.to_string(),
                    None => return Err(internal!("unexpected null value for attr name")),
                };
                let attr_num = match attr_num {
                    Some(attr_num) => attr_num,
                    None => return Err(internal!("unexpected null value for attr num")),
                };
                let datatype = match datatype {
                    Some(datatype) => datatype,
                    None => return Err(internal!("unexpected null value for datatype")),
                };
                let nullable = match nullable {
                    Some(nullable) => nullable,
                    None => return Err(internal!("unexpected null value for nullable")),
                };

                attrs.push(AttributeRow {
                    schema_id,
                    table_id,
                    attr_name,
                    attr_num,
                    datatype,
                    nullable,
                });
            }
        }

        Ok(AttributeRows(attrs))
    }

    /// Insert all attributes into the system attributes table.
    pub async fn insert_all(
        &self,
        ctx: &SessionContext,
        runtime: &Arc<AccessRuntime>,
        system: &SystemSchema,
    ) -> Result<()> {
        let accessor = system
            .get_system_table_accessor(ATTRIBUTES_TABLE_NAME)
            .ok_or_else(|| CatalogError::MissingSystemTable(ATTRIBUTES_TABLE_NAME.to_string()))?;
        let schemas_table = accessor.get_table(runtime.clone());
        let partitioned_table = schemas_table.get_partitioned_table()?;

        let cap = self.0.len();
        let name_cap = self
            .0
            .iter()
            .fold(0, |acc, attr| acc + attr.attr_name.len());

        let mut schema_ids = UInt32Builder::with_capacity(cap);
        let mut table_ids = UInt32Builder::with_capacity(cap);
        let mut attr_names = StringBuilder::with_capacity(cap, name_cap);
        let mut attr_nums = Int8Builder::with_capacity(cap);
        let mut datatypes = UInt32Builder::with_capacity(cap);
        let mut nullables = BooleanBuilder::with_capacity(cap);

        for attr in self.0.iter() {
            schema_ids.append_value(attr.schema_id);
            table_ids.append_value(attr.table_id);
            attr_names.append_value(&attr.attr_name);
            attr_nums.append_value(attr.attr_num);
            datatypes.append_value(attr.datatype);
            nullables.append_value(attr.nullable);
        }

        let batch = RecordBatch::try_new(
            accessor.schema(),
            vec![
                Arc::new(schema_ids.finish()),
                Arc::new(table_ids.finish()),
                Arc::new(attr_names.finish()),
                Arc::new(attr_nums.finish()),
                Arc::new(datatypes.finish()),
                Arc::new(nullables.finish()),
            ],
        )?;

        partitioned_table.insert(ctx, batch).await?;

        Ok(())
    }

    /// Try to convert an arrow schema into attribute rows for a given table.
    ///
    /// The order of the fields in the arrow schema determines the order of
    /// table attributes.
    pub fn try_from_arrow_schema(
        schema_id: SchemaId,
        table_id: TableId,
        arrow_schema: &Schema,
    ) -> Result<Self> {
        let mut attrs = Vec::with_capacity(arrow_schema.fields.len());
        for (num, field) in (0..).zip(arrow_schema.fields.iter()) {
            let type_id = type_id_for_arrow_type(field.data_type())
                .ok_or_else(|| internal!("invalid arrow data type: {}", field.data_type()))?;
            let attr = AttributeRow {
                schema_id,
                table_id,
                attr_name: field.name().to_string(),
                attr_num: num,
                datatype: type_id,
                nullable: field.is_nullable(),
            };
            attrs.push(attr);
        }

        Ok(AttributeRows(attrs))
    }
}

impl From<AttributeRows> for Vec<AttributeRow> {
    fn from(attrs: AttributeRows) -> Self {
        attrs.0
    }
}

/// Try to convert attributes into an arrow schema.
///
/// The resulting schema will have all fields ordered by `attr_num`.
///
/// NOTE: This does not check that all attributes belong to the same table.
impl TryFrom<AttributeRows> for Schema {
    type Error = CatalogError;
    fn try_from(attrs: AttributeRows) -> Result<Self, Self::Error> {
        let mut attrs = attrs.0;
        attrs.sort_by(|a, b| a.attr_num.cmp(&b.attr_num));

        let mut fields = Vec::with_capacity(attrs.len());
        for attr in attrs {
            let arrow_type = arrow_type_for_type_id(attr.datatype)
                .ok_or_else(|| internal!("invalid type id: {}", attr.datatype))?;
            let field = Field::new(&attr.attr_name, arrow_type, attr.nullable);
            fields.push(field);
        }

        Ok(Schema::new(fields))
    }
}
