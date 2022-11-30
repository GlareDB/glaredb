//! System bootstrap steps.
//!
//! While these steps should eventually be stable, they are subject to change
//! during the duration of Alpha.
use crate::errors::Result;
use crate::system::relations::RelationRow;
use crate::system::schemas::SchemaRow;
use crate::system::sequences::SequenceRow;
use crate::system::{
    constants, SystemSchema, PUBLIC_SCHEMA_ID, PUBLIC_SCHEMA_NAME, SYSTEM_SCHEMA_ID,
    SYSTEM_SCHEMA_NAME,
};
use access::runtime::AccessRuntime;
use async_trait::async_trait;
use catalog_types::context::SessionContext;
use std::sync::Arc;

/// Describes a step in the bootstrap process.
///
/// A bootstrap step may have implicit dependencies on other steps, it's on the
/// caller to ensure bootstrap steps are called in order, and that a failure to
/// run one step stops the entire bootstrap sequence.
#[async_trait]
pub trait SystemBootstrapStep: 'static {
    /// A name describing the bootstrap step. This is purely informational.
    fn name(&self) -> &'static str;

    /// Run the bootstrap step to completion.
    async fn run(
        &self,
        ctx: &SessionContext,
        runtime: &Arc<AccessRuntime>,
        system: &SystemSchema,
    ) -> Result<()>;
}

/// "Create" the v0 system tables.
///
/// "Create" in this context means ensuring that the system tables each have an
/// entry in the "relations" table.
#[derive(Debug)]
pub struct CreateV0SystemTables;

#[async_trait]
impl SystemBootstrapStep for CreateV0SystemTables {
    fn name(&self) -> &'static str {
        "create_v0_system_tables"
    }

    async fn run(
        &self,
        ctx: &SessionContext,
        runtime: &Arc<AccessRuntime>,
        system: &SystemSchema,
    ) -> Result<()> {
        // Insert all system tables into the "relations" table. Note that we
        // don't need to create the physical resources for tables since that's
        // done lazily on insert/compaction.

        // Bootstrap
        RelationRow {
            schema_id: SYSTEM_SCHEMA_ID,
            table_id: constants::BOOTSTRAP_TABLE_ID,
            table_name: constants::BOOTSTRAP_TABLE_NAME.to_string(),
        }
        .insert(ctx, runtime, system)
        .await?;

        // Builtin types
        RelationRow {
            schema_id: SYSTEM_SCHEMA_ID,
            table_id: constants::BUILTIN_TYPES_TABLE_ID,
            table_name: constants::BUILTIN_TYPES_TABLE_NAME.to_string(),
        }
        .insert(ctx, runtime, system)
        .await?;

        // Sequences
        RelationRow {
            schema_id: SYSTEM_SCHEMA_ID,
            table_id: constants::SEQUENCES_TABLE_ID,
            table_name: constants::SEQUENCES_TABLE_NAME.to_string(),
        }
        .insert(ctx, runtime, system)
        .await?;

        // Schemas
        RelationRow {
            schema_id: SYSTEM_SCHEMA_ID,
            table_id: constants::SCHEMAS_TABLE_ID,
            table_name: constants::SCHEMAS_TABLE_NAME.to_string(),
        }
        .insert(ctx, runtime, system)
        .await?;

        // Relations
        RelationRow {
            schema_id: SYSTEM_SCHEMA_ID,
            table_id: constants::RELATIONS_TABLE_ID,
            table_name: constants::RELATIONS_TABLE_NAME.to_string(),
        }
        .insert(ctx, runtime, system)
        .await?;

        // Attributes
        RelationRow {
            schema_id: SYSTEM_SCHEMA_ID,
            table_id: constants::ATTRIBUTES_TABLE_ID,
            table_name: constants::ATTRIBUTES_TABLE_NAME.to_string(),
        }
        .insert(ctx, runtime, system)
        .await?;

        Ok(())
    }
}

/// Ensure we have a sequence entry to generate ids when creating
/// resources.
#[derive(Debug)]
pub struct InsertIdSequence;

#[async_trait]
impl SystemBootstrapStep for InsertIdSequence {
    fn name(&self) -> &'static str {
        "insert_id_sequence"
    }

    async fn run(
        &self,
        ctx: &SessionContext,
        runtime: &Arc<AccessRuntime>,
        system: &SystemSchema,
    ) -> Result<()> {
        SequenceRow {
            schema: SYSTEM_SCHEMA_ID,
            table: constants::SEQUENCES_TABLE_ID,
            // TODO: This is jank. Currently doing this avoid specifying an
            // id that would actually belong to a reserved schema.
            //
            // See <https://github.com/GlareDB/glaredb/issues/352>
            next: (PUBLIC_SCHEMA_ID + 1) as i64,
            inc: 1,
        }
        .insert(ctx, runtime, system)
        .await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct InsertSystemSchema;

#[async_trait]
impl SystemBootstrapStep for InsertSystemSchema {
    fn name(&self) -> &'static str {
        "insert_system_schema"
    }

    async fn run(
        &self,
        ctx: &SessionContext,
        runtime: &Arc<AccessRuntime>,
        system: &SystemSchema,
    ) -> Result<()> {
        SchemaRow {
            id: SYSTEM_SCHEMA_ID,
            name: SYSTEM_SCHEMA_NAME.to_string(),
        }
        .insert(ctx, runtime, system)
        .await?;
        Ok(())
    }
}

/// Create the public schema.
#[derive(Debug)]
pub struct CreatePublicSchema;

#[async_trait]
impl SystemBootstrapStep for CreatePublicSchema {
    fn name(&self) -> &'static str {
        "create_public_schema"
    }

    async fn run(
        &self,
        ctx: &SessionContext,
        runtime: &Arc<AccessRuntime>,
        system: &SystemSchema,
    ) -> Result<()> {
        SchemaRow {
            id: PUBLIC_SCHEMA_ID,
            name: PUBLIC_SCHEMA_NAME.to_string(),
        }
        .insert(ctx, runtime, system)
        .await?;
        Ok(())
    }
}
