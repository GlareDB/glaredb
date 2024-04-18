/*
 Current approach implement a DataSourceAccess/Client + reqwest => which then wraps the HTTP resource, streams the data.
 defaults to csv or find the right format.
 read the first row of the sheet, infer the table schema.
 from here, since csv and xlsx are _already supported_, why not dispatch to their api's and re-use that functionality.

ie the xlsx functions and csv reader
*/

pub struct GoogleSheet {
    sheet_id: usize,
    title: String,
    index: usize,
}

pub struct GoogleSheetClient;

#[derive(Debug, Clone)]
pub struct GoogleSheetAccessor;

pub struct GoogleSheetAccessor {
    metadata: GoogleSheetClient,

    // auth creds
}

impl BigQueryAccessor {
    pub async fn connect() -> Result<Self> {
        Ok(())
    }

    /// Validate conn
    pub async fn validate_external_api() -> Result<()> {
        Ok(())
    }

    // validate can read first row from table and cast to schema
    pub async fn validate_table_access() -> Result<()> {
        Ok(())
    }

    pub async fn into_table_provider(
        self,
        table_access: GoogleSheetTableAccess,
        predicate_pushdown: bool,
    ) -> Result<GoogleSheetTableProvider> {
        Ok(GoogleSheetTableProvider {})
    }
}
