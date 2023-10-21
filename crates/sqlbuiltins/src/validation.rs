use protogen::metastore::types::options::{
    CopyToDestinationOptions, CredentialsOptions, DatabaseOptions, TableOptions, TunnelOptions,
};

#[derive(thiserror::Error, Debug)]
pub enum ValidationError {
    #[error("Invalid object name length: {length}, max: {max}")]
    InvalidNameLength { length: usize, max: usize },

    #[error("Tunnel '{tunnel}' not supported by datasource '{datasource}'")]
    TunnelNotSupportedByDatasource { tunnel: String, datasource: String },

    #[error("Credentials '{credentials}' not supported by datasource '{datasource}'")]
    CredentialsNotSupportedByDatasource {
        credentials: String,
        datasource: String,
    },

    #[error("Format '{format}' not supported by datasource '{datasource}'")]
    FormatNotSupportedByDatasource { format: String, datasource: String },
}

type Result<T> = std::result::Result<T, ValidationError>;

/// Validate idents as per postgres identifier
/// syntax](https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS)
pub fn validate_object_name(name: &str) -> Result<()> {
    const POSTGRES_IDENT_MAX_LENGTH: usize = 63;
    if name.len() > POSTGRES_IDENT_MAX_LENGTH {
        return Err(ValidationError::InvalidNameLength {
            length: name.len(),
            max: POSTGRES_IDENT_MAX_LENGTH,
        });
    }

    Ok(())
}

/// Validate if the tunnel is supported by the external database.
pub fn validate_database_tunnel_support(database: &str, tunnel: &str) -> Result<()> {
    if matches!(
        (database, tunnel),
        // Debug
        (DatabaseOptions::DEBUG, TunnelOptions::DEBUG)
        // Postgres
        | (DatabaseOptions::POSTGRES, TunnelOptions::SSH)
        // MySQL
        | (DatabaseOptions::MYSQL, TunnelOptions::SSH)
    ) {
        Ok(())
    } else {
        Err(ValidationError::TunnelNotSupportedByDatasource {
            tunnel: tunnel.to_owned(),
            datasource: database.to_owned(),
        })
    }
}

/// Validate if the tunnel is supported by the external table.
pub fn validate_table_tunnel_support(table: &str, tunnel: &str) -> Result<()> {
    if matches!(
        (table, tunnel),
        // Debug
        (TableOptions::DEBUG, TunnelOptions::DEBUG)
        // Postgres
        | (TableOptions::POSTGRES, TunnelOptions::SSH)
        // MySQL
        | (TableOptions::MYSQL, TunnelOptions::SSH)
    ) {
        Ok(())
    } else {
        Err(ValidationError::TunnelNotSupportedByDatasource {
            tunnel: tunnel.to_owned(),
            datasource: table.to_owned(),
        })
    }
}

/// Validate if the credentials provider is supported by the external database.
pub fn validate_database_creds_support(database: &str, creds: &str) -> Result<()> {
    if matches!(
        (database, creds),
        // Google cloud
        (DatabaseOptions::BIGQUERY, CredentialsOptions::GCP) |
        // Delta
        (DatabaseOptions::DELTA, CredentialsOptions::GCP | CredentialsOptions::AWS | CredentialsOptions::AZURE)
    ) {
        Ok(())
    } else {
        Err(ValidationError::CredentialsNotSupportedByDatasource {
            credentials: creds.to_owned(),
            datasource: database.to_owned(),
        })
    }
}

/// Validate if the credentials provider is supported by the external table.
pub fn validate_table_creds_support(table: &str, creds: &str) -> Result<()> {
    if matches!(
        (table, creds),
        // Debug
        (TableOptions::DEBUG, CredentialsOptions::DEBUG) |
        // Google cloud
        (TableOptions::GCS, CredentialsOptions::GCP) |
        (TableOptions::BIGQUERY, CredentialsOptions::GCP) |
        // AWS
        (TableOptions::S3_STORAGE, CredentialsOptions::AWS) |
        // Delta & Iceberg
        (TableOptions::DELTA | TableOptions::ICEBERG, CredentialsOptions::GCP | CredentialsOptions::AWS | CredentialsOptions::AZURE)
    ) {
        Ok(())
    } else {
        Err(ValidationError::CredentialsNotSupportedByDatasource {
            credentials: creds.to_owned(),
            datasource: table.to_owned(),
        })
    }
}

/// Validate if the credentials provider is supported by the "copy to"
/// destination.
pub fn validate_copyto_dest_creds_support(dest: &str, creds: &str) -> Result<()> {
    if matches!(
        (dest, creds),
        // Google cloud
        (CopyToDestinationOptions::GCS, CredentialsOptions::GCP) |
        // Aws
        (CopyToDestinationOptions::S3_STORAGE, CredentialsOptions::AWS)
    ) {
        Ok(())
    } else {
        Err(ValidationError::CredentialsNotSupportedByDatasource {
            credentials: creds.to_owned(),
            datasource: dest.to_owned(),
        })
    }
}

/// Validate if the sink format is supported by the "copy to" destination.
pub fn validate_copyto_dest_format_support(dest: &str, format: &str) -> Result<()> {
    if matches!(
        (dest, format),
        // Local
        (CopyToDestinationOptions::LOCAL, _all) |
        // Google cloud
        (CopyToDestinationOptions::GCS, _all) |
        // AWS
        (CopyToDestinationOptions::S3_STORAGE, _all)
    ) {
        Ok(())
    } else {
        Err(ValidationError::FormatNotSupportedByDatasource {
            format: format.to_owned(),
            datasource: dest.to_owned(),
        })
    }
}
