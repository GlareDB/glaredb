use crate::errors::{MetastoreClientError, Result};
use crate::types::options::{
    CopyToDestinationOptions, CredentialsOptions, DatabaseOptions, TableOptions, TunnelOptions,
};

/// Validate idents as per postgres identifier
/// syntax](https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS)
pub fn validate_object_name(name: &str) -> Result<()> {
    const POSTGRES_IDENT_MAX_LENGTH: usize = 63;
    if name.len() > POSTGRES_IDENT_MAX_LENGTH {
        return Err(MetastoreClientError::InvalidNameLength {
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
        Err(MetastoreClientError::TunnelNotSupportedByDatasource {
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
        Err(MetastoreClientError::TunnelNotSupportedByDatasource {
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
        (DatabaseOptions::BIGQUERY, CredentialsOptions::GCP)
    ) {
        Ok(())
    } else {
        Err(MetastoreClientError::CredentialsNotSupportedByDatasource {
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
        (TableOptions::S3_STORAGE, CredentialsOptions::AWS)
    ) {
        Ok(())
    } else {
        Err(MetastoreClientError::CredentialsNotSupportedByDatasource {
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
        Err(MetastoreClientError::CredentialsNotSupportedByDatasource {
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
        Err(MetastoreClientError::FormatNotSupportedByDatasource {
            format: format.to_owned(),
            datasource: dest.to_owned(),
        })
    }
}
