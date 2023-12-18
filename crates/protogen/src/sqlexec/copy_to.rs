use prost::{Message, Oneof};

use crate::ProtoConvError;

#[derive(Clone, PartialEq, Message)]
pub struct CopyToDestinationOptions {
    #[prost(oneof = "CopyToDestinationOptionsEnum", tags = "1, 2, 3, 4")]
    pub copy_to_destination_options_enum: Option<CopyToDestinationOptionsEnum>,
}

#[derive(Clone, PartialEq, Oneof)]
pub enum CopyToDestinationOptionsEnum {
    #[prost(message, tag = "1")]
    Local(CopyToDestinationOptionsLocal),
    #[prost(message, tag = "2")]
    Gcs(CopyToDestinationOptionsGcs),
    #[prost(message, tag = "3")]
    S3(CopyToDestinationOptionsS3),
    #[prost(message, tag = "4")]
    Azure(CopyToDestinationOptionsAzure),
}

#[derive(Clone, PartialEq, Message)]
pub struct CopyToDestinationOptionsLocal {
    #[prost(string, tag = "1")]
    pub location: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct CopyToDestinationOptionsGcs {
    #[prost(string, optional, tag = "1")]
    pub service_account_key: Option<String>,
    #[prost(string, tag = "2")]
    pub bucket: String,
    #[prost(string, tag = "3")]
    pub location: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct CopyToDestinationOptionsS3 {
    #[prost(string, optional, tag = "1")]
    pub access_key_id: Option<String>,
    #[prost(string, optional, tag = "2")]
    pub secret_access_key: Option<String>,
    #[prost(string, tag = "3")]
    pub region: String,
    #[prost(string, tag = "4")]
    pub bucket: String,
    #[prost(string, tag = "5")]
    pub location: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct CopyToDestinationOptionsAzure {
    #[prost(string, tag = "1")]
    pub account: String,
    #[prost(string, tag = "2")]
    pub access_key: String,
    #[prost(string, tag = "3")]
    pub location: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct CopyToFormatOptions {
    #[prost(oneof = "CopyToFormatOptionsEnum", tags = "1, 2, 3")]
    pub copy_to_format_options_enum: Option<CopyToFormatOptionsEnum>,
}

#[derive(Clone, PartialEq, Oneof)]
pub enum CopyToFormatOptionsEnum {
    #[prost(message, tag = "1")]
    Csv(CopyToFormatOptionsCsv),
    #[prost(message, tag = "2")]
    Json(CopyToFormatOptionsJson),
    #[prost(message, tag = "3")]
    Parquet(CopyToFormatOptionsParquet),
}

#[derive(Clone, PartialEq, Message)]
pub struct CopyToFormatOptionsCsv {
    #[prost(uint32, tag = "1")]
    pub delim: u32,
    #[prost(bool, tag = "2")]
    pub header: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct CopyToFormatOptionsJson {
    #[prost(bool, tag = "1")]
    pub array: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct CopyToFormatOptionsParquet {
    #[prost(uint64, tag = "1")]
    pub row_group_size: u64,
}

impl TryFrom<crate::metastore::types::options::CopyToFormatOptions> for CopyToFormatOptions {
    type Error = crate::errors::ProtoConvError;
    fn try_from(
        value: crate::metastore::types::options::CopyToFormatOptions,
    ) -> Result<Self, Self::Error> {
        match value {
            crate::metastore::types::options::CopyToFormatOptions::Bson => {
                Ok(CopyToFormatOptions::default())
            }
            crate::metastore::types::options::CopyToFormatOptions::Csv(csv) => {
                Ok(CopyToFormatOptions {
                    copy_to_format_options_enum: Some(CopyToFormatOptionsEnum::Csv(
                        CopyToFormatOptionsCsv {
                            delim: csv.delim as u32,
                            header: csv.header,
                        },
                    )),
                })
            }
            crate::metastore::types::options::CopyToFormatOptions::Json(json) => {
                Ok(CopyToFormatOptions {
                    copy_to_format_options_enum: Some(CopyToFormatOptionsEnum::Json(
                        CopyToFormatOptionsJson { array: json.array },
                    )),
                })
            }
            crate::metastore::types::options::CopyToFormatOptions::Parquet(parquet) => {
                Ok(CopyToFormatOptions {
                    copy_to_format_options_enum: Some(CopyToFormatOptionsEnum::Parquet(
                        CopyToFormatOptionsParquet {
                            row_group_size: parquet.row_group_size as u64,
                        },
                    )),
                })
            }
        }
    }
}

impl TryFrom<CopyToFormatOptions> for crate::metastore::types::options::CopyToFormatOptions {
    type Error = ProtoConvError;

    fn try_from(value: CopyToFormatOptions) -> Result<Self, Self::Error> {
        let value = value
            .copy_to_format_options_enum
            .ok_or(ProtoConvError::RequiredField(
                "copy_to_format_options_enum".to_string(),
            ))?;

        match value {
            CopyToFormatOptionsEnum::Csv(csv) => {
                Ok(crate::metastore::types::options::CopyToFormatOptions::Csv(
                    crate::metastore::types::options::CopyToFormatOptionsCsv {
                        delim: csv.delim as u8,
                        header: csv.header,
                    },
                ))
            }
            CopyToFormatOptionsEnum::Json(json) => {
                Ok(crate::metastore::types::options::CopyToFormatOptions::Json(
                    crate::metastore::types::options::CopyToFormatOptionsJson { array: json.array },
                ))
            }

            CopyToFormatOptionsEnum::Parquet(parquet) => Ok(
                crate::metastore::types::options::CopyToFormatOptions::Parquet(
                    crate::metastore::types::options::CopyToFormatOptionsParquet {
                        row_group_size: parquet.row_group_size as usize,
                    },
                ),
            ),
        }
    }
}

impl TryFrom<crate::metastore::types::options::CopyToDestinationOptions>
    for CopyToDestinationOptions
{
    type Error = crate::errors::ProtoConvError;
    fn try_from(
        value: crate::metastore::types::options::CopyToDestinationOptions,
    ) -> Result<Self, Self::Error> {
        match value {
            crate::metastore::types::options::CopyToDestinationOptions::Local(local) => {
                Ok(CopyToDestinationOptions {
                    copy_to_destination_options_enum: Some(CopyToDestinationOptionsEnum::Local(
                        CopyToDestinationOptionsLocal {
                            location: local.location,
                        },
                    )),
                })
            }
            crate::metastore::types::options::CopyToDestinationOptions::Gcs(gcs) => {
                Ok(CopyToDestinationOptions {
                    copy_to_destination_options_enum: Some(CopyToDestinationOptionsEnum::Gcs(
                        CopyToDestinationOptionsGcs {
                            service_account_key: gcs.service_account_key,
                            bucket: gcs.bucket,
                            location: gcs.location,
                        },
                    )),
                })
            }
            crate::metastore::types::options::CopyToDestinationOptions::S3(s3) => {
                Ok(CopyToDestinationOptions {
                    copy_to_destination_options_enum: Some(CopyToDestinationOptionsEnum::S3(
                        CopyToDestinationOptionsS3 {
                            access_key_id: s3.access_key_id,
                            secret_access_key: s3.secret_access_key,
                            region: s3.region,
                            bucket: s3.bucket,
                            location: s3.location,
                        },
                    )),
                })
            }
            crate::metastore::types::options::CopyToDestinationOptions::Azure(azure) => {
                Ok(CopyToDestinationOptions {
                    copy_to_destination_options_enum: Some(CopyToDestinationOptionsEnum::Azure(
                        CopyToDestinationOptionsAzure {
                            account: azure.account,
                            access_key: azure.access_key,
                            location: azure.location,
                        },
                    )),
                })
            }
        }
    }
}

impl TryFrom<CopyToDestinationOptions>
    for crate::metastore::types::options::CopyToDestinationOptions
{
    type Error = ProtoConvError;

    fn try_from(value: CopyToDestinationOptions) -> Result<Self, Self::Error> {
        let value = value
            .copy_to_destination_options_enum
            .ok_or(ProtoConvError::RequiredField(
                "copy_to_destination_options_enum".to_string(),
            ))?;
        match value {
            CopyToDestinationOptionsEnum::Local(local) => Ok(
                crate::metastore::types::options::CopyToDestinationOptions::Local(
                    crate::metastore::types::options::CopyToDestinationOptionsLocal {
                        location: local.location,
                    },
                ),
            ),
            CopyToDestinationOptionsEnum::Gcs(gcs) => Ok(
                crate::metastore::types::options::CopyToDestinationOptions::Gcs(
                    crate::metastore::types::options::CopyToDestinationOptionsGcs {
                        service_account_key: gcs.service_account_key,
                        bucket: gcs.bucket,
                        location: gcs.location,
                    },
                ),
            ),
            CopyToDestinationOptionsEnum::S3(s3) => Ok(
                crate::metastore::types::options::CopyToDestinationOptions::S3(
                    crate::metastore::types::options::CopyToDestinationOptionsS3 {
                        access_key_id: s3.access_key_id,
                        secret_access_key: s3.secret_access_key,
                        region: s3.region,
                        bucket: s3.bucket,
                        location: s3.location,
                    },
                ),
            ),
            CopyToDestinationOptionsEnum::Azure(azure) => Ok(
                crate::metastore::types::options::CopyToDestinationOptions::Azure(
                    crate::metastore::types::options::CopyToDestinationOptionsAzure {
                        access_key: azure.access_key,
                        account: azure.account,
                        location: azure.location,
                    },
                ),
            ),
        }
    }
}
