use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use datafusion::datasource::streaming::StreamingTable;
use datafusion::datasource::TableProvider;
use datafusion::parquet::data_type::AsBytes;
use futures::StreamExt;
use tokio_util::codec::LengthDelimitedCodec;

use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::{FuncParamValue, TableFunc, TableFuncContextProvider};
use datasources::bson::schema::{merge_schemas, schema_from_document};
use datasources::object_store::generic::GenericStoreAccess;
use datasources::object_store::ObjStoreAccess;
use protogen::metastore::types::catalog::RuntimePreference;

use crate::functions::table_location_and_opts;

#[derive(Debug, Clone, Copy, Default)]
pub struct BsonScan {}

#[async_trait]
impl TableFunc for BsonScan {
    fn runtime_preference(&self) -> RuntimePreference {
        RuntimePreference::Unspecified
    }

    fn name(&self) -> &str {
        "read_bson"
    }

    // TODO: in addition to a much needed refactor, most of this should be implemented as a
    // TableProvider in the datasources bson package and just wrapped here.
    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>, ExtensionError> {
        let (source_url, storage_options) = table_location_and_opts(ctx, args, &mut opts)?;

        let store_access = GenericStoreAccess::new_from_location_and_opts(
            source_url.to_string().as_str(),
            storage_options,
        )
        .map_err(|e| ExtensionError::Arrow(e.into()))?;

        let store = store_access
            .create_store()
            .map_err(|e| ExtensionError::Arrow(e.into()))?;
        let path = source_url.path();

        // assume that the file type is a glob and see if there are
        // more files...
        let mut list = store_access
            .list_globbed(&store, path.as_ref())
            .await
            .map_err(|e| ExtensionError::Arrow(e.into()))?;

        if list.is_empty() {
            return Err(ExtensionError::String(format!(
                "no matching objects for '{path}'"
            )));
        }

        // for consistent results, particularly for the sample, always
        // sort by location
        list.sort_by(|a, b| a.location.cmp(&b.location));

        // parse arguments to see how many documents to consider
        let sample_size = match opts.get("schema_sample_size") {
            // TODO: set a maximum (1024?) or have an adaptive mode
            // (at least n but stop after n the same) or skip documents
            Some(v) => v.to_owned().param_into()?,
            None => 100,
        };

        // by default we'll set up streams that could, in theory (after the sample) read from the
        // files in parallel, leading to a table that will be streamed
        let _serial_scan = opts.get("serial_scan").is_some();

        // build a vector of streams, one for each file, that handle BSON's framing.
        let mut readers = VecDeque::with_capacity(list.len());
        for obj in list {
            readers.push_back(
                // BSON is just length-prefixed byte sequences
                LengthDelimitedCodec::builder()
                    // set up the framing parameters, use a 16MB max-doc size, which is the same as
                    // the MongoDB server, this is arbitrary, and we could easily support larger
                    // documents, but one has to draw the line somewhere, 16MB is (frankly) absurdly
                    // large for a row size, and anything larger wouldn't be round-trippable to
                    // MongoDB.
                    .max_frame_length(16 * 1024 * 1024)
                    .length_field_type::<u32>() // actually signed int32s
                    .length_field_offset(0) // length field is first
                    .length_adjustment(4) // length prefix includes
                    // the prefix use the object_store buffered reader
                    // to stream data from the object store:
                    .new_read(object_store::buffered::BufReader::with_capacity(
                        store.to_owned(),
                        &obj,
                        32 * 1024 * 1024, // 32 MB buffer, probably still too small
                    ))
                    // convert the chunk of bytes to bson.
                    .map(
                        // TODO: this probably wants to be a raw document eventually, so we can put
                        // all the _fields_ in a map, iterate over the document once, and check each
                        // bson field name against the schema, and only pull out the fields that
                        // match. This is easier in the short term but less performant for large
                        // documents where the docuemnts are a superset of the schema, we'll end up
                        // doing much more parsing work than is actually needed for the bson
                        // documents.
                        |bt: Result<BytesMut, std::io::Error>| -> Result<bson::Document, ExtensionError> {
                            Ok(bson::de::from_slice::<bson::Document>(
                                bt?.freeze().as_bytes().to_owned().as_slice(),
                            )?
                            .to_owned())
                        }),
            );
        }

        // iterate through the readers and build up a sample of the first <n> documents to be used
        // to infer the schema.
        let mut sample = Vec::<bson::Document>::with_capacity(sample_size as usize);
        let mut first_active: usize = 0;
        'readers: for reader in readers.iter_mut() {
            while let Some(res) = reader.next().await {
                match res {
                    Ok(doc) => sample.push(doc),
                    Err(e) => return Err(e),
                };

                if sample.len() as i64 >= sample_size {
                    break 'readers;
                }
            }
            first_active += 1;
        }

        // if we had to read through one or more than of the input files in the glob, we already
        // have their documents and should truncate the vector of readers.
        for _ in 0..first_active {
            readers.pop_front();
        }

        // infer the sechema; in the future we can allow users to specify the schema directly; in
        // the future users could specify the schema (kind of as a base-level projection, but we'd
        // need a schema specification language). Or have some other strategy for inference rather
        // than every unique field from the first <n> documents.
        let schema = merge_schemas(sample.iter().map(|doc| schema_from_document(doc).unwrap()))
            .map_err(|e| ExtensionError::String(e.to_string()))?;

        // TODO: create two partion scanner implementations and add them to a vector as below: one
        // that just returns the documents from the sample in the table and a second one for each
        // reader.

        Ok(Arc::new(StreamingTable::try_new(
            Arc::new(schema), // <= inferred schema
            Vec::new(),       // <= vector of partition scanners
        )?))
    }
}
