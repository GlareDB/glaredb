use std::collections::VecDeque;
use std::sync::Arc;

use bson::RawDocumentBuf;
use bytes::BytesMut;
use datafusion::datasource::streaming::StreamingTable;
use datafusion::datasource::TableProvider;
use datafusion::parquet::data_type::AsBytes;
use datafusion::physical_plan::streaming::PartitionStream;
use futures::StreamExt;
use tokio_util::codec::LengthDelimitedCodec;

use crate::bson::errors::BsonError;
use crate::bson::schema::{merge_schemas, schema_from_document};
use crate::bson::stream::BsonPartitionStream;
use crate::common::url::DatasourceUrl;
use crate::object_store::glob_util::get_resolved_patterns;
use crate::object_store::ObjStoreAccess;

pub async fn bson_streaming_table(
    store_access: Arc<dyn ObjStoreAccess>,
    schema_inference_sample_size: Option<i64>,
    source_url: DatasourceUrl,
) -> Result<Arc<dyn TableProvider>, BsonError> {
    // TODO: set a maximum (1024?) or have an adaptive mode
    // (at least n but stop after n the same) or skip documents
    let sample_size = schema_inference_sample_size.unwrap_or(100);

    let path = source_url.path().into_owned();

    let store = store_access.create_store()?;

    let paths = get_resolved_patterns(path.clone());

    // assume that the file type is a glob and see if there are
    // more files...
    let mut list = Vec::new();
    for path in paths {
        let sub_list = store_access.list_globbed(&store, path).await?;
        list.extend(sub_list);
    }

    if list.is_empty() {
        return Err(BsonError::NotFound(path));
    }

    // for consistent results, particularly for the sample, always
    // sort by location
    list.sort_by(|a, b| a.location.cmp(&b.location));

    // build a vector of streams, one for each file, that handle BSON's framing.
    let mut readers = VecDeque::with_capacity(list.len());
    for obj in list {
        readers.push_back(
            // BSON is just length-prefixed byte sequences
            LengthDelimitedCodec::builder()
                // set up the framing parameters, use a 16MB max-doc size,
                // which is the same as the MongoDB server, this is
                // arbitrary, and we could easily support larger documents,
                // but one has to draw the line somewhere, 16MB is (frankly)
                // absurdly large for a row size, and anything larger
                // wouldn't be round-trippable to MongoDB.
                .max_frame_length(16 * 1024 * 1024)
                .little_endian() // bson is always little-endian
                .length_field_type::<u32>() // actually signed int32s
                .length_field_offset(0) // length field is first
                .length_adjustment(0) // length prefix includes
                .num_skip(0) // send the prefix and payload to the bson library
                // the prefix use the object_store buffered reader
                // to stream data from the object store:
                .new_read(object_store::buffered::BufReader::with_capacity(
                    store.to_owned(),
                    &obj,
                    32 * 1024 * 1024, // 32 MB buffer, probably still too small.
                ))
                // convert the chunk of bytes to bson.
                .map(
                    // TODO: this probably wants to be a raw document
                    // eventually, so we can put all the _fields_ in a map,
                    // iterate over the document once, and check each bson
                    // field name against the schema, and only pull out the
                    // fields that match. This is easier in the short term
                    // but less performant for large documents where the
                    // documents are a superset of the schema, we'll end up
                    // doing much more parsing work than is actually needed
                    // for the bson documents.
                    |bt: Result<BytesMut, std::io::Error>| -> Result<RawDocumentBuf, BsonError> {
                        Ok(bson::de::from_slice::<RawDocumentBuf>(
                            bt?.freeze().as_bytes().to_owned().as_slice(),
                        )?)
                    },
                ),
        );
    }

    // iterate through the readers and build up a sample of the first <n>
    // documents to be used to infer the schema.
    let mut sample = Vec::with_capacity(sample_size as usize);
    let mut first_active: usize = 0;
    'readers: for reader in readers.iter_mut() {
        while let Some(res) = reader.next().await {
            match res {
                Ok(doc) => sample.push(doc),
                Err(e) => return Err(e),
            };

            if sample.len() >= sample_size as usize {
                break 'readers;
            }
        }
        first_active += 1;
    }

    // if we had to read through one or more than of the input files in the
    // glob, we already have their documents and should truncate the vector
    // of readers.
    for _ in 0..first_active {
        readers.pop_front();
    }

    // infer the schema; in the future we can allow users to specify the
    // schema directly; in the future users could specify the schema (kind
    // of as a base-level projection, but we'd need a schema specification
    // language). Or have some other strategy for inference rather than
    // every unique field from the first <n> documents.
    let schema = Arc::new(merge_schemas(
        sample
            .iter()
            .map(|doc| schema_from_document(&doc.to_raw_document_buf())),
    )?);

    let mut streams = Vec::<Arc<(dyn PartitionStream + 'static)>>::with_capacity(readers.len() + 1);

    // all the documents we read for the sample are hanging around
    // somewhere and we want to make sure that callers access
    // them: we're going to make a special stream with these
    // documents here.
    streams.push(Arc::new(BsonPartitionStream::new(
        schema.clone(),
        futures::stream::iter(
            sample
                .into_iter()
                .map(|doc| -> Result<RawDocumentBuf, BsonError> { Ok(doc) }),
        )
        .boxed(),
    )));

    // for all remaining streams we wrap the stream of documents
    // and convert them into partition streams which the streaming
    // table exec can read.
    while let Some(reader) = readers.pop_front() {
        streams.push(Arc::new(BsonPartitionStream::new(
            schema.clone(),
            reader.boxed(),
        )));
    }

    Ok(Arc::new(StreamingTable::try_new(
        schema.clone(), // <= inferred schema
        streams,        // <= vector of partition streams
    )?))
}
