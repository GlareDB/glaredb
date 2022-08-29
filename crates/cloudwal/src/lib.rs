pub mod errors;
pub mod wal;

mod lemur_impl;

#[cfg(test)]
mod tests {
    use super::wal::Wal;
    use lemur::execute::stream::source::{DataSource, MemoryDataSource, ReadTx, WriteTx};
    use lemur::repr::df::{DataFrame, Schema};
    use lemur::repr::value::{Value, ValueType};
    use object_store::local::LocalFileSystem;
    use tempdir::TempDir;

    #[tokio::test]
    async fn simple() {
        logutil::init_test();

        let tmp = TempDir::new("simple").unwrap();
        // Create new in-memory data. Everything should be written to the wal.
        {
            let store = LocalFileSystem::new_with_prefix(tmp.path()).unwrap();
            let source = MemoryDataSource::new();
            let wal = Wal::open(source, store, "1").await.unwrap();
            let tx = wal.begin().await.unwrap();

            let table = "test_table".to_string();
            tx.allocate_table(table.clone(), Schema::from(vec![ValueType::Int32]))
                .await
                .unwrap();
            let data = DataFrame::from_row(vec![Value::Int32(Some(4))]).unwrap();
            tx.insert(&table, &[0], data).await.unwrap();
            tx.commit().await.unwrap();

            wal.shutdown_wait().await.unwrap();
        }

        // Try to open an existing wal into a new in-memory data source.
        {
            let store = LocalFileSystem::new_with_prefix(tmp.path()).unwrap();
            let source = MemoryDataSource::new();
            let wal = Wal::open(source, store, "1").await.unwrap();
            let tx = wal.begin().await.unwrap();

            let table = "test_table".to_string();
            let maybe_schema = tx.get_schema(&table).await.unwrap();
            match maybe_schema {
                Some(schema) => assert_eq!(Schema::from(vec![ValueType::Int32]), schema),
                None => panic!("schema not persisted"),
            }
        }
    }
}
