/// Configuration for intermediate pipeline planning.
#[derive(Debug, Clone)]
pub struct OperatorPlanConfig {
    /// If "rows inserted" should be returned as a grand total, or if we should
    /// return counts per partition.
    pub per_partition_counts: bool,
    /// If hash joins are enabled.
    pub enable_hash_joins: bool,
}

#[derive(Debug, Clone)]
pub struct ExecutablePlanConfig {
    /// Target number of partitions in executable pipelines.
    ///
    /// Partitionining determines parallelism for a single pipeline.
    pub partitions: usize,
    /// Target batch size.
    pub batch_size: usize,
}
