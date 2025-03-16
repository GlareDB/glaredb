/// Configuration for intermediate pipeline planning.
#[derive(Debug, Clone)]
pub struct OperatorPlanConfig {
    pub per_partition_counts: bool,
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
