use std::collections::HashMap;
use std::sync::Arc;

use rayexec_error::{OptionExt, Result};
use rayexec_proto::ProtoConv;
use uuid::Uuid;

use crate::database::DatabaseContext;
use crate::execution::operators::PhysicalOperator;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::logical::binder::bind_context::MaterializationRef;
use crate::proto::DatabaseProtoConv;

/// ID of a single intermediate pipeline.
///
/// Unique within a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IntermediatePipelineId(pub usize);

/// Identifier for streams that connect different pipelines across pipeline
/// groups.
///
/// Globally unique.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamId {
    pub query_id: Uuid,
    pub stream_id: Uuid,
}

impl ProtoConv for StreamId {
    type ProtoType = rayexec_proto::generated::execution::StreamId;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            query_id: Some(self.query_id.to_proto()?),
            stream_id: Some(self.stream_id.to_proto()?),
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            query_id: Uuid::from_proto(proto.query_id.required("query_id")?)?,
            stream_id: Uuid::from_proto(proto.stream_id.required("stream_id")?)?,
        })
    }
}

impl ProtoConv for IntermediatePipelineId {
    type ProtoType = rayexec_proto::generated::execution::IntermediatePipelineId;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType { id: self.0 as u32 })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self(proto.id as usize))
    }
}

/// Location of the sink for a particular pipeline.
///
/// During single-node execution this will point to an operator where batches
/// should be pushed to (e.g. the build side of a join).
///
/// Hyrbid execution introduces a chance for the sink to be a remote pipeline.
/// To handle this, we insert an ipc sink/source operator on both ends. The
/// `Remote` variant contians information for building the sink side
/// appropriately.
#[derive(Debug, Clone)]
pub enum PipelineSink {
    /// Send this pipeline's results the query output (client).
    QueryOutput,
    /// The pipeline's sink is already included in the pipeline.
    InPipeline,
    /// Sink is in the same group of operators as itself.
    InGroup {
        /// ID of the pipeline we should send output to.
        pipeline_id: IntermediatePipelineId,
        /// Index of the operator in the pipeline.
        operator_idx: usize,
        /// Index of the input this pipeline is into the other pipeline.
        ///
        /// Typically 0, but may be some other value in the case of joins.
        input_idx: usize,
    },
    /// Sink is a pipeline in some other group.
    ///
    /// Currently this just indicates that we're sending to a pipeline "not on
    /// this machine".
    OtherGroup {
        /// Stream ID we should be using when sending output to the other group.
        stream_id: StreamId,
        /// Number of partitions the receiving pipeline expects.
        partitions: usize,
    },
    /// Sink is into a materialization operator.
    Materialization { mat_ref: MaterializationRef },
}

impl ProtoConv for PipelineSink {
    type ProtoType = rayexec_proto::generated::execution::PipelineSink;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::execution::pipeline_sink::Value;
        use rayexec_proto::generated::execution::{
            PipelineSinkInGroup,
            PipelineSinkMaterialization,
            PipelineSinkOtherGroup,
        };

        let value = match self {
            Self::QueryOutput => Value::QueryOutput(Default::default()),
            Self::InPipeline => Value::InPipeline(Default::default()),
            Self::InGroup {
                pipeline_id,
                operator_idx,
                input_idx,
            } => Value::InGroup(PipelineSinkInGroup {
                id: Some(pipeline_id.to_proto()?),
                operator_idx: *operator_idx as u32,
                input_idx: *input_idx as u32,
            }),
            Self::OtherGroup {
                partitions,
                stream_id,
            } => Value::OtherGroup(PipelineSinkOtherGroup {
                stream_id: Some(stream_id.to_proto()?),
                partitions: *partitions as u32,
            }),
            Self::Materialization { mat_ref } => {
                Value::Materialization(PipelineSinkMaterialization {
                    materialization_ref: mat_ref.materialization_idx as u32,
                })
            }
        };

        Ok(Self::ProtoType { value: Some(value) })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        use rayexec_proto::generated::execution::pipeline_sink::Value;
        use rayexec_proto::generated::execution::{
            PipelineSinkInGroup,
            PipelineSinkMaterialization,
            PipelineSinkOtherGroup,
        };

        Ok(match proto.value.required("value")? {
            Value::QueryOutput(_) => Self::QueryOutput,
            Value::InPipeline(_) => Self::InPipeline,
            Value::InGroup(PipelineSinkInGroup {
                id,
                operator_idx,
                input_idx,
            }) => Self::InGroup {
                pipeline_id: IntermediatePipelineId::from_proto(id.required("id")?)?,
                operator_idx: operator_idx as usize,
                input_idx: input_idx as usize,
            },
            Value::OtherGroup(PipelineSinkOtherGroup {
                stream_id,
                partitions,
            }) => Self::OtherGroup {
                stream_id: StreamId::from_proto(stream_id.required("stream_id")?)?,
                partitions: partitions as usize,
            },
            Value::Materialization(PipelineSinkMaterialization {
                materialization_ref,
            }) => Self::Materialization {
                mat_ref: MaterializationRef {
                    materialization_idx: materialization_ref as usize,
                },
            },
        })
    }
}

/// Location of the source of a pipeline.
///
/// Single-node execution will always have the source as the first operator in
/// the chain (and nothing needs to be done).
///
/// For hybrid execution, the source may be a remote pipeline, and so we will
/// include an ipc source operator as this pipeline's source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineSource {
    /// Source is already in the pipeline, don't do anything.
    InPipeline,
    /// Source is some other pipeline in the same group as this pipeline.
    OtherPipeline {
        /// Pipeline to pull from.
        pipeline: IntermediatePipelineId,
        /// Optional partitioning requirement.
        ///
        /// This should be set if the pipline we're constructing should maintain
        /// the output partitioning of the source.
        ///
        /// Mostly for ORDER BY to maintain sortedness through the pipeline.
        partitioning_requirement: Option<usize>,
    },
    /// Source is remote, build an ipc source.
    OtherGroup {
        stream_id: StreamId,
        partitions: usize,
    },
    /// Source is from a materialization operator.
    Materialization { mat_ref: MaterializationRef },
}

impl ProtoConv for PipelineSource {
    type ProtoType = rayexec_proto::generated::execution::PipelineSource;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::execution::pipeline_source::Value;
        use rayexec_proto::generated::execution::{
            PipelineSourceMaterialization,
            PipelineSourceOtherGroup,
            PipelineSourceOtherPipeline,
        };

        let value = match self {
            Self::InPipeline => Value::InPipeline(Default::default()),
            Self::OtherPipeline {
                pipeline,
                partitioning_requirement,
            } => Value::OtherPipeline(PipelineSourceOtherPipeline {
                id: Some(pipeline.to_proto()?),
                partitioning_requirement: partitioning_requirement.map(|p| p as u32),
            }),
            Self::OtherGroup {
                partitions,
                stream_id,
            } => Value::OtherGroup(PipelineSourceOtherGroup {
                stream_id: Some(stream_id.to_proto()?),
                partitions: *partitions as u32,
            }),
            Self::Materialization { mat_ref } => {
                Value::Materialization(PipelineSourceMaterialization {
                    materialization_ref: mat_ref.materialization_idx as u32,
                })
            }
        };

        Ok(Self::ProtoType { value: Some(value) })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        use rayexec_proto::generated::execution::pipeline_source::Value;
        use rayexec_proto::generated::execution::{
            PipelineSourceMaterialization,
            PipelineSourceOtherGroup,
            PipelineSourceOtherPipeline,
        };

        Ok(match proto.value.required("value")? {
            Value::InPipeline(_) => Self::InPipeline,
            Value::OtherPipeline(PipelineSourceOtherPipeline {
                id,
                partitioning_requirement,
            }) => Self::OtherPipeline {
                pipeline: IntermediatePipelineId::from_proto(id.required("id")?)?,
                partitioning_requirement: partitioning_requirement.map(|p| p as usize),
            },
            Value::OtherGroup(PipelineSourceOtherGroup {
                stream_id,
                partitions,
            }) => Self::OtherGroup {
                stream_id: StreamId::from_proto(stream_id.required("stream_id")?)?,
                partitions: partitions as usize,
            },
            Value::Materialization(PipelineSourceMaterialization {
                materialization_ref,
            }) => Self::Materialization {
                mat_ref: MaterializationRef {
                    materialization_idx: materialization_ref as usize,
                },
            },
        })
    }
}

#[derive(Debug, Default)]
pub struct IntermediatePipelineGroup {
    pub(crate) pipelines: HashMap<IntermediatePipelineId, IntermediatePipeline>,
}

impl IntermediatePipelineGroup {
    pub fn is_empty(&self) -> bool {
        self.pipelines.is_empty()
    }

    pub fn merge_from_other(&mut self, other: &mut Self) {
        self.pipelines.extend(other.pipelines.drain())
    }
}

impl DatabaseProtoConv for IntermediatePipelineGroup {
    type ProtoType = rayexec_proto::generated::execution::IntermediatePipelineGroup;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            pipelines: self
                .pipelines
                .values()
                .map(|p| p.to_proto_ctx(context))
                .collect::<Result<Vec<_>>>()?,
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            pipelines: proto
                .pipelines
                .into_iter()
                .map(|p| {
                    let pipeline = IntermediatePipeline::from_proto_ctx(p, context)?;
                    Ok((pipeline.id, pipeline))
                })
                .collect::<Result<HashMap<_, _>>>()?,
        })
    }
}

#[derive(Debug, Default)]
pub struct IntermediateMaterializationGroup {
    pub(crate) materializations: HashMap<MaterializationRef, IntermediateMaterialization>,
}

impl IntermediateMaterializationGroup {
    pub fn is_empty(&self) -> bool {
        self.materializations.is_empty()
    }
}

/// An intermediate materialization.
///
/// Almost the same thing as a normal pipeline, but we don't know where the
/// batches will be sent yet.
// TODO: There might be path to unifying this with intermediate pipeline.
// Eventually everything gets turned into a pipeline.
#[derive(Debug)]
pub struct IntermediateMaterialization {
    pub(crate) id: IntermediatePipelineId,
    pub(crate) source: PipelineSource,
    pub(crate) operators: Vec<IntermediateOperator>,
    /// How many output scans there are for this materialization.
    pub(crate) scan_count: usize,
}

#[derive(Debug)]
pub struct IntermediatePipeline {
    pub(crate) id: IntermediatePipelineId,
    pub(crate) sink: PipelineSink,
    pub(crate) source: PipelineSource,
    pub(crate) operators: Vec<IntermediateOperator>,
}

impl DatabaseProtoConv for IntermediatePipeline {
    type ProtoType = rayexec_proto::generated::execution::IntermediatePipeline;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            id: Some(self.id.to_proto()?),
            sink: Some(self.sink.to_proto()?),
            source: Some(self.source.to_proto()?),
            operators: self
                .operators
                .iter()
                .map(|o| o.to_proto_ctx(context))
                .collect::<Result<Vec<_>>>()?,
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            id: IntermediatePipelineId::from_proto(proto.id.required("id")?)?,
            sink: PipelineSink::from_proto(proto.sink.required("sink")?)?,
            source: PipelineSource::from_proto(proto.source.required("source")?)?,
            operators: proto
                .operators
                .into_iter()
                .map(|o| IntermediateOperator::from_proto_ctx(o, context))
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

#[derive(Debug)]
pub struct IntermediateOperator {
    /// The physical operator that will be used in the executable pipline.
    pub(crate) operator: Arc<PhysicalOperator>,

    /// If this operator has a partitioning requirement.
    ///
    /// If set, the input and output partitions for this operator will be the
    /// value provided. If unset, it'll default to a value determined by the
    /// executable pipeline planner.
    pub(crate) partitioning_requirement: Option<usize>,
}

impl DatabaseProtoConv for IntermediateOperator {
    type ProtoType = rayexec_proto::generated::execution::IntermediateOperator;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            operator: Some(self.operator.to_proto_ctx(context)?),
            partitioning_requirement: self.partitioning_requirement.map(|v| v as u32),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            operator: Arc::new(PhysicalOperator::from_proto_ctx(
                proto.operator.required("operator")?,
                context,
            )?),
            partitioning_requirement: proto.partitioning_requirement.map(|v| v as usize),
        })
    }
}

impl Explainable for IntermediateOperator {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.operator.explain_entry(conf).with_value(
            "partitioning_requirement",
            format!("{:?}", self.partitioning_requirement),
        )
    }
}
