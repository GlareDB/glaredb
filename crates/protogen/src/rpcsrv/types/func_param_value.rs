use datafusion_proto::protobuf::ScalarValue;
use prost::{Message, Oneof};

#[derive(Clone, PartialEq, Message)]
pub struct FuncParamValue {
    #[prost(oneof = "FuncParamValueEnum", tags = "1, 2, 3")]
    pub func_param_value_enum: Option<FuncParamValueEnum>,
}

#[derive(Clone, PartialEq, Oneof)]
pub enum FuncParamValueEnum {
    #[prost(string, tag = "1")]
    Ident(String),
    #[prost(message, tag = "2")]
    Scalar(ScalarValue),
    #[prost(message, tag = "3")]
    Array(FuncParamValueArrayVariant),
}

#[derive(Clone, PartialEq, Message)]
pub struct FuncParamValueArrayVariant {
    #[prost(message, repeated, tag = "1")]
    pub array: Vec<FuncParamValue>,
}
