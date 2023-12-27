use datafusion::arrow::array::{Array, ArrayRef, Decimal128Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use datafusion::scalar::ScalarValue;

pub fn try_cast(
    array: &dyn Array,
    op: &dyn Fn(ScalarValue) -> Result<ScalarValue, ArrowError>,
) -> Result<ArrayRef, ArrowError> {
    Ok(match array.data_type() {
        // DataType::Null => ScalarValue::iter_to_array(
        //     (0..array.len())
        //         .map(|_| ScalarValue::Null)
        //         .map(op)
        //         .collect()?,
        // )?,
        DataType::Decimal128(precision, scale) => ScalarValue::iter_to_array(
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or(ArrowError::CastError("decimal128".to_string()))?
                .iter()
                .map(|v| ScalarValue::Decimal128(v, *precision, *scale))
                .map(op)
                .)()?,
        )?,
        // DataType::Decimal256(precision, scale) => {
        //     ScalarValue::get_decimal_value_from_array(array, index, *precision, *scale)?
        // }
        // DataType::Boolean => typed_cast!(array, index, BooleanArray, Boolean),
        // DataType::Float64 => typed_cast!(array, index, Float64Array, Float64),
        // DataType::Float32 => typed_cast!(array, index, Float32Array, Float32),
        // DataType::UInt64 => typed_cast!(array, index, UInt64Array, UInt64),
        // DataType::UInt32 => typed_cast!(array, index, UInt32Array, UInt32),
        // DataType::UInt16 => typed_cast!(array, index, UInt16Array, UInt16),
        // DataType::UInt8 => typed_cast!(array, index, UInt8Array, UInt8),
        // DataType::Int64 => typed_cast!(array, index, Int64Array, Int64),
        // DataType::Int32 => typed_cast!(array, index, Int32Array, Int32),
        // DataType::Int16 => typed_cast!(array, index, Int16Array, Int16),
        // DataType::Int8 => typed_cast!(array, index, Int8Array, Int8),
        // DataType::Binary => typed_cast!(array, index, BinaryArray, Binary),
        // DataType::LargeBinary => {
        //     typed_cast!(array, index, LargeBinaryArray, LargeBinary)
        // }
        // DataType::Utf8 => typed_cast!(array, index, StringArray, Utf8),
        // DataType::LargeUtf8 => typed_cast!(array, index, LargeStringArray, LargeUtf8),
        // DataType::List(nested_type) => {
        //     let list_array = as_list_array(array)?;
        //     let value = match list_array.is_null(index) {
        //         true => None,
        //         false => {
        //             let nested_array = list_array.value(index);
        //             let scalar_vec = (0..nested_array.len())
        //                 .map(|i| ScalarValue::try_from_array(&nested_array, i))
        //                 .collect::<Result<Vec<_>>>()?;
        //             Some(scalar_vec)
        //         }
        //     };
        //     ScalarValue::new_list(value, nested_type.data_type().clone())
        // }
        // DataType::Date32 => {
        //     typed_cast!(array, index, Date32Array, Date32)
        // }
        // DataType::Date64 => {
        //     typed_cast!(array, index, Date64Array, Date64)
        // }
        // DataType::Time32(TimeUnit::Second) => {
        //     typed_cast!(array, index, Time32SecondArray, Time32Second)
        // }
        // DataType::Time32(TimeUnit::Millisecond) => {
        //     typed_cast!(array, index, Time32MillisecondArray, Time32Millisecond)
        // }
        // DataType::Time64(TimeUnit::Microsecond) => {
        //     typed_cast!(array, index, Time64MicrosecondArray, Time64Microsecond)
        // }
        // DataType::Time64(TimeUnit::Nanosecond) => {
        //     typed_cast!(array, index, Time64NanosecondArray, Time64Nanosecond)
        // }
        // DataType::Timestamp(TimeUnit::Second, tz_opt) => {
        //     typed_cast_tz!(array, index, TimestampSecondArray, TimestampSecond, tz_opt)
        // }
        // DataType::Timestamp(TimeUnit::Millisecond, tz_opt) => {
        //     typed_cast_tz!(
        //         array,
        //         index,
        //         TimestampMillisecondArray,
        //         TimestampMillisecond,
        //         tz_opt
        //     )
        // }
        // DataType::Timestamp(TimeUnit::Microsecond, tz_opt) => {
        //     typed_cast_tz!(
        //         array,
        //         index,
        //         TimestampMicrosecondArray,
        //         TimestampMicrosecond,
        //         tz_opt
        //     )
        // }
        // DataType::Timestamp(TimeUnit::Nanosecond, tz_opt) => {
        //     typed_cast_tz!(
        //         array,
        //         index,
        //         TimestampNanosecondArray,
        //         TimestampNanosecond,
        //         tz_opt
        //     )
        // }
        // DataType::Dictionary(key_type, _) => {
        //     let (values_array, values_index) = match key_type.as_ref() {
        //         DataType::Int8 => get_dict_value::<Int8Type>(array, index),
        //         DataType::Int16 => get_dict_value::<Int16Type>(array, index),
        //         DataType::Int32 => get_dict_value::<Int32Type>(array, index),
        //         DataType::Int64 => get_dict_value::<Int64Type>(array, index),
        //         DataType::UInt8 => get_dict_value::<UInt8Type>(array, index),
        //         DataType::UInt16 => get_dict_value::<UInt16Type>(array, index),
        //         DataType::UInt32 => get_dict_value::<UInt32Type>(array, index),
        //         DataType::UInt64 => get_dict_value::<UInt64Type>(array, index),
        //         _ => unreachable!("Invalid dictionary keys type: {:?}", key_type),
        //     };
        //     // look up the index in the values dictionary
        //     let value = match values_index {
        //         Some(values_index) => ScalarValue::try_from_array(values_array, values_index),
        //         // else entry was null, so return null
        //         None => values_array.data_type().try_into(),
        //     }?;

        //     Self::Dictionary(key_type.clone(), Box::new(value))
        // }
        // DataType::Struct(fields) => {
        //     let array = as_struct_array(array)?;
        //     let mut field_values: Vec<ScalarValue> = Vec::new();
        //     for col_index in 0..array.num_columns() {
        //         let col_array = array.column(col_index);
        //         let col_scalar = ScalarValue::try_from_array(col_array, index)?;
        //         field_values.push(col_scalar);
        //     }
        //     Self::Struct(Some(field_values), fields.clone())
        // }
        // DataType::FixedSizeList(nested_type, _len) => {
        //     let list_array = as_fixed_size_list_array(array)?;
        //     let value = match list_array.is_null(index) {
        //         true => None,
        //         false => {
        //             let nested_array = list_array.value(index);
        //             let scalar_vec = (0..nested_array.len())
        //                 .map(|i| ScalarValue::try_from_array(&nested_array, i))
        //                 .collect::<Result<Vec<_>>>()?;
        //             Some(scalar_vec)
        //         }
        //     };
        //     ScalarValue::new_list(value, nested_type.data_type().clone())
        // }
        // DataType::FixedSizeBinary(_) => {
        //     let array = as_fixed_size_binary_array(array)?;
        //     let size = match array.data_type() {
        //         DataType::FixedSizeBinary(size) => *size,
        //         _ => unreachable!(),
        //     };
        //     ScalarValue::FixedSizeBinary(
        //         size,
        //         match array.is_null(index) {
        //             true => None,
        //             false => Some(array.value(index).into()),
        //         },
        //     )
        // }
        // DataType::Interval(IntervalUnit::DayTime) => {
        //     typed_cast!(array, index, IntervalDayTimeArray, IntervalDayTime)
        // }
        // DataType::Interval(IntervalUnit::YearMonth) => {
        //     typed_cast!(array, index, IntervalYearMonthArray, IntervalYearMonth)
        // }
        // DataType::Interval(IntervalUnit::MonthDayNano) => {
        //     typed_cast!(
        //         array,
        //         index,
        //         IntervalMonthDayNanoArray,
        //         IntervalMonthDayNano
        //     )
        // }

        // DataType::Duration(TimeUnit::Second) => {
        //     typed_cast!(array, index, DurationSecondArray, DurationSecond)
        // }
        // DataType::Duration(TimeUnit::Millisecond) => {
        //     typed_cast!(array, index, DurationMillisecondArray, DurationMillisecond)
        // }
        // DataType::Duration(TimeUnit::Microsecond) => {
        //     typed_cast!(array, index, DurationMicrosecondArray, DurationMicrosecond)
        // }
        // DataType::Duration(TimeUnit::Nanosecond) => {
        //     typed_cast!(array, index, DurationNanosecondArray, DurationNanosecond)
        // }
        other => {
            return Err(ArrowError::CastError(
                format!("Can't create a scalar from array of type \"{other:?}\"",).to_string(),
            ));
        }
    })
}
