use datafusion::arrow::array::{
    new_empty_array, new_null_array, Array, ArrayDataBuilder, ArrayRef, BinaryArray, BooleanArray,
    BooleanBufferBuilder, BooleanBuilder, Date32Array, Date64Array, Decimal128Array,
    Decimal256Array, DictionaryArray, FixedSizeBinaryArray, Float32Array, Float64Array,
    GenericListArray, Int16Array, Int32Array, Int64Array, Int8Array, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray, LargeStringArray,
    LargeStringBuilder, ListArray, ListBuilder, PrimitiveArray, StringArray, StringBuilder,
    StructArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::nullif;
use datafusion::arrow::datatypes::ArrowNativeType;
use datafusion::arrow::datatypes::{
    ArrowDictionaryKeyType, DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, IntervalUnit, TimeUnit, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use datafusion::error::DataFusionError;
use datafusion::scalar::ScalarValue;
use std::sync::Arc;

use crate::errors::ExtensionError;

pub fn scalar_iter_to_array(
    data_type: &DataType,
    scalars: impl IntoIterator<Item = Result<ScalarValue, ExtensionError>>,
) -> Result<ArrayRef, ExtensionError> {
    let scalars = scalars.into_iter();

    /// Creates an array of $ARRAY_TY by unpacking values of
    /// SCALAR_TY for primitive types
    macro_rules! build_array_primitive {
        ($ARRAY_TY:ident, $SCALAR_TY:ident) => {{
            {
                let array = scalars
                    .map(|sv| {
                        let sv = sv?;
                        if let ScalarValue::$SCALAR_TY(v) = sv {
                            Ok(v)
                        } else {
                            Err(ExtensionError::String(format!(
                                "Inconsistent types in scalar_iter_to_array. \
                                    Expected {:?}, got {:?}",
                                data_type, sv
                            )))
                        }
                    })
                    .collect::<Result<$ARRAY_TY, ExtensionError>>()?;
                Arc::new(array)
            }
        }};
    }

    macro_rules! build_array_primitive_tz {
        ($ARRAY_TY:ident, $SCALAR_TY:ident, $TZ:expr) => {{
            {
                let array = scalars
                    .map(|sv| {
                        let sv = sv?;
                        if let ScalarValue::$SCALAR_TY(v, _) = sv {
                            Ok(v)
                        } else {
                            Err(ExtensionError::String(format!(
                                "Inconsistent types in scalar_iter_to_array. \
                                    Expected {:?}, got {:?}",
                                data_type, sv
                            )))
                        }
                    })
                    .collect::<Result<$ARRAY_TY, ExtensionError>>()?;
                Arc::new(array.with_timezone_opt($TZ.clone()))
            }
        }};
    }

    /// Creates an array of $ARRAY_TY by unpacking values of
    /// SCALAR_TY for "string-like" types.
    macro_rules! build_array_string {
        ($ARRAY_TY:ident, $SCALAR_TY:ident) => {{
            {
                let array = scalars
                    .map(|sv| {
                        let sv = sv?;
                        if let ScalarValue::$SCALAR_TY(v) = sv {
                            Ok(v)
                        } else {
                            return Err(ExtensionError::String(format!(
                                "Inconsistent types in scalar_iter_to_array. \
                                    Expected {:?}, got {:?}",
                                data_type, sv,
                            )));
                        }
                    })
                    .collect::<Result<$ARRAY_TY, ExtensionError>>()?;
                Arc::new(array)
            }
        }};
    }

    macro_rules! build_array_list_primitive {
        ($ARRAY_TY:ident, $SCALAR_TY:ident, $NATIVE_TYPE:ident) => {{
            Arc::new(ListArray::from_iter_primitive::<$ARRAY_TY, _, _>(
                scalars
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .map(|x| match x {
                        ScalarValue::List(xs, _) => xs.map(|x| {
                            x.iter()
                                .map(|x| match x {
                                    ScalarValue::$SCALAR_TY(i) => *i,
                                    sv => panic!(
                                        "Inconsistent types in scalar_iter_to_array. \
                                        Expected {:?}, got {:?}",
                                        data_type, sv
                                    ),
                                })
                                .collect::<Vec<Option<$NATIVE_TYPE>>>()
                        }),
                        sv => panic!(
                            "Inconsistent types in scalar_iter_to_array. \
                                Expected {:?}, got {:?}",
                            data_type, sv
                        ),
                    }),
            ))
        }};
    }

    macro_rules! build_array_list_string {
        ($BUILDER:ident, $SCALAR_TY:ident) => {{
            let mut builder = ListBuilder::new($BUILDER::new());
            for scalar in scalars.into_iter() {
                match scalar? {
                    ScalarValue::List(Some(xs), _) => {
                        for s in xs {
                            match s {
                                ScalarValue::$SCALAR_TY(Some(val)) => {
                                    builder.values().append_value(val);
                                }
                                ScalarValue::$SCALAR_TY(None) => {
                                    builder.values().append_null();
                                }
                                sv => {
                                    return Err(ExtensionError::String(format!(
                                        "Inconsistent types in scalar_iter_to_array. \
                                                Expected Utf8, got {:?}",
                                        sv
                                    )))
                                }
                            }
                        }
                        builder.append(true);
                    }
                    ScalarValue::List(None, _) => {
                        builder.append(false);
                    }
                    sv => {
                        return Err(ExtensionError::String(format!(
                            "Inconsistent types in scalar_iter_to_array. \
                                    Expected List, got {:?}",
                            sv
                        )));
                    }
                }
            }
            Arc::new(builder.finish())
        }};
    }

    let array: ArrayRef = match &data_type {
        DataType::Decimal128(precision, scale) => {
            let decimal_array = iter_to_decimal_array(scalars, *precision, *scale)?;
            Arc::new(decimal_array)
        }
        DataType::Decimal256(precision, scale) => {
            let decimal_array = iter_to_decimal256_array(scalars, *precision, *scale)?;
            Arc::new(decimal_array)
        }
        DataType::Null => iter_to_null_array(scalars),
        DataType::Boolean => build_array_primitive!(BooleanArray, Boolean),
        DataType::Float32 => build_array_primitive!(Float32Array, Float32),
        DataType::Float64 => build_array_primitive!(Float64Array, Float64),
        DataType::Int8 => build_array_primitive!(Int8Array, Int8),
        DataType::Int16 => build_array_primitive!(Int16Array, Int16),
        DataType::Int32 => build_array_primitive!(Int32Array, Int32),
        DataType::Int64 => build_array_primitive!(Int64Array, Int64),
        DataType::UInt8 => build_array_primitive!(UInt8Array, UInt8),
        DataType::UInt16 => build_array_primitive!(UInt16Array, UInt16),
        DataType::UInt32 => build_array_primitive!(UInt32Array, UInt32),
        DataType::UInt64 => build_array_primitive!(UInt64Array, UInt64),
        DataType::Utf8 => build_array_string!(StringArray, Utf8),
        DataType::LargeUtf8 => build_array_string!(LargeStringArray, LargeUtf8),
        DataType::Binary => build_array_string!(BinaryArray, Binary),
        DataType::LargeBinary => build_array_string!(LargeBinaryArray, LargeBinary),
        DataType::Date32 => build_array_primitive!(Date32Array, Date32),
        DataType::Date64 => build_array_primitive!(Date64Array, Date64),
        DataType::Time32(TimeUnit::Second) => {
            build_array_primitive!(Time32SecondArray, Time32Second)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            build_array_primitive!(Time32MillisecondArray, Time32Millisecond)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            build_array_primitive!(Time64MicrosecondArray, Time64Microsecond)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            build_array_primitive!(Time64NanosecondArray, Time64Nanosecond)
        }
        DataType::Timestamp(TimeUnit::Second, tz) => {
            build_array_primitive_tz!(TimestampSecondArray, TimestampSecond, tz)
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            build_array_primitive_tz!(TimestampMillisecondArray, TimestampMillisecond, tz)
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            build_array_primitive_tz!(TimestampMicrosecondArray, TimestampMicrosecond, tz)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            build_array_primitive_tz!(TimestampNanosecondArray, TimestampNanosecond, tz)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            build_array_primitive!(IntervalDayTimeArray, IntervalDayTime)
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            build_array_primitive!(IntervalYearMonthArray, IntervalYearMonth)
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            build_array_primitive!(IntervalMonthDayNanoArray, IntervalMonthDayNano)
        }
        DataType::List(fields) if fields.data_type() == &DataType::Int8 => {
            build_array_list_primitive!(Int8Type, Int8, i8)
        }
        DataType::List(fields) if fields.data_type() == &DataType::Int16 => {
            build_array_list_primitive!(Int16Type, Int16, i16)
        }
        DataType::List(fields) if fields.data_type() == &DataType::Int32 => {
            build_array_list_primitive!(Int32Type, Int32, i32)
        }
        DataType::List(fields) if fields.data_type() == &DataType::Int64 => {
            build_array_list_primitive!(Int64Type, Int64, i64)
        }
        DataType::List(fields) if fields.data_type() == &DataType::UInt8 => {
            build_array_list_primitive!(UInt8Type, UInt8, u8)
        }
        DataType::List(fields) if fields.data_type() == &DataType::UInt16 => {
            build_array_list_primitive!(UInt16Type, UInt16, u16)
        }
        DataType::List(fields) if fields.data_type() == &DataType::UInt32 => {
            build_array_list_primitive!(UInt32Type, UInt32, u32)
        }
        DataType::List(fields) if fields.data_type() == &DataType::UInt64 => {
            build_array_list_primitive!(UInt64Type, UInt64, u64)
        }
        DataType::List(fields) if fields.data_type() == &DataType::Float32 => {
            build_array_list_primitive!(Float32Type, Float32, f32)
        }
        DataType::List(fields) if fields.data_type() == &DataType::Float64 => {
            build_array_list_primitive!(Float64Type, Float64, f64)
        }
        DataType::List(fields) if fields.data_type() == &DataType::Utf8 => {
            build_array_list_string!(StringBuilder, Utf8)
        }
        DataType::List(fields) if fields.data_type() == &DataType::LargeUtf8 => {
            build_array_list_string!(LargeStringBuilder, LargeUtf8)
        }
        DataType::List(_) => {
            // Fallback case handling homogeneous lists with any ScalarValue element type
            let list_array = iter_to_array_list(scalars, &data_type)?;
            Arc::new(list_array)
        }
        DataType::Struct(fields) => {
            // Initialize a Vector to store the ScalarValues for each column
            let mut columns: Vec<Vec<ScalarValue>> =
                (0..fields.len()).map(|_| Vec::new()).collect();

            // null mask
            let mut null_mask_builder = BooleanBuilder::new();

            // Iterate over scalars to populate the column scalars for each row
            for scalar in scalars {
                let scalar = scalar?;
                if let ScalarValue::Struct(values, fields) = scalar {
                    match values {
                        Some(values) => {
                            // Push value for each field
                            for (column, value) in columns.iter_mut().zip(values) {
                                column.push(value.clone());
                            }
                            null_mask_builder.append_value(false);
                        }
                        None => {
                            // Push NULL of the appropriate type for each field
                            for (column, field) in columns.iter_mut().zip(fields.as_ref()) {
                                column.push(ScalarValue::try_from(field.data_type())?);
                            }
                            null_mask_builder.append_value(true);
                        }
                    };
                } else {
                    return Err(ExtensionError::String(format!(
                        "Expected Struct but found: {scalar}"
                    )));
                };
            }

            // Call iter_to_array recursively to convert the scalars for each column into Arrow arrays
            let field_values = fields
                .iter()
                .zip(columns)
                .map(|(field, column)| {
                    Ok((
                        field.clone(),
                        ScalarValue::iter_to_array(column.iter().map(|v| v.to_owned()))?,
                    ))
                })
                .collect::<Result<Vec<_>, ExtensionError>>()?;

            let array = StructArray::from(field_values);
            nullif(&array, &null_mask_builder.finish())?
        }
        DataType::Dictionary(key_type, value_type) => {
            // create the values array
            let value_scalars = scalars
                .map(|scalar| {
                    let scalar = scalar?;
                    match scalar {
                        ScalarValue::Dictionary(inner_key_type, scalar) => {
                            if &inner_key_type == key_type {
                                Ok(*scalar)
                            } else {
                                panic!("Expected inner key type of {key_type} but found: {inner_key_type}, value was ({scalar:?})");
                            }
                        }
                        _ => {
                            Err(ExtensionError::String(format!(
                                "Expected scalar of type {value_type} but found: {} {:?}", scalar,scalar
                            )))
                        }
                    }})
                    .collect::<Result<Vec<_>, ExtensionError>>()?;

            let values = ScalarValue::iter_to_array(value_scalars)?;
            assert_eq!(values.data_type(), value_type.as_ref());

            match key_type.as_ref() {
                DataType::Int8 => dict_from_values::<Int8Type>(values)?,
                DataType::Int16 => dict_from_values::<Int16Type>(values)?,
                DataType::Int32 => dict_from_values::<Int32Type>(values)?,
                DataType::Int64 => dict_from_values::<Int64Type>(values)?,
                DataType::UInt8 => dict_from_values::<UInt8Type>(values)?,
                DataType::UInt16 => dict_from_values::<UInt16Type>(values)?,
                DataType::UInt32 => dict_from_values::<UInt32Type>(values)?,
                DataType::UInt64 => dict_from_values::<UInt64Type>(values)?,
                _ => unreachable!("Invalid dictionary keys type: {:?}", key_type),
            }
        }
        DataType::FixedSizeBinary(size) => {
            let array = scalars
                .map(|sv| {
                    let sv = sv?;
                    if let ScalarValue::FixedSizeBinary(_, v) = sv {
                        Ok(v)
                    } else {
                        return Err(ExtensionError::String(format!(
                            "Inconsistent types in scalar_iter_to_array. \
                                Expected {data_type:?}, got {sv:?}"
                        )));
                    }
                })
                .collect::<Result<Vec<_>, ExtensionError>>()?;
            let array =
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(array.into_iter(), *size)?;
            Arc::new(array)
        }
        // explicitly enumerate unsupported types so newly added
        // types must be aknowledged, Time32 and Time64 types are
        // not supported if the TimeUnit is not valid (Time32 can
        // only be used with Second and Millisecond, Time64 only
        // with Microsecond and Nanosecond)
        DataType::Float16
        | DataType::Time32(TimeUnit::Microsecond)
        | DataType::Time32(TimeUnit::Nanosecond)
        | DataType::Time64(TimeUnit::Second)
        | DataType::Time64(TimeUnit::Millisecond)
        | DataType::Duration(_)
        | DataType::FixedSizeList(_, _)
        | DataType::LargeList(_)
        | DataType::Union(_, _)
        | DataType::Map(_, _)
        | DataType::RunEndEncoded(_, _) => {
            return Err(ExtensionError::String("unsupported type".to_string()))
        }
    };

    Ok(array)
}

fn dict_from_values<K: ArrowDictionaryKeyType>(
    values_array: ArrayRef,
) -> Result<ArrayRef, DataFusionError> {
    // Create a key array with `size` elements of 0..array_len for all
    // non-null value elements
    let key_array: PrimitiveArray<K> = (0..values_array.len())
        .map(|index| {
            if values_array.is_valid(index) {
                let native_index = K::Native::from_usize(index).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Can not create index of type {} from value {}",
                        K::DATA_TYPE,
                        index
                    ))
                })?;
                Ok(Some(native_index))
            } else {
                Ok(None)
            }
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?
        .into_iter()
        .collect();

    // create a new DictionaryArray
    //
    // Note: this path could be made faster by using the ArrayData
    // APIs and skipping validation, if it every comes up in
    // performance traces.
    let dict_array = DictionaryArray::<K>::try_new(key_array, values_array)?;
    Ok(Arc::new(dict_array))
}

fn iter_to_array_list(
    scalars: impl IntoIterator<Item = Result<ScalarValue, ExtensionError>>,
    data_type: &DataType,
) -> Result<GenericListArray<i32>, ExtensionError> {
    let mut offsets = Int32Array::builder(0);
    offsets.append_value(0);

    let mut elements: Vec<ArrayRef> = Vec::new();
    let mut valid = BooleanBufferBuilder::new(0);
    let mut flat_len = 0i32;
    for scalar in scalars {
        let scalar = scalar?;
        if let ScalarValue::List(values, field) = scalar {
            match values {
                Some(values) => {
                    let element_array = if !values.is_empty() {
                        ScalarValue::iter_to_array(values.iter().map(|v| v.to_owned()))?
                    } else {
                        new_empty_array(field.data_type())
                    };

                    // Add new offset index
                    flat_len += element_array.len() as i32;
                    offsets.append_value(flat_len);

                    elements.push(element_array);

                    // Element is valid
                    valid.append(true);
                }
                None => {
                    // Repeat previous offset index
                    offsets.append_value(flat_len);

                    // Element is null
                    valid.append(false);
                }
            }
        } else {
            return Err(ExtensionError::String(format!(
                "Expected ScalarValue::List element. Received {scalar:?}"
            )));
        }
    }

    // Concatenate element arrays to create single flat array
    let element_arrays: Vec<&dyn Array> = elements.iter().map(|a| a.as_ref()).collect();
    let flat_array = match datafusion::arrow::compute::concat(&element_arrays) {
        Ok(flat_array) => flat_array,
        Err(err) => return Ok(Err(DataFusionError::ArrowError(err))?),
    };

    // Build ListArray using ArrayData so we can specify a flat inner array, and offset indices
    let offsets_array = offsets.finish();
    let array_data = ArrayDataBuilder::new(data_type.clone())
        .len(offsets_array.len() - 1)
        .nulls(Some(NullBuffer::new(valid.finish())))
        .add_buffer(offsets_array.values().inner().clone())
        .add_child_data(flat_array.to_data());

    let list_array = ListArray::from(array_data.build()?);
    Ok(list_array)
}

fn iter_to_decimal_array(
    scalars: impl IntoIterator<Item = Result<ScalarValue, ExtensionError>>,
    precision: u8,
    scale: i8,
) -> Result<Decimal128Array, ExtensionError> {
    let array = scalars
        .into_iter()
        .map(
            |element: Result<ScalarValue, ExtensionError>| match element? {
                ScalarValue::Decimal128(v1, _, _) => Ok(v1),
                _ => unreachable!(),
            },
        )
        .collect::<Result<Decimal128Array, ExtensionError>>()?
        .with_precision_and_scale(precision, scale)?;
    Ok(array)
}

fn iter_to_decimal256_array(
    scalars: impl IntoIterator<Item = Result<ScalarValue, ExtensionError>>,
    precision: u8,
    scale: i8,
) -> Result<Decimal256Array, ExtensionError> {
    let array = scalars
        .into_iter()
        .map(
            |element: Result<ScalarValue, ExtensionError>| match element? {
                ScalarValue::Decimal256(v1, _, _) => Ok(v1),
                _ => unreachable!(),
            },
        )
        .collect::<Result<Decimal256Array, ExtensionError>>()?
        .with_precision_and_scale(precision, scale)?;
    Ok(array)
}

fn iter_to_null_array(
    scalars: impl IntoIterator<Item = Result<ScalarValue, ExtensionError>>,
) -> ArrayRef {
    let length =
        scalars
            .into_iter()
            .fold(
                0usize,
                |r, element: Result<ScalarValue, ExtensionError>| match element {
                    Ok(ScalarValue::Null) => r + 1,
                    _ => unreachable!(),
                },
            );
    new_null_array(&DataType::Null, length)
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::sync::Arc;

    use chrono::NaiveDate;
    use datafusion::arrow::array::{
        ArrowNumericType, AsArray, PrimitiveBuilder, StringBuilder, StructBuilder,
    };
    use datafusion::arrow::compute::kernels;
    use datafusion::arrow::datatypes::{ArrowPrimitiveType, Field};
    use rand::Rng;

    use datafusion::common::cast::{
        as_decimal128_array, as_dictionary_array, as_list_array, as_string_array, as_struct_array,
        as_uint32_array, as_uint64_array,
    };
    use std::collections::HashSet;

    use super::*;
    use datafusion::common::*;

    #[test]
    fn scalar_add_trait_test() -> Result<()> {
        let float_value = ScalarValue::Float64(Some(123.));
        let float_value_2 = ScalarValue::Float64(Some(123.));
        assert_eq!(
            (float_value.add(&float_value_2))?,
            ScalarValue::Float64(Some(246.))
        );
        assert_eq!(
            (float_value.add(float_value_2))?,
            ScalarValue::Float64(Some(246.))
        );
        Ok(())
    }

    #[test]
    fn scalar_sub_trait_test() -> Result<()> {
        let float_value = ScalarValue::Float64(Some(123.));
        let float_value_2 = ScalarValue::Float64(Some(123.));
        assert_eq!(
            float_value.sub(&float_value_2)?,
            ScalarValue::Float64(Some(0.))
        );
        assert_eq!(
            float_value.sub(float_value_2)?,
            ScalarValue::Float64(Some(0.))
        );
        Ok(())
    }

    #[test]
    fn scalar_sub_trait_int32_test() -> Result<()> {
        let int_value = ScalarValue::Int32(Some(42));
        let int_value_2 = ScalarValue::Int32(Some(100));
        assert_eq!(int_value.sub(&int_value_2)?, ScalarValue::Int32(Some(-58)));
        assert_eq!(int_value_2.sub(int_value)?, ScalarValue::Int32(Some(58)));
        Ok(())
    }

    #[test]
    fn scalar_sub_trait_int32_overflow_test() {
        let int_value = ScalarValue::Int32(Some(i32::MAX));
        let int_value_2 = ScalarValue::Int32(Some(i32::MIN));
        let err = int_value
            .sub_checked(&int_value_2)
            .unwrap_err()
            .strip_backtrace();
        assert_eq!(
            err,
            "Arrow error: Compute error: Overflow happened on: 2147483647 - -2147483648"
        )
    }

    #[test]
    fn scalar_sub_trait_int64_test() -> Result<()> {
        let int_value = ScalarValue::Int64(Some(42));
        let int_value_2 = ScalarValue::Int64(Some(100));
        assert_eq!(int_value.sub(&int_value_2)?, ScalarValue::Int64(Some(-58)));
        assert_eq!(int_value_2.sub(int_value)?, ScalarValue::Int64(Some(58)));
        Ok(())
    }

    #[test]
    fn scalar_sub_trait_int64_overflow_test() {
        let int_value = ScalarValue::Int64(Some(i64::MAX));
        let int_value_2 = ScalarValue::Int64(Some(i64::MIN));
        let err = int_value
            .sub_checked(&int_value_2)
            .unwrap_err()
            .strip_backtrace();
        assert_eq!(err, "Arrow error: Compute error: Overflow happened on: 9223372036854775807 - -9223372036854775808")
    }

    #[test]
    fn scalar_add_overflow_test() -> Result<()> {
        check_scalar_add_overflow::<Int8Type>(
            ScalarValue::Int8(Some(i8::MAX)),
            ScalarValue::Int8(Some(i8::MAX)),
        );
        check_scalar_add_overflow::<UInt8Type>(
            ScalarValue::UInt8(Some(u8::MAX)),
            ScalarValue::UInt8(Some(u8::MAX)),
        );
        check_scalar_add_overflow::<Int16Type>(
            ScalarValue::Int16(Some(i16::MAX)),
            ScalarValue::Int16(Some(i16::MAX)),
        );
        check_scalar_add_overflow::<UInt16Type>(
            ScalarValue::UInt16(Some(u16::MAX)),
            ScalarValue::UInt16(Some(u16::MAX)),
        );
        check_scalar_add_overflow::<Int32Type>(
            ScalarValue::Int32(Some(i32::MAX)),
            ScalarValue::Int32(Some(i32::MAX)),
        );
        check_scalar_add_overflow::<UInt32Type>(
            ScalarValue::UInt32(Some(u32::MAX)),
            ScalarValue::UInt32(Some(u32::MAX)),
        );
        check_scalar_add_overflow::<Int64Type>(
            ScalarValue::Int64(Some(i64::MAX)),
            ScalarValue::Int64(Some(i64::MAX)),
        );
        check_scalar_add_overflow::<UInt64Type>(
            ScalarValue::UInt64(Some(u64::MAX)),
            ScalarValue::UInt64(Some(u64::MAX)),
        );

        Ok(())
    }

    // Verifies that ScalarValue has the same behavior with compute kernal when it overflows.
    fn check_scalar_add_overflow<T>(left: ScalarValue, right: ScalarValue)
    where
        T: ArrowNumericType,
    {
        let scalar_result = left.add_checked(&right);

        let left_array = left.to_array();
        let right_array = right.to_array();
        let arrow_left_array = left_array.as_primitive::<T>();
        let arrow_right_array = right_array.as_primitive::<T>();
        let arrow_result = kernels::numeric::add(arrow_left_array, arrow_right_array);

        assert_eq!(scalar_result.is_ok(), arrow_result.is_ok());
    }

    #[test]
    fn test_interval_add_timestamp() -> Result<()> {
        let interval = ScalarValue::IntervalMonthDayNano(Some(123));
        let timestamp = ScalarValue::TimestampNanosecond(Some(123), None);
        let result = interval.add(&timestamp)?;
        let expect = timestamp.add(&interval)?;
        assert_eq!(result, expect);

        let interval = ScalarValue::IntervalYearMonth(Some(123));
        let timestamp = ScalarValue::TimestampNanosecond(Some(123), None);
        let result = interval.add(&timestamp)?;
        let expect = timestamp.add(&interval)?;
        assert_eq!(result, expect);

        let interval = ScalarValue::IntervalDayTime(Some(123));
        let timestamp = ScalarValue::TimestampNanosecond(Some(123), None);
        let result = interval.add(&timestamp)?;
        let expect = timestamp.add(&interval)?;
        assert_eq!(result, expect);
        Ok(())
    }

    #[test]
    fn scalar_decimal_test() -> Result<()> {
        let decimal_value = ScalarValue::Decimal128(Some(123), 10, 1);
        assert_eq!(DataType::Decimal128(10, 1), decimal_value.data_type());
        let try_into_value: i128 = decimal_value.clone().try_into().unwrap();
        assert_eq!(123_i128, try_into_value);
        assert!(!decimal_value.is_null());
        let neg_decimal_value = decimal_value.arithmetic_negate()?;
        match neg_decimal_value {
            ScalarValue::Decimal128(v, _, _) => {
                assert_eq!(-123, v.unwrap());
            }
            _ => {
                unreachable!();
            }
        }

        // decimal scalar to array
        let array = decimal_value.to_array();
        let array = as_decimal128_array(&array)?;
        assert_eq!(1, array.len());
        assert_eq!(DataType::Decimal128(10, 1), array.data_type().clone());
        assert_eq!(123i128, array.value(0));

        // decimal scalar to array with size
        let array = decimal_value.to_array_of_size(10);
        let array_decimal = as_decimal128_array(&array)?;
        assert_eq!(10, array.len());
        assert_eq!(DataType::Decimal128(10, 1), array.data_type().clone());
        assert_eq!(123i128, array_decimal.value(0));
        assert_eq!(123i128, array_decimal.value(9));
        // test eq array
        assert!(decimal_value.eq_array(&array, 1));
        assert!(decimal_value.eq_array(&array, 5));
        // test try from array
        assert_eq!(
            decimal_value,
            ScalarValue::try_from_array(&array, 5).unwrap()
        );

        assert_eq!(
            decimal_value,
            ScalarValue::try_new_decimal128(123, 10, 1).unwrap()
        );

        // test compare
        let left = ScalarValue::Decimal128(Some(123), 10, 2);
        let right = ScalarValue::Decimal128(Some(124), 10, 2);
        assert!(!left.eq(&right));
        let result = left < right;
        assert!(result);
        let result = left <= right;
        assert!(result);
        let right = ScalarValue::Decimal128(Some(124), 10, 3);
        // make sure that two decimals with diff datatype can't be compared.
        let result = left.partial_cmp(&right);
        assert_eq!(None, result);

        let decimal_vec = vec![
            ScalarValue::Decimal128(Some(1), 10, 2),
            ScalarValue::Decimal128(Some(2), 10, 2),
            ScalarValue::Decimal128(Some(3), 10, 2),
        ];
        // convert the vec to decimal array and check the result
        let array = scalar_iter_to_array(
            &DataType::Decimal128(10, 2),
            decimal_vec.iter().map(|v| v.to_owned()).map(Ok),
        )
        .unwrap();
        assert_eq!(3, array.len());
        assert_eq!(DataType::Decimal128(10, 2), array.data_type().clone());

        let decimal_vec = vec![
            ScalarValue::Decimal128(Some(1), 10, 2),
            ScalarValue::Decimal128(Some(2), 10, 2),
            ScalarValue::Decimal128(Some(3), 10, 2),
            ScalarValue::Decimal128(None, 10, 2),
        ];
        let array = scalar_iter_to_array(
            &DataType::Decimal128(10, 2),
            decimal_vec.iter().map(|v| v.to_owned()).map(Ok),
        )
        .unwrap();
        assert_eq!(4, array.len());
        assert_eq!(DataType::Decimal128(10, 2), array.data_type().clone());

        assert!(ScalarValue::try_new_decimal128(1, 10, 2)
            .unwrap()
            .eq_array(&array, 0));
        assert!(ScalarValue::try_new_decimal128(2, 10, 2)
            .unwrap()
            .eq_array(&array, 1));
        assert!(ScalarValue::try_new_decimal128(3, 10, 2)
            .unwrap()
            .eq_array(&array, 2));
        assert_eq!(
            ScalarValue::Decimal128(None, 10, 2),
            ScalarValue::try_from_array(&array, 3).unwrap()
        );

        Ok(())
    }

    #[test]
    fn scalar_value_to_array_u64() -> Result<()> {
        let value = ScalarValue::UInt64(Some(13u64));
        let array = value.to_array();
        let array = as_uint64_array(&array)?;
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        assert_eq!(array.value(0), 13);

        let value = ScalarValue::UInt64(None);
        let array = value.to_array();
        let array = as_uint64_array(&array)?;
        assert_eq!(array.len(), 1);
        assert!(array.is_null(0));
        Ok(())
    }

    #[test]
    fn scalar_value_to_array_u32() -> Result<()> {
        let value = ScalarValue::UInt32(Some(13u32));
        let array = value.to_array();
        let array = as_uint32_array(&array)?;
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        assert_eq!(array.value(0), 13);

        let value = ScalarValue::UInt32(None);
        let array = value.to_array();
        let array = as_uint32_array(&array)?;
        assert_eq!(array.len(), 1);
        assert!(array.is_null(0));
        Ok(())
    }

    #[test]
    fn scalar_list_null_to_array() {
        let list_array_ref =
            ScalarValue::List(None, Arc::new(Field::new("item", DataType::UInt64, false)))
                .to_array();
        let list_array = as_list_array(&list_array_ref).unwrap();

        assert!(list_array.is_null(0));
        assert_eq!(list_array.len(), 1);
        assert_eq!(list_array.values().len(), 0);
    }

    #[test]
    fn scalar_list_to_array() -> Result<()> {
        let list_array_ref = ScalarValue::List(
            Some(vec![
                ScalarValue::UInt64(Some(100)),
                ScalarValue::UInt64(None),
                ScalarValue::UInt64(Some(101)),
            ]),
            Arc::new(Field::new("item", DataType::UInt64, false)),
        )
        .to_array();

        let list_array = as_list_array(&list_array_ref)?;
        assert_eq!(list_array.len(), 1);
        assert_eq!(list_array.values().len(), 3);

        let prim_array_ref = list_array.value(0);
        let prim_array = as_uint64_array(&prim_array_ref)?;
        assert_eq!(prim_array.len(), 3);
        assert_eq!(prim_array.value(0), 100);
        assert!(prim_array.is_null(1));
        assert_eq!(prim_array.value(2), 101);
        Ok(())
    }

    /// Creates array directly and via ScalarValue and ensures they are the same
    macro_rules! check_scalar_iter {
        ($SCALAR_T:ident, $ARRAYTYPE:ident, $INPUT:expr) => {{
            let scalars: Vec<_> = $INPUT.iter().map(|v| ScalarValue::$SCALAR_T(*v)).collect();

            let array = scalar_iter_to_array(
                &DataType::$SCALAR_T,
                scalars.into_iter().map(|v| Ok(v.to_owned())),
            )
            .unwrap();

            let expected: ArrayRef = Arc::new($ARRAYTYPE::from($INPUT));

            assert_eq!(&array, &expected);
        }};
    }

    /// Creates array directly and via ScalarValue and ensures they
    /// are the same, for string  arrays
    macro_rules! check_scalar_iter_string {
        ($SCALAR_T:ident, $ARRAYTYPE:ident, $INPUT:expr) => {{
            let scalars: Vec<_> = $INPUT
                .iter()
                .map(|v| ScalarValue::$SCALAR_T(v.map(|v| v.to_string())))
                .collect();

            let array = scalar_iter_to_array(
                &DataType::$SCALAR_T,
                scalars.into_iter().map(|v| Ok(v.to_owned())),
            )
            .unwrap();

            let expected: ArrayRef = Arc::new($ARRAYTYPE::from($INPUT));

            assert_eq!(&array, &expected);
        }};
    }

    /// Creates array directly and via ScalarValue and ensures they
    /// are the same, for binary arrays
    macro_rules! check_scalar_iter_binary {
        ($SCALAR_T:ident, $ARRAYTYPE:ident, $INPUT:expr) => {{
            let scalars: Vec<_> = $INPUT
                .iter()
                .map(|v| ScalarValue::$SCALAR_T(v.map(|v| v.to_vec())))
                .collect();

            let array = scalar_iter_to_array(
                &DataType::$SCALAR_T,
                scalars.into_iter().map(|v| Ok(v.to_owned())),
            )
            .unwrap();

            let expected: $ARRAYTYPE = $INPUT.iter().map(|v| v.map(|v| v.to_vec())).collect();

            let expected: ArrayRef = Arc::new(expected);

            assert_eq!(&array, &expected);
        }};
    }

    #[test]
    // despite clippy claiming they are useless, the code doesn't compile otherwise.
    #[allow(clippy::useless_vec)]
    fn scalar_iter_to_array_boolean() {
        check_scalar_iter!(Boolean, BooleanArray, vec![Some(true), None, Some(false)]);
        check_scalar_iter!(Float32, Float32Array, vec![Some(1.9), None, Some(-2.1)]);
        check_scalar_iter!(Float64, Float64Array, vec![Some(1.9), None, Some(-2.1)]);

        check_scalar_iter!(Int8, Int8Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(Int16, Int16Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(Int32, Int32Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(Int64, Int64Array, vec![Some(1), None, Some(3)]);

        check_scalar_iter!(UInt8, UInt8Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(UInt16, UInt16Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(UInt32, UInt32Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(UInt64, UInt64Array, vec![Some(1), None, Some(3)]);

        check_scalar_iter_string!(Utf8, StringArray, vec![Some("foo"), None, Some("bar")]);
        check_scalar_iter_string!(
            LargeUtf8,
            LargeStringArray,
            vec![Some("foo"), None, Some("bar")]
        );
        check_scalar_iter_binary!(Binary, BinaryArray, vec![Some(b"foo"), None, Some(b"bar")]);
        check_scalar_iter_binary!(
            LargeBinary,
            LargeBinaryArray,
            vec![Some(b"foo"), None, Some(b"bar")]
        );
    }

    #[test]
    fn scalar_iter_to_dictionary() {
        fn make_val(v: Option<String>) -> ScalarValue {
            let key_type = DataType::Int32;
            let value = ScalarValue::Utf8(v);
            ScalarValue::Dictionary(Box::new(key_type), Box::new(value))
        }

        let scalars = [
            make_val(Some("Foo".into())),
            make_val(None),
            make_val(Some("Bar".into())),
        ];

        let array = scalar_iter_to_array(
            &scalars.get(0).unwrap().data_type(),
            scalars.into_iter().map(|v| Ok(v.to_owned())),
        )
        .unwrap();
        let array = as_dictionary_array::<Int32Type>(&array).unwrap();
        let values_array = as_string_array(array.values()).unwrap();

        let values = array
            .keys_iter()
            .map(|k| {
                k.map(|k| {
                    assert!(values_array.is_valid(k));
                    values_array.value(k)
                })
            })
            .collect::<Vec<_>>();

        let expected = vec![Some("Foo"), None, Some("Bar")];
        assert_eq!(values, expected);
    }

    #[test]
    fn scalar_iter_to_array_mismatched_types() {
        use ScalarValue::*;
        // If the scalar values are not all the correct type, error here
        let scalars = [Boolean(Some(true)), Int32(Some(5))];

        let result = scalar_iter_to_array(
            &DataType::Boolean,
            scalars.into_iter().map(|v| Ok(v.to_owned())),
        )
        .unwrap_err();
        assert!(
            result.to_string().contains(
                "Inconsistent types in scalar_iter_to_array. Expected Boolean, got Int32(5)"
            ),
            "{}",
            result
        );
    }

    #[test]
    fn scalar_try_from_array_null() {
        let array = vec![Some(33), None].into_iter().collect::<Int64Array>();
        let array: ArrayRef = Arc::new(array);

        assert_eq!(
            ScalarValue::Int64(Some(33)),
            ScalarValue::try_from_array(&array, 0).unwrap()
        );
        assert_eq!(
            ScalarValue::Int64(None),
            ScalarValue::try_from_array(&array, 1).unwrap()
        );
    }

    #[test]
    fn scalar_try_from_dict_datatype() {
        let data_type = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
        let data_type = &data_type;
        let expected =
            ScalarValue::Dictionary(Box::new(DataType::Int8), Box::new(ScalarValue::Utf8(None)));
        assert_eq!(expected, data_type.try_into().unwrap())
    }

    // this test fails on aarch, so don't run it there
    #[cfg(not(target_arch = "aarch64"))]
    #[test]
    fn size_of_scalar() {
        // Since ScalarValues are used in a non trivial number of places,
        // making it larger means significant more memory consumption
        // per distinct value.
        //
        // The alignment requirements differ across architectures and
        // thus the size of the enum appears to as as well

        assert_eq!(std::mem::size_of::<ScalarValue>(), 48);
    }

    #[test]
    fn memory_size() {
        let sv = ScalarValue::Binary(Some(Vec::with_capacity(10)));
        assert_eq!(sv.size(), std::mem::size_of::<ScalarValue>() + 10,);
        let sv_size = sv.size();

        let mut v = Vec::with_capacity(10);
        // do NOT clone `sv` here because this may shrink the vector capacity
        v.push(sv);
        assert_eq!(v.capacity(), 10);
        assert_eq!(
            ScalarValue::size_of_vec(&v),
            std::mem::size_of::<Vec<ScalarValue>>()
                + (9 * std::mem::size_of::<ScalarValue>())
                + sv_size,
        );

        let mut s = HashSet::with_capacity(0);
        // do NOT clone `sv` here because this may shrink the vector capacity
        s.insert(v.pop().unwrap());
        // hashsets may easily grow during insert, so capacity is dynamic
        let s_capacity = s.capacity();
        assert_eq!(
            ScalarValue::size_of_hashset(&s),
            std::mem::size_of::<HashSet<ScalarValue>>()
                + ((s_capacity - 1) * std::mem::size_of::<ScalarValue>())
                + sv_size,
        );
    }

    #[test]
    fn scalar_eq_array() {
        // Validate that eq_array has the same semantics as ScalarValue::eq
        macro_rules! make_typed_vec {
            ($INPUT:expr, $TYPE:ident) => {{
                $INPUT
                    .iter()
                    .map(|v| v.map(|v| v as $TYPE))
                    .collect::<Vec<_>>()
            }};
        }

        let bool_vals = [Some(true), None, Some(false)];
        let f32_vals = [Some(-1.0), None, Some(1.0)];
        let f64_vals = make_typed_vec!(f32_vals, f64);

        let i8_vals = [Some(-1), None, Some(1)];
        let i16_vals = make_typed_vec!(i8_vals, i16);
        let i32_vals = make_typed_vec!(i8_vals, i32);
        let i64_vals = make_typed_vec!(i8_vals, i64);

        let u8_vals = [Some(0), None, Some(1)];
        let u16_vals = make_typed_vec!(u8_vals, u16);
        let u32_vals = make_typed_vec!(u8_vals, u32);
        let u64_vals = make_typed_vec!(u8_vals, u64);

        let str_vals = [Some("foo"), None, Some("bar")];

        /// Test each value in `scalar` with the corresponding element
        /// at `array`. Assumes each element is unique (aka not equal
        /// with all other indexes)
        #[derive(Debug)]
        struct TestCase {
            array: ArrayRef,
            scalars: Vec<ScalarValue>,
        }

        /// Create a test case for casing the input to the specified array type
        macro_rules! make_test_case {
            ($INPUT:expr, $ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                TestCase {
                    array: Arc::new($INPUT.iter().collect::<$ARRAY_TY>()),
                    scalars: $INPUT.iter().map(|v| ScalarValue::$SCALAR_TY(*v)).collect(),
                }
            }};

            ($INPUT:expr, $ARRAY_TY:ident, $SCALAR_TY:ident, $TZ:expr) => {{
                let tz = $TZ;
                TestCase {
                    array: Arc::new($INPUT.iter().collect::<$ARRAY_TY>()),
                    scalars: $INPUT
                        .iter()
                        .map(|v| ScalarValue::$SCALAR_TY(*v, tz.clone()))
                        .collect(),
                }
            }};
        }

        macro_rules! make_str_test_case {
            ($INPUT:expr, $ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                TestCase {
                    array: Arc::new($INPUT.iter().cloned().collect::<$ARRAY_TY>()),
                    scalars: $INPUT
                        .iter()
                        .map(|v| ScalarValue::$SCALAR_TY(v.map(|v| v.to_string())))
                        .collect(),
                }
            }};
        }

        macro_rules! make_binary_test_case {
            ($INPUT:expr, $ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                TestCase {
                    array: Arc::new($INPUT.iter().cloned().collect::<$ARRAY_TY>()),
                    scalars: $INPUT
                        .iter()
                        .map(|v| ScalarValue::$SCALAR_TY(v.map(|v| v.as_bytes().to_vec())))
                        .collect(),
                }
            }};
        }

        /// create a test case for DictionaryArray<$INDEX_TY>
        macro_rules! make_str_dict_test_case {
            ($INPUT:expr, $INDEX_TY:ident) => {{
                TestCase {
                    array: Arc::new(
                        $INPUT
                            .iter()
                            .cloned()
                            .collect::<DictionaryArray<$INDEX_TY>>(),
                    ),
                    scalars: $INPUT
                        .iter()
                        .map(|v| {
                            ScalarValue::Dictionary(
                                Box::new($INDEX_TY::DATA_TYPE),
                                Box::new(ScalarValue::Utf8(v.map(|v| v.to_string()))),
                            )
                        })
                        .collect(),
                }
            }};
        }

        let cases = vec![
            make_test_case!(bool_vals, BooleanArray, Boolean),
            make_test_case!(f32_vals, Float32Array, Float32),
            make_test_case!(f64_vals, Float64Array, Float64),
            make_test_case!(i8_vals, Int8Array, Int8),
            make_test_case!(i16_vals, Int16Array, Int16),
            make_test_case!(i32_vals, Int32Array, Int32),
            make_test_case!(i64_vals, Int64Array, Int64),
            make_test_case!(u8_vals, UInt8Array, UInt8),
            make_test_case!(u16_vals, UInt16Array, UInt16),
            make_test_case!(u32_vals, UInt32Array, UInt32),
            make_test_case!(u64_vals, UInt64Array, UInt64),
            make_str_test_case!(str_vals, StringArray, Utf8),
            make_str_test_case!(str_vals, LargeStringArray, LargeUtf8),
            make_binary_test_case!(str_vals, BinaryArray, Binary),
            make_binary_test_case!(str_vals, LargeBinaryArray, LargeBinary),
            make_test_case!(i32_vals, Date32Array, Date32),
            make_test_case!(i64_vals, Date64Array, Date64),
            make_test_case!(i32_vals, Time32SecondArray, Time32Second),
            make_test_case!(i32_vals, Time32MillisecondArray, Time32Millisecond),
            make_test_case!(i64_vals, Time64MicrosecondArray, Time64Microsecond),
            make_test_case!(i64_vals, Time64NanosecondArray, Time64Nanosecond),
            make_test_case!(i64_vals, TimestampSecondArray, TimestampSecond, None),
            make_test_case!(
                i64_vals,
                TimestampSecondArray,
                TimestampSecond,
                Some("UTC".into())
            ),
            make_test_case!(
                i64_vals,
                TimestampMillisecondArray,
                TimestampMillisecond,
                None
            ),
            make_test_case!(
                i64_vals,
                TimestampMillisecondArray,
                TimestampMillisecond,
                Some("UTC".into())
            ),
            make_test_case!(
                i64_vals,
                TimestampMicrosecondArray,
                TimestampMicrosecond,
                None
            ),
            make_test_case!(
                i64_vals,
                TimestampMicrosecondArray,
                TimestampMicrosecond,
                Some("UTC".into())
            ),
            make_test_case!(
                i64_vals,
                TimestampNanosecondArray,
                TimestampNanosecond,
                None
            ),
            make_test_case!(
                i64_vals,
                TimestampNanosecondArray,
                TimestampNanosecond,
                Some("UTC".into())
            ),
            make_test_case!(i32_vals, IntervalYearMonthArray, IntervalYearMonth),
            make_test_case!(i64_vals, IntervalDayTimeArray, IntervalDayTime),
            make_str_dict_test_case!(str_vals, Int8Type),
            make_str_dict_test_case!(str_vals, Int16Type),
            make_str_dict_test_case!(str_vals, Int32Type),
            make_str_dict_test_case!(str_vals, Int64Type),
            make_str_dict_test_case!(str_vals, UInt8Type),
            make_str_dict_test_case!(str_vals, UInt16Type),
            make_str_dict_test_case!(str_vals, UInt32Type),
            make_str_dict_test_case!(str_vals, UInt64Type),
        ];

        for case in cases {
            println!("**** Test Case *****");
            let TestCase { array, scalars } = case;
            println!("Input array type: {}", array.data_type());
            println!("Input scalars: {scalars:#?}");
            assert_eq!(array.len(), scalars.len());

            for (index, scalar) in scalars.into_iter().enumerate() {
                assert!(
                    scalar.eq_array(&array, index),
                    "Expected {scalar:?} to be equal to {array:?} at index {index}"
                );

                // test that all other elements are *not* equal
                for other_index in 0..array.len() {
                    if index != other_index {
                        assert!(
                            !scalar.eq_array(&array, other_index),
                            "Expected {scalar:?} to be NOT equal to {array:?} at index {other_index}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn scalar_partial_ordering() {
        use ScalarValue::*;

        assert_eq!(
            Int64(Some(33)).partial_cmp(&Int64(Some(0))),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Int64(Some(0)).partial_cmp(&Int64(Some(33))),
            Some(Ordering::Less)
        );
        assert_eq!(
            Int64(Some(33)).partial_cmp(&Int64(Some(33))),
            Some(Ordering::Equal)
        );
        // For different data type, `partial_cmp` returns None.
        assert_eq!(Int64(Some(33)).partial_cmp(&Int32(Some(33))), None);
        assert_eq!(Int32(Some(33)).partial_cmp(&Int64(Some(33))), None);

        assert_eq!(
            List(
                Some(vec![Int32(Some(1)), Int32(Some(5))]),
                Arc::new(Field::new("item", DataType::Int32, false)),
            )
            .partial_cmp(&List(
                Some(vec![Int32(Some(1)), Int32(Some(5))]),
                Arc::new(Field::new("item", DataType::Int32, false)),
            )),
            Some(Ordering::Equal)
        );

        assert_eq!(
            List(
                Some(vec![Int32(Some(10)), Int32(Some(5))]),
                Arc::new(Field::new("item", DataType::Int32, false)),
            )
            .partial_cmp(&List(
                Some(vec![Int32(Some(1)), Int32(Some(5))]),
                Arc::new(Field::new("item", DataType::Int32, false)),
            )),
            Some(Ordering::Greater)
        );

        assert_eq!(
            List(
                Some(vec![Int32(Some(1)), Int32(Some(5))]),
                Arc::new(Field::new("item", DataType::Int32, false)),
            )
            .partial_cmp(&List(
                Some(vec![Int32(Some(10)), Int32(Some(5))]),
                Arc::new(Field::new("item", DataType::Int32, false)),
            )),
            Some(Ordering::Less)
        );

        // For different data type, `partial_cmp` returns None.
        assert_eq!(
            List(
                Some(vec![Int64(Some(1)), Int64(Some(5))]),
                Arc::new(Field::new("item", DataType::Int64, false)),
            )
            .partial_cmp(&List(
                Some(vec![Int32(Some(1)), Int32(Some(5))]),
                Arc::new(Field::new("item", DataType::Int32, false)),
            )),
            None
        );

        assert_eq!(
            ScalarValue::from(vec![
                ("A", ScalarValue::from(1.0)),
                ("B", ScalarValue::from("Z")),
            ])
            .partial_cmp(&ScalarValue::from(vec![
                ("A", ScalarValue::from(2.0)),
                ("B", ScalarValue::from("A")),
            ])),
            Some(Ordering::Less)
        );

        // For different struct fields, `partial_cmp` returns None.
        assert_eq!(
            ScalarValue::from(vec![
                ("A", ScalarValue::from(1.0)),
                ("B", ScalarValue::from("Z")),
            ])
            .partial_cmp(&ScalarValue::from(vec![
                ("a", ScalarValue::from(2.0)),
                ("b", ScalarValue::from("A")),
            ])),
            None
        );
    }

    #[test]
    fn test_scalar_struct() {
        let field_a = Arc::new(Field::new("A", DataType::Int32, false));
        let field_b = Arc::new(Field::new("B", DataType::Boolean, false));
        let field_c = Arc::new(Field::new("C", DataType::Utf8, false));

        let field_e = Arc::new(Field::new("e", DataType::Int16, false));
        let field_f = Arc::new(Field::new("f", DataType::Int64, false));
        let field_d = Arc::new(Field::new(
            "D",
            DataType::Struct(vec![field_e.clone(), field_f.clone()].into()),
            false,
        ));

        let scalar = ScalarValue::Struct(
            Some(vec![
                ScalarValue::Int32(Some(23)),
                ScalarValue::Boolean(Some(false)),
                ScalarValue::Utf8(Some("Hello".to_string())),
                ScalarValue::from(vec![
                    ("e", ScalarValue::from(2i16)),
                    ("f", ScalarValue::from(3i64)),
                ]),
            ]),
            vec![
                field_a.clone(),
                field_b.clone(),
                field_c.clone(),
                field_d.clone(),
            ]
            .into(),
        );

        // Check Display
        assert_eq!(
            format!("{scalar}"),
            String::from("{A:23,B:false,C:Hello,D:{e:2,f:3}}")
        );

        // Check Debug
        assert_eq!(
            format!("{scalar:?}"),
            String::from(
                r#"Struct({A:Int32(23),B:Boolean(false),C:Utf8("Hello"),D:Struct({e:Int16(2),f:Int64(3)})})"#
            )
        );

        // Convert to length-2 array
        let array = scalar.to_array_of_size(2);

        let expected = Arc::new(StructArray::from(vec![
            (
                field_a.clone(),
                Arc::new(Int32Array::from(vec![23, 23])) as ArrayRef,
            ),
            (
                field_b.clone(),
                Arc::new(BooleanArray::from(vec![false, false])) as ArrayRef,
            ),
            (
                field_c.clone(),
                Arc::new(StringArray::from(vec!["Hello", "Hello"])) as ArrayRef,
            ),
            (
                field_d.clone(),
                Arc::new(StructArray::from(vec![
                    (
                        field_e.clone(),
                        Arc::new(Int16Array::from(vec![2, 2])) as ArrayRef,
                    ),
                    (
                        field_f.clone(),
                        Arc::new(Int64Array::from(vec![3, 3])) as ArrayRef,
                    ),
                ])) as ArrayRef,
            ),
        ])) as ArrayRef;

        assert_eq!(&array, &expected);

        // Construct from second element of ArrayRef
        let constructed = ScalarValue::try_from_array(&expected, 1).unwrap();
        assert_eq!(constructed, scalar);

        // None version
        let none_scalar = ScalarValue::try_from(array.data_type()).unwrap();
        assert!(none_scalar.is_null());
        assert_eq!(format!("{none_scalar:?}"), String::from("Struct(NULL)"));

        // Construct with convenience From<Vec<(&str, ScalarValue)>>
        let constructed = ScalarValue::from(vec![
            ("A", ScalarValue::from(23)),
            ("B", ScalarValue::from(false)),
            ("C", ScalarValue::from("Hello")),
            (
                "D",
                ScalarValue::from(vec![
                    ("e", ScalarValue::from(2i16)),
                    ("f", ScalarValue::from(3i64)),
                ]),
            ),
        ]);
        assert_eq!(constructed, scalar);

        // Build Array from Vec of structs
        let scalars = vec![
            ScalarValue::from(vec![
                ("A", ScalarValue::from(23)),
                ("B", ScalarValue::from(false)),
                ("C", ScalarValue::from("Hello")),
                (
                    "D",
                    ScalarValue::from(vec![
                        ("e", ScalarValue::from(2i16)),
                        ("f", ScalarValue::from(3i64)),
                    ]),
                ),
            ]),
            ScalarValue::from(vec![
                ("A", ScalarValue::from(7)),
                ("B", ScalarValue::from(true)),
                ("C", ScalarValue::from("World")),
                (
                    "D",
                    ScalarValue::from(vec![
                        ("e", ScalarValue::from(4i16)),
                        ("f", ScalarValue::from(5i64)),
                    ]),
                ),
            ]),
            ScalarValue::from(vec![
                ("A", ScalarValue::from(-1000)),
                ("B", ScalarValue::from(true)),
                ("C", ScalarValue::from("!!!!!")),
                (
                    "D",
                    ScalarValue::from(vec![
                        ("e", ScalarValue::from(6i16)),
                        ("f", ScalarValue::from(7i64)),
                    ]),
                ),
            ]),
        ];
        let array = scalar_iter_to_array(
            &constructed.data_type(),
            scalars.iter().map(|v| v.to_owned()).map(Ok),
        )
        .unwrap();

        let expected = Arc::new(StructArray::from(vec![
            (
                field_a,
                Arc::new(Int32Array::from(vec![23, 7, -1000])) as ArrayRef,
            ),
            (
                field_b,
                Arc::new(BooleanArray::from(vec![false, true, true])) as ArrayRef,
            ),
            (
                field_c,
                Arc::new(StringArray::from(vec!["Hello", "World", "!!!!!"])) as ArrayRef,
            ),
            (
                field_d,
                Arc::new(StructArray::from(vec![
                    (
                        field_e,
                        Arc::new(Int16Array::from(vec![2, 4, 6])) as ArrayRef,
                    ),
                    (
                        field_f,
                        Arc::new(Int64Array::from(vec![3, 5, 7])) as ArrayRef,
                    ),
                ])) as ArrayRef,
            ),
        ])) as ArrayRef;

        assert_eq!(&array, &expected);
    }

    #[test]
    fn test_lists_in_struct() {
        let field_a = Arc::new(Field::new("A", DataType::Utf8, false));
        let field_primitive_list = Arc::new(Field::new(
            "primitive_list",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            false,
        ));

        // Define primitive list scalars
        let l0 = ScalarValue::List(
            Some(vec![
                ScalarValue::from(1i32),
                ScalarValue::from(2i32),
                ScalarValue::from(3i32),
            ]),
            Arc::new(Field::new("item", DataType::Int32, false)),
        );

        let l1 = ScalarValue::List(
            Some(vec![ScalarValue::from(4i32), ScalarValue::from(5i32)]),
            Arc::new(Field::new("item", DataType::Int32, false)),
        );

        let l2 = ScalarValue::List(
            Some(vec![ScalarValue::from(6i32)]),
            Arc::new(Field::new("item", DataType::Int32, false)),
        );

        // Define struct scalars
        let s0 = ScalarValue::from(vec![
            ("A", ScalarValue::Utf8(Some(String::from("First")))),
            ("primitive_list", l0),
        ]);

        let s1 = ScalarValue::from(vec![
            ("A", ScalarValue::Utf8(Some(String::from("Second")))),
            ("primitive_list", l1),
        ]);

        let s2 = ScalarValue::from(vec![
            ("A", ScalarValue::Utf8(Some(String::from("Third")))),
            ("primitive_list", l2),
        ]);

        // iter_to_array for struct scalars
        let array = scalar_iter_to_array(
            &s0.data_type(),
            vec![s0.clone(), s1.clone(), s2.clone()]
                .into_iter()
                .map(|v| Ok(v.to_owned())),
        )
        .unwrap();
        let array = as_struct_array(&array).unwrap();
        let expected = StructArray::from(vec![
            (
                field_a.clone(),
                Arc::new(StringArray::from(vec!["First", "Second", "Third"])) as ArrayRef,
            ),
            (
                field_primitive_list.clone(),
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(1), Some(2), Some(3)]),
                    Some(vec![Some(4), Some(5)]),
                    Some(vec![Some(6)]),
                ])),
            ),
        ]);

        assert_eq!(array, &expected);

        // Define list-of-structs scalars
        let nl0 = ScalarValue::new_list(Some(vec![s0.clone(), s1.clone()]), s0.data_type());

        let nl1 = ScalarValue::new_list(Some(vec![s2]), s0.data_type());

        let nl2 = ScalarValue::new_list(Some(vec![s1]), s0.data_type());
        // iter_to_array for list-of-struct
        let array = scalar_iter_to_array(
            &nl0.data_type(),
            vec![nl0, nl1, nl2].into_iter().map(|v| Ok(v.to_owned())),
        )
        .unwrap();
        let array = as_list_array(&array).unwrap();

        // Construct expected array with array builders
        let field_a_builder = StringBuilder::with_capacity(4, 1024);
        let primitive_value_builder = Int32Array::builder(8);
        let field_primitive_list_builder = ListBuilder::new(primitive_value_builder);

        let element_builder = StructBuilder::new(
            vec![field_a, field_primitive_list],
            vec![
                Box::new(field_a_builder),
                Box::new(field_primitive_list_builder),
            ],
        );
        let mut list_builder = ListBuilder::new(element_builder);

        list_builder
            .values()
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("First");
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(1);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(2);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(3);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .append(true);
        list_builder.values().append(true);

        list_builder
            .values()
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("Second");
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(4);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(5);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .append(true);
        list_builder.values().append(true);
        list_builder.append(true);

        list_builder
            .values()
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("Third");
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(6);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .append(true);
        list_builder.values().append(true);
        list_builder.append(true);

        list_builder
            .values()
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("Second");
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(4);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(5);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .append(true);
        list_builder.values().append(true);
        list_builder.append(true);

        let expected = list_builder.finish();

        assert_eq!(array, &expected);
    }

    #[test]
    fn test_nested_lists() {
        // Define inner list scalars
        let l1 = ScalarValue::new_list(
            Some(vec![
                ScalarValue::new_list(
                    Some(vec![
                        ScalarValue::from(1i32),
                        ScalarValue::from(2i32),
                        ScalarValue::from(3i32),
                    ]),
                    DataType::Int32,
                ),
                ScalarValue::new_list(
                    Some(vec![ScalarValue::from(4i32), ScalarValue::from(5i32)]),
                    DataType::Int32,
                ),
            ]),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        );

        let l2 = ScalarValue::new_list(
            Some(vec![
                ScalarValue::new_list(Some(vec![ScalarValue::from(6i32)]), DataType::Int32),
                ScalarValue::new_list(
                    Some(vec![ScalarValue::from(7i32), ScalarValue::from(8i32)]),
                    DataType::Int32,
                ),
            ]),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        );

        let l3 = ScalarValue::new_list(
            Some(vec![ScalarValue::new_list(
                Some(vec![ScalarValue::from(9i32)]),
                DataType::Int32,
            )]),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        );

        let array = scalar_iter_to_array(
            &l3.data_type(),
            vec![l1, l2, l3].iter().map(|v| v.to_owned()).map(Ok),
        )
        .unwrap();
        let array = as_list_array(&array).unwrap();

        // Construct expected array with array builders
        let inner_builder = Int32Array::builder(8);
        let middle_builder = ListBuilder::new(inner_builder);
        let mut outer_builder = ListBuilder::new(middle_builder);

        outer_builder.values().values().append_value(1);
        outer_builder.values().values().append_value(2);
        outer_builder.values().values().append_value(3);
        outer_builder.values().append(true);

        outer_builder.values().values().append_value(4);
        outer_builder.values().values().append_value(5);
        outer_builder.values().append(true);
        outer_builder.append(true);

        outer_builder.values().values().append_value(6);
        outer_builder.values().append(true);

        outer_builder.values().values().append_value(7);
        outer_builder.values().values().append_value(8);
        outer_builder.values().append(true);
        outer_builder.append(true);

        outer_builder.values().values().append_value(9);
        outer_builder.values().append(true);
        outer_builder.append(true);

        let expected = outer_builder.finish();

        assert_eq!(array, &expected);
    }

    #[test]
    fn scalar_timestamp_ns_utc_timezone() {
        let scalar =
            ScalarValue::TimestampNanosecond(Some(1599566400000000000), Some("UTC".into()));

        assert_eq!(
            scalar.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );

        let array = scalar.to_array();
        assert_eq!(array.len(), 1);
        assert_eq!(
            array.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );

        let newscalar = ScalarValue::try_from_array(&array, 0).unwrap();
        assert_eq!(
            newscalar.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
    }

    #[test]
    fn cast_round_trip() {
        check_scalar_cast(ScalarValue::Int8(Some(5)), DataType::Int16);
        check_scalar_cast(ScalarValue::Int8(None), DataType::Int16);

        check_scalar_cast(ScalarValue::Float64(Some(5.5)), DataType::Int16);

        check_scalar_cast(ScalarValue::Float64(None), DataType::Int16);

        check_scalar_cast(
            ScalarValue::Utf8(Some("foo".to_string())),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        );

        check_scalar_cast(
            ScalarValue::Utf8(None),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        );
    }

    // mimics how casting work on scalar values by `casting` `scalar` to `desired_type`
    fn check_scalar_cast(scalar: ScalarValue, desired_type: DataType) {
        // convert from scalar --> Array to call cast
        let scalar_array = scalar.to_array();
        // cast the actual value
        let cast_array = kernels::cast::cast(&scalar_array, &desired_type).unwrap();

        // turn it back to a scalar
        let cast_scalar = ScalarValue::try_from_array(&cast_array, 0).unwrap();
        assert_eq!(cast_scalar.data_type(), desired_type);

        // Some time later the "cast" scalar is turned back into an array:
        let array = cast_scalar.to_array_of_size(10);

        // The datatype should be "Dictionary" but is actually Utf8!!!
        assert_eq!(array.data_type(), &desired_type)
    }

    #[test]
    fn test_scalar_negative() -> Result<()> {
        // positive test
        let value = ScalarValue::Int32(Some(12));
        assert_eq!(ScalarValue::Int32(Some(-12)), value.arithmetic_negate()?);
        let value = ScalarValue::Int32(None);
        assert_eq!(ScalarValue::Int32(None), value.arithmetic_negate()?);

        // negative test
        let value = ScalarValue::UInt8(Some(12));
        assert!(value.arithmetic_negate().is_err());
        let value = ScalarValue::Boolean(None);
        assert!(value.arithmetic_negate().is_err());
        Ok(())
    }

    macro_rules! expect_operation_error {
        ($TEST_NAME:ident, $FUNCTION:ident, $EXPECTED_ERROR:expr) => {
            #[test]
            fn $TEST_NAME() {
                let lhs = ScalarValue::UInt64(Some(12));
                let rhs = ScalarValue::Int32(Some(-3));
                match lhs.$FUNCTION(&rhs) {
                    Ok(_result) => {
                        panic!(
                            "Expected binary operation error between lhs: '{:?}', rhs: {:?}",
                            lhs, rhs
                        );
                    }
                    Err(e) => {
                        let error_message = e.to_string();
                        assert!(
                            error_message.contains($EXPECTED_ERROR),
                            "Expected error '{}' not found in actual error '{}'",
                            $EXPECTED_ERROR,
                            error_message
                        );
                    }
                }
            }
        };
    }

    expect_operation_error!(
        expect_add_error,
        add,
        "Invalid arithmetic operation: UInt64 + Int32"
    );
    expect_operation_error!(
        expect_sub_error,
        sub,
        "Invalid arithmetic operation: UInt64 - Int32"
    );

    macro_rules! decimal_op_test_cases {
    ($OPERATION:ident, [$([$L_VALUE:expr, $L_PRECISION:expr, $L_SCALE:expr, $R_VALUE:expr, $R_PRECISION:expr, $R_SCALE:expr, $O_VALUE:expr, $O_PRECISION:expr, $O_SCALE:expr]),+]) => {
            $(

                let left = ScalarValue::Decimal128($L_VALUE, $L_PRECISION, $L_SCALE);
                let right = ScalarValue::Decimal128($R_VALUE, $R_PRECISION, $R_SCALE);
                let result = left.$OPERATION(&right).unwrap();
                assert_eq!(ScalarValue::Decimal128($O_VALUE, $O_PRECISION, $O_SCALE), result);

            )+
        };
    }

    #[test]
    fn decimal_operations() {
        decimal_op_test_cases!(
            add,
            [
                [Some(123), 10, 2, Some(124), 10, 2, Some(123 + 124), 11, 2],
                // test sum decimal with diff scale
                [
                    Some(123),
                    10,
                    3,
                    Some(124),
                    10,
                    2,
                    Some(123 + 124 * 10_i128.pow(1)),
                    12,
                    3
                ],
                // diff precision and scale for decimal data type
                [
                    Some(123),
                    10,
                    2,
                    Some(124),
                    11,
                    3,
                    Some(123 * 10_i128.pow(3 - 2) + 124),
                    12,
                    3
                ]
            ]
        );
    }

    #[test]
    fn decimal_operations_with_nulls() {
        decimal_op_test_cases!(
            add,
            [
                // Case: (None, Some, 0)
                [None, 10, 2, Some(123), 10, 2, None, 11, 2],
                // Case: (Some, None, 0)
                [Some(123), 10, 2, None, 10, 2, None, 11, 2],
                // Case: (Some, None, _) + Side=False
                [Some(123), 8, 2, None, 10, 3, None, 11, 3],
                // Case: (None, Some, _) + Side=False
                [None, 8, 2, Some(123), 10, 3, None, 11, 3],
                // Case: (Some, None, _) + Side=True
                [Some(123), 8, 4, None, 10, 3, None, 12, 4],
                // Case: (None, Some, _) + Side=True
                [None, 10, 3, Some(123), 8, 4, None, 12, 4]
            ]
        );
    }

    #[test]
    fn test_scalar_distance() {
        let cases = [
            // scalar (lhs), scalar (rhs), expected distance
            // ---------------------------------------------
            (ScalarValue::Int8(Some(1)), ScalarValue::Int8(Some(2)), 1),
            (ScalarValue::Int8(Some(2)), ScalarValue::Int8(Some(1)), 1),
            (
                ScalarValue::Int16(Some(-5)),
                ScalarValue::Int16(Some(5)),
                10,
            ),
            (
                ScalarValue::Int16(Some(5)),
                ScalarValue::Int16(Some(-5)),
                10,
            ),
            (ScalarValue::Int32(Some(0)), ScalarValue::Int32(Some(0)), 0),
            (
                ScalarValue::Int32(Some(-5)),
                ScalarValue::Int32(Some(-10)),
                5,
            ),
            (
                ScalarValue::Int64(Some(-10)),
                ScalarValue::Int64(Some(-5)),
                5,
            ),
            (ScalarValue::UInt8(Some(1)), ScalarValue::UInt8(Some(2)), 1),
            (ScalarValue::UInt8(Some(0)), ScalarValue::UInt8(Some(0)), 0),
            (
                ScalarValue::UInt16(Some(5)),
                ScalarValue::UInt16(Some(10)),
                5,
            ),
            (
                ScalarValue::UInt32(Some(10)),
                ScalarValue::UInt32(Some(5)),
                5,
            ),
            (
                ScalarValue::UInt64(Some(5)),
                ScalarValue::UInt64(Some(10)),
                5,
            ),
            (
                ScalarValue::Float32(Some(1.0)),
                ScalarValue::Float32(Some(2.0)),
                1,
            ),
            (
                ScalarValue::Float32(Some(2.0)),
                ScalarValue::Float32(Some(1.0)),
                1,
            ),
            (
                ScalarValue::Float64(Some(0.0)),
                ScalarValue::Float64(Some(0.0)),
                0,
            ),
            (
                ScalarValue::Float64(Some(-5.0)),
                ScalarValue::Float64(Some(-10.0)),
                5,
            ),
            (
                ScalarValue::Float64(Some(-10.0)),
                ScalarValue::Float64(Some(-5.0)),
                5,
            ),
            // Floats are currently special cased to f64/f32 and the result is rounded
            // rather than ceiled/floored. In the future we might want to take a mode
            // which specified the rounding behavior.
            (
                ScalarValue::Float32(Some(1.2)),
                ScalarValue::Float32(Some(1.3)),
                0,
            ),
            (
                ScalarValue::Float32(Some(1.1)),
                ScalarValue::Float32(Some(1.9)),
                1,
            ),
            (
                ScalarValue::Float64(Some(-5.3)),
                ScalarValue::Float64(Some(-9.2)),
                4,
            ),
            (
                ScalarValue::Float64(Some(-5.3)),
                ScalarValue::Float64(Some(-9.7)),
                4,
            ),
            (
                ScalarValue::Float64(Some(-5.3)),
                ScalarValue::Float64(Some(-9.9)),
                5,
            ),
        ];
        for (lhs, rhs, expected) in cases.iter() {
            let distance = lhs.distance(rhs).unwrap();
            assert_eq!(distance, *expected);
        }
    }

    #[test]
    fn test_scalar_distance_invalid() {
        let cases = [
            // scalar (lhs), scalar (rhs)
            // --------------------------
            // Same type but with nulls
            (ScalarValue::Int8(None), ScalarValue::Int8(None)),
            (ScalarValue::Int8(None), ScalarValue::Int8(Some(1))),
            (ScalarValue::Int8(Some(1)), ScalarValue::Int8(None)),
            // Different type
            (ScalarValue::Int8(Some(1)), ScalarValue::Int16(Some(1))),
            (ScalarValue::Int8(Some(1)), ScalarValue::Float32(Some(1.0))),
            (
                ScalarValue::Float64(Some(1.1)),
                ScalarValue::Float32(Some(2.2)),
            ),
            (
                ScalarValue::UInt64(Some(777)),
                ScalarValue::Int32(Some(111)),
            ),
            // Different types with nulls
            (ScalarValue::Int8(None), ScalarValue::Int16(Some(1))),
            (ScalarValue::Int8(Some(1)), ScalarValue::Int16(None)),
            // Unsupported types
            (
                ScalarValue::Utf8(Some("foo".to_string())),
                ScalarValue::Utf8(Some("bar".to_string())),
            ),
            (
                ScalarValue::Boolean(Some(true)),
                ScalarValue::Boolean(Some(false)),
            ),
            (ScalarValue::Date32(Some(0)), ScalarValue::Date32(Some(1))),
            (ScalarValue::Date64(Some(0)), ScalarValue::Date64(Some(1))),
            (
                ScalarValue::Decimal128(Some(123), 5, 5),
                ScalarValue::Decimal128(Some(120), 5, 5),
            ),
        ];
        for (lhs, rhs) in cases {
            let distance = lhs.distance(&rhs);
            assert!(distance.is_none());
        }
    }

    #[test]
    fn test_scalar_interval_negate() {
        let cases = [
            (
                ScalarValue::new_interval_ym(1, 12),
                ScalarValue::new_interval_ym(-1, -12),
            ),
            (
                ScalarValue::new_interval_dt(1, 999),
                ScalarValue::new_interval_dt(-1, -999),
            ),
            (
                ScalarValue::new_interval_mdn(12, 15, 123_456),
                ScalarValue::new_interval_mdn(-12, -15, -123_456),
            ),
        ];
        for (expr, expected) in cases.iter() {
            let result = expr.arithmetic_negate().unwrap();
            assert_eq!(*expected, result, "-expr:{expr:?}");
        }
    }

    #[test]
    fn test_scalar_interval_add() {
        let cases = [
            (
                ScalarValue::new_interval_ym(1, 12),
                ScalarValue::new_interval_ym(1, 12),
                ScalarValue::new_interval_ym(2, 24),
            ),
            (
                ScalarValue::new_interval_dt(1, 999),
                ScalarValue::new_interval_dt(1, 999),
                ScalarValue::new_interval_dt(2, 1998),
            ),
            (
                ScalarValue::new_interval_mdn(12, 15, 123_456),
                ScalarValue::new_interval_mdn(12, 15, 123_456),
                ScalarValue::new_interval_mdn(24, 30, 246_912),
            ),
        ];
        for (lhs, rhs, expected) in cases.iter() {
            let result = lhs.add(rhs).unwrap();
            let result_commute = rhs.add(lhs).unwrap();
            assert_eq!(*expected, result, "lhs:{lhs:?} + rhs:{rhs:?}");
            assert_eq!(*expected, result_commute, "lhs:{rhs:?} + rhs:{lhs:?}");
        }
    }

    #[test]
    fn test_scalar_interval_sub() {
        let cases = [
            (
                ScalarValue::new_interval_ym(1, 12),
                ScalarValue::new_interval_ym(1, 12),
                ScalarValue::new_interval_ym(0, 0),
            ),
            (
                ScalarValue::new_interval_dt(1, 999),
                ScalarValue::new_interval_dt(1, 999),
                ScalarValue::new_interval_dt(0, 0),
            ),
            (
                ScalarValue::new_interval_mdn(12, 15, 123_456),
                ScalarValue::new_interval_mdn(12, 15, 123_456),
                ScalarValue::new_interval_mdn(0, 0, 0),
            ),
        ];
        for (lhs, rhs, expected) in cases.iter() {
            let result = lhs.sub(rhs).unwrap();
            assert_eq!(*expected, result, "lhs:{lhs:?} - rhs:{rhs:?}");
        }
    }

    #[test]
    fn timestamp_op_random_tests() {
        // timestamp1 + (or -) interval = timestamp2
        // timestamp2 - timestamp1 (or timestamp1 - timestamp2) = interval ?
        let sample_size = 1000;
        let timestamps1 = get_random_timestamps(sample_size);
        let intervals = get_random_intervals(sample_size);
        // ts(sec) + interval(ns) = ts(sec); however,
        // ts(sec) - ts(sec) cannot be = interval(ns). Therefore,
        // timestamps are more precise than intervals in tests.
        for (idx, ts1) in timestamps1.iter().enumerate() {
            if idx % 2 == 0 {
                let timestamp2 = ts1.add(intervals[idx].clone()).unwrap();
                let back = timestamp2.sub(intervals[idx].clone()).unwrap();
                assert_eq!(ts1, &back);
            } else {
                let timestamp2 = ts1.sub(intervals[idx].clone()).unwrap();
                let back = timestamp2.add(intervals[idx].clone()).unwrap();
                assert_eq!(ts1, &back);
            };
        }
    }

    #[test]
    fn test_build_timestamp_millisecond_list() {
        let values = vec![ScalarValue::TimestampMillisecond(Some(1), None)];
        let ts_list = ScalarValue::new_list(
            Some(values),
            DataType::Timestamp(TimeUnit::Millisecond, None),
        );
        let list = ts_list.to_array_of_size(1);
        assert_eq!(1, list.len());
    }

    fn get_random_timestamps(sample_size: u64) -> Vec<ScalarValue> {
        let vector_size = sample_size;
        let mut timestamp = vec![];
        let mut rng = rand::thread_rng();
        for i in 0..vector_size {
            let year = rng.gen_range(1995..=2050);
            let month = rng.gen_range(1..=12);
            let day = rng.gen_range(1..=28); // to exclude invalid dates
            let hour = rng.gen_range(0..=23);
            let minute = rng.gen_range(0..=59);
            let second = rng.gen_range(0..=59);
            if i % 4 == 0 {
                timestamp.push(ScalarValue::TimestampSecond(
                    Some(
                        NaiveDate::from_ymd_opt(year, month, day)
                            .unwrap()
                            .and_hms_opt(hour, minute, second)
                            .unwrap()
                            .timestamp(),
                    ),
                    None,
                ))
            } else if i % 4 == 1 {
                let millisec = rng.gen_range(0..=999);
                timestamp.push(ScalarValue::TimestampMillisecond(
                    Some(
                        NaiveDate::from_ymd_opt(year, month, day)
                            .unwrap()
                            .and_hms_milli_opt(hour, minute, second, millisec)
                            .unwrap()
                            .timestamp_millis(),
                    ),
                    None,
                ))
            } else if i % 4 == 2 {
                let microsec = rng.gen_range(0..=999_999);
                timestamp.push(ScalarValue::TimestampMicrosecond(
                    Some(
                        NaiveDate::from_ymd_opt(year, month, day)
                            .unwrap()
                            .and_hms_micro_opt(hour, minute, second, microsec)
                            .unwrap()
                            .timestamp_micros(),
                    ),
                    None,
                ))
            } else if i % 4 == 3 {
                let nanosec = rng.gen_range(0..=999_999_999);
                timestamp.push(ScalarValue::TimestampNanosecond(
                    Some(
                        NaiveDate::from_ymd_opt(year, month, day)
                            .unwrap()
                            .and_hms_nano_opt(hour, minute, second, nanosec)
                            .unwrap()
                            .timestamp_nanos_opt()
                            .unwrap(),
                    ),
                    None,
                ))
            }
        }
        timestamp
    }

    fn get_random_intervals(sample_size: u64) -> Vec<ScalarValue> {
        const MILLISECS_IN_ONE_DAY: i64 = 86_400_000;
        const NANOSECS_IN_ONE_DAY: i64 = 86_400_000_000_000;

        let vector_size = sample_size;
        let mut intervals = vec![];
        let mut rng = rand::thread_rng();
        const SECS_IN_ONE_DAY: i32 = 86_400;
        const MICROSECS_IN_ONE_DAY: i64 = 86_400_000_000;
        for i in 0..vector_size {
            if i % 4 == 0 {
                let days = rng.gen_range(0..5000);
                // to not break second precision
                let millis = rng.gen_range(0..SECS_IN_ONE_DAY) * 1000;
                intervals.push(ScalarValue::new_interval_dt(days, millis));
            } else if i % 4 == 1 {
                let days = rng.gen_range(0..5000);
                let millisec = rng.gen_range(0..(MILLISECS_IN_ONE_DAY as i32));
                intervals.push(ScalarValue::new_interval_dt(days, millisec));
            } else if i % 4 == 2 {
                let days = rng.gen_range(0..5000);
                // to not break microsec precision
                let nanosec = rng.gen_range(0..MICROSECS_IN_ONE_DAY) * 1000;
                intervals.push(ScalarValue::new_interval_mdn(0, days, nanosec));
            } else {
                let days = rng.gen_range(0..5000);
                let nanosec = rng.gen_range(0..NANOSECS_IN_ONE_DAY);
                intervals.push(ScalarValue::new_interval_mdn(0, days, nanosec));
            }
        }
        intervals
    }
}
