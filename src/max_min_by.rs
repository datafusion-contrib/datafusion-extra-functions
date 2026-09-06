use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, AsArray, BooleanArray,
    GenericStringArray, OffsetSizeTrait, PrimitiveArray, StringViewArray, make_array,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type,
    Field, FieldRef, Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator};
use datafusion::{arrow, common, error, functions_aggregate, logical_expr};
use std::fmt;
use std::marker::PhantomData;
use std::mem::{size_of, size_of_val};
use std::ops::Deref;
use std::sync::Arc;

make_udaf_expr_and_func!(
    MaxByFunction,
    max_by,
    x y,
    "Returns the value of the first column corresponding to the maximum value in the second column.",
    max_by_udaf
);

#[derive(Eq, Hash, PartialEq)]
pub struct MaxByFunction {
    null_first: bool,
    native: bool,
    signature: logical_expr::Signature,
}

impl fmt::Debug for MaxByFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("MaxBy")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}
impl Default for MaxByFunction {
    fn default() -> Self {
        Self::new(true)
    }
}

impl MaxByFunction {
    pub fn new(null_first: bool) -> Self {
        Self {
            null_first,
            native: false,
            signature: logical_expr::Signature::user_defined(logical_expr::Volatility::Immutable),
        }
    }

    fn native() -> Self {
        Self {
            null_first: true,
            native: true,
            signature: logical_expr::Signature::user_defined(logical_expr::Volatility::Immutable),
        }
    }
}

fn get_min_max_by_result_type(
    input_types: &[arrow::datatypes::DataType],
) -> error::Result<Vec<arrow::datatypes::DataType>> {
    if input_types.len() != 2 {
        return common::exec_err!(
            "max_by/min_by requires exactly two arguments, got {}",
            input_types.len()
        );
    }
    match &input_types[0] {
        arrow::datatypes::DataType::Dictionary(_, dict_value_type) => {
            // x add checker, if the value type is complex data type
            let mut result = vec![dict_value_type.deref().clone()];
            // Preserve all other argument types
            result.extend_from_slice(&input_types[1..]);
            Ok(result)
        }
        _ => Ok(input_types.to_vec()),
    }
}

impl logical_expr::AggregateUDFImpl for MaxByFunction {
    fn name(&self) -> &str {
        "max_by"
    }

    fn signature(&self) -> &logical_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> error::Result<arrow::datatypes::DataType> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(
        &self,
        acc_args: logical_expr::function::AccumulatorArgs,
    ) -> error::Result<Box<dyn logical_expr::Accumulator>> {
        if !acc_args.order_bys.is_empty() || acc_args.ignore_nulls {
            return common::internal_err!(
                "native max_by does not support ORDER BY or IGNORE NULLS"
            );
        }
        Ok(Box::new(MaxByAccumulator::try_new(
            acc_args.return_field.data_type(),
            acc_args.expr_fields[1].data_type(),
        )?))
    }

    fn coerce_types(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> error::Result<Vec<arrow::datatypes::DataType>> {
        get_min_max_by_result_type(arg_types)
    }

    fn simplify(&self) -> Option<logical_expr::function::AggregateFunctionSimplification> {
        if self.native {
            return None;
        }
        let null_first = self.null_first;
        let simplify = move |mut aggr_func: logical_expr::expr::AggregateFunction,
                             _: &logical_expr::simplify::SimplifyContext| {
            if null_first
                && aggr_func.params.order_by.is_empty()
                && aggr_func.params.null_treatment.is_none()
            {
                let func = logical_expr::expr::AggregateFunction::new_udf(
                    Arc::new(logical_expr::AggregateUDF::from(MaxByFunction::native())),
                    aggr_func.params.args,
                    aggr_func.params.distinct,
                    aggr_func.params.filter,
                    vec![],
                    None,
                );
                return Ok(logical_expr::Expr::AggregateFunction(func));
            }

            // Preserve custom NULL ordering, explicit ORDER BY, and
            // programmatic NULL treatment with the existing rewrite.
            let mut order_by = aggr_func.params.order_by;
            let (second_arg, first_arg) = (
                aggr_func.params.args.remove(1),
                aggr_func.params.args.remove(0),
            );
            let sort = logical_expr::expr::Sort::new(second_arg, true, null_first);
            order_by.push(sort);
            let func = logical_expr::expr::AggregateFunction::new_udf(
                functions_aggregate::first_last::last_value_udaf(),
                vec![first_arg],
                aggr_func.params.distinct,
                aggr_func.params.filter,
                order_by,
                aggr_func.params.null_treatment,
            );
            let func = logical_expr::expr::Expr::AggregateFunction(func);
            Ok(func)
        };
        Some(Box::new(simplify))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> error::Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(args.name, "value"),
                args.return_field.data_type().clone(),
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "key"),
                args.input_fields[1].data_type().clone(),
                true,
            )),
        ])
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.native
            && !args.is_distinct
            && !args.ignore_nulls
            && args.order_bys.is_empty()
            && args.expr_fields.len() == 2
            && is_native_key_type(args.expr_fields[1].data_type())
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> error::Result<Box<dyn GroupsAccumulator>> {
        create_groups_accumulator(
            args.return_field.data_type().clone(),
            args.expr_fields[1].data_type().clone(),
        )
    }
}

/// Scalar fallback for ungrouped aggregation and key types without a grouped
/// specialization.
#[derive(Debug)]
struct MaxByAccumulator {
    value: ScalarValue,
    key: ScalarValue,
}

impl MaxByAccumulator {
    fn try_new(value_type: &DataType, key_type: &DataType) -> error::Result<Self> {
        Ok(Self {
            value: ScalarValue::try_from(value_type)?,
            key: ScalarValue::try_from(key_type)?,
        })
    }
}

impl Accumulator for MaxByAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> error::Result<()> {
        let value_array = &values[0];
        let key_array = &values[1];

        for row_idx in 0..key_array.len() {
            if key_array.is_null(row_idx) {
                continue;
            }
            let candidate_key = ScalarValue::try_from_array(key_array, row_idx)?;
            if candidate_key > self.key {
                self.value = ScalarValue::try_from_array(value_array, row_idx)?;
                self.value.compact();
                self.key = candidate_key;
                self.key.compact();
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> error::Result<ScalarValue> {
        Ok(self.value.clone())
    }

    fn state(&mut self) -> error::Result<Vec<ScalarValue>> {
        Ok(vec![self.value.clone(), self.key.clone()])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> error::Result<()> {
        self.update_batch(states)
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.value) - size_of_val(&self.key)
            + self.value.size()
            + self.key.size()
    }
}

trait MaxByKey: Send + Sync + 'static {
    type Storage: Default + Send + Sync + 'static;
    type Array: Array + ?Sized;

    fn downcast(array: &dyn Array) -> &Self::Array;
    fn is_greater(current: &Self::Storage, array: &Self::Array, row_idx: usize) -> bool;
    fn update(current: &mut Self::Storage, array: &Self::Array, row_idx: usize);
    fn into_array(values: Vec<Self::Storage>, is_set: Vec<bool>, data_type: &DataType) -> ArrayRef;
    fn storage_size() -> usize;
    fn heap_size(value: &Self::Storage) -> usize;
}

struct MaxByGroupsAccumulator<K: MaxByKey> {
    keys: Vec<K::Storage>,
    values: Vec<ScalarValue>,
    is_set: Vec<bool>,
    key_type: DataType,
    value_type: DataType,
    key_heap_size: usize,
    value_heap_size: usize,
    _key: PhantomData<K>,
}

impl<K: MaxByKey> MaxByGroupsAccumulator<K> {
    fn new(value_type: DataType, key_type: DataType) -> Self {
        Self {
            keys: vec![],
            values: vec![],
            is_set: vec![],
            key_type,
            value_type,
            key_heap_size: 0,
            value_heap_size: 0,
            _key: PhantomData,
        }
    }

    fn resize(&mut self, total_num_groups: usize) -> error::Result<()> {
        if self.keys.len() < total_num_groups {
            self.keys.resize_with(total_num_groups, K::Storage::default);
            self.is_set.resize(total_num_groups, false);
            self.values
                .resize(total_num_groups, ScalarValue::try_from(&self.value_type)?);
            self.value_heap_size = self
                .values
                .iter()
                .map(|value| value.size() - size_of_val(value))
                .sum();
        }
        Ok(())
    }

    fn update_value(
        &mut self,
        group_idx: usize,
        value_array: &ArrayRef,
        row_idx: usize,
    ) -> error::Result<()> {
        let mut value = ScalarValue::try_from_array(value_array, row_idx)?;
        value.compact();
        self.value_heap_size -=
            self.values[group_idx].size() - size_of_val(&self.values[group_idx]);
        self.value_heap_size += value.size() - size_of_val(&value);
        self.values[group_idx] = value;
        Ok(())
    }

    fn update_batch_inner(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> error::Result<()> {
        self.resize(total_num_groups)?;
        let value_array = &values[0];
        let key_array = K::downcast(values[1].as_ref());

        for (row_idx, &group_idx) in group_indices.iter().enumerate() {
            if opt_filter.is_some_and(|filter| filter.is_null(row_idx) || !filter.value(row_idx))
                || key_array.is_null(row_idx)
            {
                continue;
            }

            if !self.is_set[group_idx] || K::is_greater(&self.keys[group_idx], key_array, row_idx) {
                self.key_heap_size -= K::heap_size(&self.keys[group_idx]);
                K::update(&mut self.keys[group_idx], key_array, row_idx);
                self.key_heap_size += K::heap_size(&self.keys[group_idx]);
                self.update_value(group_idx, value_array, row_idx)?;
                self.is_set[group_idx] = true;
            }
        }
        Ok(())
    }

    fn take_keys(&mut self, emit_to: EmitTo) -> Vec<K::Storage> {
        let keys = emit_to.take_needed(&mut self.keys);
        self.key_heap_size = self.keys.iter().map(K::heap_size).sum();
        keys
    }

    fn take_values(&mut self, emit_to: EmitTo) -> Vec<ScalarValue> {
        let values = emit_to.take_needed(&mut self.values);
        self.value_heap_size = self
            .values
            .iter()
            .map(|value| value.size() - size_of_val(value))
            .sum();
        values
    }
}

impl<K: MaxByKey> GroupsAccumulator for MaxByGroupsAccumulator<K> {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> error::Result<()> {
        self.update_batch_inner(values, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> error::Result<ArrayRef> {
        let values = self.take_values(emit_to);
        self.take_keys(emit_to);
        emit_to.take_needed(&mut self.is_set);
        ScalarValue::iter_to_array(values)
    }

    fn state(&mut self, emit_to: EmitTo) -> error::Result<Vec<ArrayRef>> {
        let values = self.take_values(emit_to);
        let keys = self.take_keys(emit_to);
        let is_set = emit_to.take_needed(&mut self.is_set);
        Ok(vec![
            ScalarValue::iter_to_array(values)?,
            K::into_array(keys, is_set, &self.key_type),
        ])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> error::Result<()> {
        self.update_batch_inner(values, group_indices, None, total_num_groups)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> error::Result<Vec<ArrayRef>> {
        Ok(vec![
            Arc::clone(&values[0]),
            apply_filter_as_nulls(Arc::clone(&values[1]), opt_filter)?,
        ])
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.keys.capacity() * K::storage_size()
            + self.key_heap_size
            + self.values.capacity() * size_of::<ScalarValue>()
            + self.value_heap_size
            + self.is_set.capacity() / 8
    }
}

struct PrimitiveKey<T>(PhantomData<T>);

impl<T> MaxByKey for PrimitiveKey<T>
where
    T: ArrowPrimitiveType + Send + Sync,
    T::Native: ArrowNativeTypeOp,
{
    type Storage = T::Native;
    type Array = PrimitiveArray<T>;

    fn downcast(array: &dyn Array) -> &Self::Array {
        array.as_primitive()
    }

    fn is_greater(current: &Self::Storage, array: &Self::Array, row_idx: usize) -> bool {
        array.value(row_idx).is_gt(*current)
    }

    fn update(current: &mut Self::Storage, array: &Self::Array, row_idx: usize) {
        *current = array.value(row_idx);
    }

    fn into_array(values: Vec<Self::Storage>, is_set: Vec<bool>, data_type: &DataType) -> ArrayRef {
        Arc::new(
            PrimitiveArray::<T>::new(values.into(), Some(NullBuffer::from_iter(is_set)))
                .with_data_type(data_type.clone()),
        )
    }

    fn storage_size() -> usize {
        size_of::<T::Native>()
    }

    fn heap_size(_value: &Self::Storage) -> usize {
        0
    }
}

struct StringKey<O>(PhantomData<O>);

impl<O: OffsetSizeTrait> MaxByKey for StringKey<O> {
    type Storage = String;
    type Array = GenericStringArray<O>;

    fn downcast(array: &dyn Array) -> &Self::Array {
        array.as_string()
    }

    fn is_greater(current: &Self::Storage, array: &Self::Array, row_idx: usize) -> bool {
        array.value(row_idx) > current.as_str()
    }

    fn update(current: &mut Self::Storage, array: &Self::Array, row_idx: usize) {
        current.clear();
        current.push_str(array.value(row_idx));
    }

    fn into_array(
        values: Vec<Self::Storage>,
        is_set: Vec<bool>,
        _data_type: &DataType,
    ) -> ArrayRef {
        Arc::new(
            values
                .iter()
                .zip(is_set)
                .map(|(value, is_set)| is_set.then_some(value.as_str()))
                .collect::<GenericStringArray<O>>(),
        )
    }

    fn storage_size() -> usize {
        size_of::<String>()
    }

    fn heap_size(value: &Self::Storage) -> usize {
        value.capacity()
    }
}

struct StringViewKey;

impl MaxByKey for StringViewKey {
    type Storage = String;
    type Array = StringViewArray;

    fn downcast(array: &dyn Array) -> &Self::Array {
        array.as_string_view()
    }

    fn is_greater(current: &Self::Storage, array: &Self::Array, row_idx: usize) -> bool {
        array.value(row_idx) > current.as_str()
    }

    fn update(current: &mut Self::Storage, array: &Self::Array, row_idx: usize) {
        current.clear();
        current.push_str(array.value(row_idx));
    }

    fn into_array(
        values: Vec<Self::Storage>,
        is_set: Vec<bool>,
        _data_type: &DataType,
    ) -> ArrayRef {
        Arc::new(
            values
                .iter()
                .zip(is_set)
                .map(|(value, is_set)| is_set.then_some(value.as_str()))
                .collect::<StringViewArray>(),
        )
    }

    fn storage_size() -> usize {
        size_of::<String>()
    }

    fn heap_size(value: &Self::Storage) -> usize {
        value.capacity()
    }
}

fn apply_filter_as_nulls(
    array: ArrayRef,
    opt_filter: Option<&BooleanArray>,
) -> error::Result<ArrayRef> {
    let Some(filter) = opt_filter else {
        return Ok(array);
    };
    let filter_nulls = NullBuffer::new(match filter.nulls() {
        Some(nulls) => filter.values() & nulls.inner(),
        None => filter.values().clone(),
    });
    let nulls = NullBuffer::union(array.nulls(), Some(&filter_nulls));
    Ok(make_array(
        array.to_data().into_builder().nulls(nulls).build()?,
    ))
}

fn is_native_key_type(data_type: &DataType) -> bool {
    use DataType::*;
    use TimeUnit::*;
    matches!(
        data_type,
        Int8 | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Float16
            | Float32
            | Float64
            | Decimal32(_, _)
            | Decimal64(_, _)
            | Decimal128(_, _)
            | Decimal256(_, _)
            | Date32
            | Date64
            | Time32(Second)
            | Time32(Millisecond)
            | Time64(Microsecond)
            | Time64(Nanosecond)
            | Timestamp(Second, _)
            | Timestamp(Millisecond, _)
            | Timestamp(Microsecond, _)
            | Timestamp(Nanosecond, _)
            | Utf8
            | LargeUtf8
            | Utf8View
    )
}

macro_rules! primitive_accumulator {
    ($type:ty, $value_type:expr, $key_type:expr) => {
        Ok(Box::new(
            MaxByGroupsAccumulator::<PrimitiveKey<$type>>::new($value_type, $key_type),
        ))
    };
}

fn create_groups_accumulator(
    value_type: DataType,
    key_type: DataType,
) -> error::Result<Box<dyn GroupsAccumulator>> {
    use DataType::*;
    use TimeUnit::*;
    match &key_type {
        Int8 => primitive_accumulator!(Int8Type, value_type, key_type),
        Int16 => primitive_accumulator!(Int16Type, value_type, key_type),
        Int32 => primitive_accumulator!(Int32Type, value_type, key_type),
        Int64 => primitive_accumulator!(Int64Type, value_type, key_type),
        UInt8 => primitive_accumulator!(UInt8Type, value_type, key_type),
        UInt16 => primitive_accumulator!(UInt16Type, value_type, key_type),
        UInt32 => primitive_accumulator!(UInt32Type, value_type, key_type),
        UInt64 => primitive_accumulator!(UInt64Type, value_type, key_type),
        Float16 => primitive_accumulator!(Float16Type, value_type, key_type),
        Float32 => primitive_accumulator!(Float32Type, value_type, key_type),
        Float64 => primitive_accumulator!(Float64Type, value_type, key_type),
        Decimal32(_, _) => primitive_accumulator!(Decimal32Type, value_type, key_type),
        Decimal64(_, _) => primitive_accumulator!(Decimal64Type, value_type, key_type),
        Decimal128(_, _) => primitive_accumulator!(Decimal128Type, value_type, key_type),
        Decimal256(_, _) => primitive_accumulator!(Decimal256Type, value_type, key_type),
        Date32 => primitive_accumulator!(Date32Type, value_type, key_type),
        Date64 => primitive_accumulator!(Date64Type, value_type, key_type),
        Time32(Second) => {
            primitive_accumulator!(Time32SecondType, value_type, key_type)
        }
        Time32(Millisecond) => {
            primitive_accumulator!(Time32MillisecondType, value_type, key_type)
        }
        Time64(Microsecond) => {
            primitive_accumulator!(Time64MicrosecondType, value_type, key_type)
        }
        Time64(Nanosecond) => {
            primitive_accumulator!(Time64NanosecondType, value_type, key_type)
        }
        Timestamp(Second, _) => {
            primitive_accumulator!(TimestampSecondType, value_type, key_type)
        }
        Timestamp(Millisecond, _) => {
            primitive_accumulator!(TimestampMillisecondType, value_type, key_type)
        }
        Timestamp(Microsecond, _) => {
            primitive_accumulator!(TimestampMicrosecondType, value_type, key_type)
        }
        Timestamp(Nanosecond, _) => {
            primitive_accumulator!(TimestampNanosecondType, value_type, key_type)
        }
        Utf8 => Ok(Box::new(MaxByGroupsAccumulator::<StringKey<i32>>::new(
            value_type, key_type,
        ))),
        LargeUtf8 => Ok(Box::new(MaxByGroupsAccumulator::<StringKey<i64>>::new(
            value_type, key_type,
        ))),
        Utf8View => Ok(Box::new(MaxByGroupsAccumulator::<StringViewKey>::new(
            value_type, key_type,
        ))),
        _ => common::internal_err!("unsupported max_by key type {key_type}"),
    }
}

make_udaf_expr_and_func!(
    MinByFunction,
    min_by,
    x y,
    "Returns the value of the first column corresponding to the minimum value in the second column.",
    min_by_udaf
);

#[derive(Eq, Hash, PartialEq)]
pub struct MinByFunction {
    null_first: bool,
    signature: logical_expr::Signature,
}

impl fmt::Debug for MinByFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("MinBy")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}

impl Default for MinByFunction {
    fn default() -> Self {
        Self::new(true)
    }
}

impl MinByFunction {
    pub fn new(null_first: bool) -> Self {
        Self {
            null_first,
            signature: logical_expr::Signature::user_defined(logical_expr::Volatility::Immutable),
        }
    }
}

impl logical_expr::AggregateUDFImpl for MinByFunction {
    fn name(&self) -> &str {
        "min_by"
    }

    fn signature(&self) -> &logical_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> error::Result<arrow::datatypes::DataType> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(
        &self,
        _acc_args: logical_expr::function::AccumulatorArgs,
    ) -> error::Result<Box<dyn logical_expr::Accumulator>> {
        common::exec_err!("should not reach here")
    }

    fn coerce_types(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> error::Result<Vec<arrow::datatypes::DataType>> {
        get_min_max_by_result_type(arg_types)
    }

    fn simplify(&self) -> Option<logical_expr::function::AggregateFunctionSimplification> {
        let null_first = self.null_first;
        let simplify = move |mut aggr_func: logical_expr::expr::AggregateFunction,
                             _: &logical_expr::simplify::SimplifyContext| {
            let mut order_by = aggr_func.params.order_by;
            let (second_arg, first_arg) = (
                aggr_func.params.args.remove(1),
                aggr_func.params.args.remove(0),
            );

            let sort = logical_expr::expr::Sort::new(second_arg, false, null_first);
            order_by.push(sort); // false for ascending sort
            let func = logical_expr::expr::AggregateFunction::new_udf(
                functions_aggregate::first_last::last_value_udaf(),
                vec![first_arg],
                aggr_func.params.distinct,
                aggr_func.params.filter,
                order_by,
                aggr_func.params.null_treatment,
            );
            let func = logical_expr::expr::Expr::AggregateFunction(func);
            Ok(func)
        };
        Some(Box::new(simplify))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::ArrayAccessor;
    use datafusion::{arrow, datasource, error, prelude};
    use std::sync;

    const TEST_TABLE_NAME: &str = "types";
    const STRING_COLUMN_NAME: &str = "string";
    const DICTIONARY_COLUMN_NAME: &str = "dict_string";
    const INT64_COLUMN_NAME: &str = "int64";
    const FLOAT64_COLUMN_NAME: &str = "float64";

    const MIN_STRING_VALUE: &str = "a";
    const MID_STRING_VALUE: &str = "b";
    const MAX_STRING_VALUE: &str = "c";
    const MIN_FLOAT_VALUE: f64 = 0.25;
    const MID_FLOAT_VALUE: f64 = 0.5;
    const MAX_FLOAT_VALUE: f64 = 0.75;
    const MIN_INT_VALUE: i64 = -1;
    const MID_INT_VALUE: i64 = 0;
    const MAX_INT_VALUE: i64 = 1;
    const MIN_DICTIONARY_VALUE: &str = "a";
    const MID_DICTIONARY_VALUE: &str = "b";
    const MAX_DICTIONARY_VALUE: &str = "c";

    fn test_schema() -> sync::Arc<arrow::datatypes::Schema> {
        sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(
                STRING_COLUMN_NAME,
                arrow::datatypes::DataType::Utf8,
                false,
            ),
            arrow::datatypes::Field::new_dictionary(
                DICTIONARY_COLUMN_NAME,
                arrow::datatypes::DataType::Int32,
                arrow::datatypes::DataType::Utf8,
                false,
            ),
            arrow::datatypes::Field::new(
                INT64_COLUMN_NAME,
                arrow::datatypes::DataType::Int64,
                false,
            ),
            arrow::datatypes::Field::new(
                FLOAT64_COLUMN_NAME,
                arrow::datatypes::DataType::Float64,
                false,
            ),
        ]))
    }

    fn test_data(
        schema: sync::Arc<arrow::datatypes::Schema>,
    ) -> Vec<arrow::record_batch::RecordBatch> {
        vec![
            arrow::record_batch::RecordBatch::try_new(
                schema,
                vec![
                    sync::Arc::new(arrow::array::StringArray::from(vec![
                        MID_STRING_VALUE,
                        MIN_STRING_VALUE,
                        MAX_STRING_VALUE,
                    ])),
                    sync::Arc::new(
                        vec![
                            Some(MID_DICTIONARY_VALUE),
                            Some(MIN_DICTIONARY_VALUE),
                            Some(MAX_DICTIONARY_VALUE),
                        ]
                        .into_iter()
                        .collect::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>(),
                    ),
                    sync::Arc::new(arrow::array::Int64Array::from(vec![
                        MID_INT_VALUE,
                        MIN_INT_VALUE,
                        MAX_INT_VALUE,
                    ])),
                    sync::Arc::new(arrow::array::Float64Array::from(vec![
                        MID_FLOAT_VALUE,
                        MIN_FLOAT_VALUE,
                        MAX_FLOAT_VALUE,
                    ])),
                ],
            )
            .unwrap(),
        ]
    }

    fn test_ctx() -> datafusion::common::Result<prelude::SessionContext> {
        let schema = test_schema();
        let data = test_data(schema.clone());
        let table = datasource::MemTable::try_new(schema, vec![data])?;
        let ctx = prelude::SessionContext::new();
        ctx.register_table(TEST_TABLE_NAME, sync::Arc::new(table))?;
        Ok(ctx)
    }

    async fn extract_single_value<T, A>(df: prelude::DataFrame) -> error::Result<T>
    where
        A: arrow::array::Array + 'static,
        for<'a> &'a A: arrow::array::ArrayAccessor,
        for<'a> <&'a A as arrow::array::ArrayAccessor>::Item: Into<T>,
    {
        let results = df.collect().await?;
        let col = results[0].column(0);
        let v1 = col.as_any().downcast_ref::<A>().unwrap();
        let value = v1.value(0).into();
        Ok(value)
    }

    #[cfg(test)]
    mod max_by {

        use super::*;

        #[tokio::test]
        async fn test_max_by_string_int() -> error::Result<()> {
            let query = format!(
                "SELECT max_by({}, {}) FROM {}",
                STRING_COLUMN_NAME, INT64_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<String, arrow::array::StringArray>(df).await?;
            assert_eq!(result, MAX_STRING_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_max_by_string_float() -> error::Result<()> {
            let query = format!(
                "SELECT max_by({}, {}) FROM {}",
                STRING_COLUMN_NAME, FLOAT64_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<String, arrow::array::StringArray>(df).await?;
            assert_eq!(result, MAX_STRING_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_max_by_float_string() -> error::Result<()> {
            let query = format!(
                "SELECT max_by({}, {}) FROM {}",
                FLOAT64_COLUMN_NAME, STRING_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<f64, arrow::array::Float64Array>(df).await?;
            assert_eq!(result, MAX_FLOAT_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_max_by_int_string() -> error::Result<()> {
            let query = format!(
                "SELECT max_by({}, {}) FROM {}",
                INT64_COLUMN_NAME, STRING_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<i64, arrow::array::Int64Array>(df).await?;
            assert_eq!(result, MAX_INT_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_max_by_dictionary_int() -> error::Result<()> {
            let query = format!(
                "SELECT max_by({}, {}) FROM {}",
                DICTIONARY_COLUMN_NAME, INT64_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<String, arrow::array::StringArray>(df).await?;
            assert_eq!(result, MAX_DICTIONARY_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_max_by_ignores_nulls() -> error::Result<()> {
            let query = r#"
                SELECT max_by(v, k)
                FROM (
                    VALUES
                        ('a', 1),
                        ('b', CAST(NULL AS INT)),
                        ('c', 2)
                ) AS t(v, k)
            "#;
            let df = ctx()?.sql(query).await?;
            let result = extract_single_value::<String, arrow::array::StringArray>(df).await?;
            assert_eq!(result, "c", "max_by should ignore NULLs");
            Ok(())
        }

        fn ctx() -> error::Result<prelude::SessionContext> {
            let ctx = test_ctx()?;
            let max_by_udaf = MaxByFunction::default();
            ctx.register_udaf(max_by_udaf.into());
            Ok(ctx)
        }
    }

    #[cfg(test)]
    mod min_by {

        use super::*;

        #[tokio::test]
        async fn test_min_by_string_int() -> error::Result<()> {
            let query = format!(
                "SELECT min_by({}, {}) FROM {}",
                STRING_COLUMN_NAME, INT64_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<String, arrow::array::StringArray>(df).await?;
            assert_eq!(result, MIN_STRING_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_min_by_string_float() -> error::Result<()> {
            let query = format!(
                "SELECT min_by({}, {}) FROM {}",
                STRING_COLUMN_NAME, FLOAT64_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<String, arrow::array::StringArray>(df).await?;
            assert_eq!(result, MIN_STRING_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_min_by_float_string() -> error::Result<()> {
            let query = format!(
                "SELECT min_by({}, {}) FROM {}",
                FLOAT64_COLUMN_NAME, STRING_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<f64, arrow::array::Float64Array>(df).await?;
            assert_eq!(result, MIN_FLOAT_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_min_by_int_string() -> error::Result<()> {
            let query = format!(
                "SELECT min_by({}, {}) FROM {}",
                INT64_COLUMN_NAME, STRING_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<i64, arrow::array::Int64Array>(df).await?;
            assert_eq!(result, MIN_INT_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_min_by_dictionary_int() -> error::Result<()> {
            let query = format!(
                "SELECT min_by({}, {}) FROM {}",
                DICTIONARY_COLUMN_NAME, INT64_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<String, arrow::array::StringArray>(df).await?;
            assert_eq!(result, MIN_DICTIONARY_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_min_by_ignores_nulls() -> error::Result<()> {
            let query = r#"
                SELECT min_by(v, k)
                FROM (
                    VALUES
                        ('a', 1),
                        ('b', CAST(NULL AS INT)),
                        ('c', 2)
                ) AS t(v, k)
            "#;
            let df = ctx()?.sql(query).await?;
            let result = extract_single_value::<String, arrow::array::StringArray>(df).await?;
            assert_eq!(result, "a", "min_by should ignore NULLs");
            Ok(())
        }

        fn ctx() -> error::Result<prelude::SessionContext> {
            let ctx = test_ctx()?;
            let min_by_udaf = MinByFunction::default();
            ctx.register_udaf(min_by_udaf.into());
            Ok(ctx)
        }
    }
}
