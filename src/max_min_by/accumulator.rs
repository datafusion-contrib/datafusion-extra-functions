// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, AsArray, BooleanArray,
    GenericStringArray, PrimitiveArray, StringViewArray, make_array,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type,
    Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::common::ScalarValue;
use datafusion::error;
use datafusion::logical_expr::{Accumulator, EmitTo, GroupsAccumulator};
use std::marker::PhantomData;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

/// Scalar fallback for ungrouped aggregation and key types without a grouped
/// specialization.
#[derive(Debug)]
pub(super) struct MaxByAccumulator {
    value: ScalarValue,
    key: ScalarValue,
}

impl MaxByAccumulator {
    pub(super) fn try_new(value_type: &DataType, key_type: &DataType) -> error::Result<Self> {
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

/// Comparison hook so a later `min_by` native path can reuse this accumulator
/// with the opposite predicate.
trait OrderByKey: Send + Sync + 'static {
    type Storage: Default + Send + Sync + 'static;
    type Keys<'a>;

    fn keys(array: &dyn Array) -> Self::Keys<'_>;
    fn is_null(keys: &Self::Keys<'_>, row_idx: usize) -> bool;
    fn should_replace(current: &Self::Storage, keys: &Self::Keys<'_>, row_idx: usize) -> bool;
    fn update(current: &mut Self::Storage, keys: &Self::Keys<'_>, row_idx: usize);
    fn into_array(values: Vec<Self::Storage>, is_set: Vec<bool>, data_type: &DataType) -> ArrayRef;
    fn storage_size() -> usize;
    fn heap_size(value: &Self::Storage) -> usize;
}

struct MaxByGroupsAccumulator<K: OrderByKey> {
    keys: Vec<K::Storage>,
    values: Vec<ScalarValue>,
    is_set: Vec<bool>,
    key_type: DataType,
    value_type: DataType,
    key_heap_size: usize,
    value_heap_size: usize,
    _key: PhantomData<K>,
}

impl<K: OrderByKey> MaxByGroupsAccumulator<K> {
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
        let old_len = self.keys.len();
        if old_len < total_num_groups {
            self.keys.resize_with(total_num_groups, K::Storage::default);
            self.is_set.resize(total_num_groups, false);
            let empty = ScalarValue::try_from(&self.value_type)?;
            let empty_heap = empty.size().saturating_sub(size_of_val(&empty));
            self.values.resize(total_num_groups, empty);
            self.value_heap_size += (total_num_groups - old_len) * empty_heap;
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
        let key_array = K::keys(values[1].as_ref());

        for (row_idx, &group_idx) in group_indices.iter().enumerate() {
            if opt_filter.is_some_and(|filter| filter.is_null(row_idx) || !filter.value(row_idx))
                || K::is_null(&key_array, row_idx)
            {
                continue;
            }

            if !self.is_set[group_idx]
                || K::should_replace(&self.keys[group_idx], &key_array, row_idx)
            {
                self.key_heap_size -= K::heap_size(&self.keys[group_idx]);
                K::update(&mut self.keys[group_idx], &key_array, row_idx);
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

impl<K: OrderByKey> GroupsAccumulator for MaxByGroupsAccumulator<K> {
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

impl<T> OrderByKey for PrimitiveKey<T>
where
    T: ArrowPrimitiveType + Send + Sync,
    T::Native: ArrowNativeTypeOp,
{
    type Storage = T::Native;
    type Keys<'a> = &'a PrimitiveArray<T>;

    fn keys(array: &dyn Array) -> Self::Keys<'_> {
        array.as_primitive()
    }

    fn is_null(keys: &Self::Keys<'_>, row_idx: usize) -> bool {
        keys.is_null(row_idx)
    }

    fn should_replace(current: &Self::Storage, keys: &Self::Keys<'_>, row_idx: usize) -> bool {
        keys.value(row_idx).is_gt(*current)
    }

    fn update(current: &mut Self::Storage, keys: &Self::Keys<'_>, row_idx: usize) {
        *current = keys.value(row_idx);
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

struct BytesKey;

enum BytesKeys<'a> {
    Utf8(&'a GenericStringArray<i32>),
    LargeUtf8(&'a GenericStringArray<i64>),
    Utf8View(&'a StringViewArray),
}

impl<'a> BytesKeys<'a> {
    fn new(array: &'a dyn Array) -> Self {
        if let Some(array) = array.as_string_opt::<i32>() {
            Self::Utf8(array)
        } else if let Some(array) = array.as_string_opt::<i64>() {
            Self::LargeUtf8(array)
        } else {
            Self::Utf8View(array.as_string_view())
        }
    }

    fn is_null(&self, row_idx: usize) -> bool {
        match self {
            Self::Utf8(array) => array.is_null(row_idx),
            Self::LargeUtf8(array) => array.is_null(row_idx),
            Self::Utf8View(array) => array.is_null(row_idx),
        }
    }

    fn value(&self, row_idx: usize) -> &str {
        match self {
            Self::Utf8(array) => array.value(row_idx),
            Self::LargeUtf8(array) => array.value(row_idx),
            Self::Utf8View(array) => array.value(row_idx),
        }
    }
}

impl OrderByKey for BytesKey {
    type Storage = String;
    type Keys<'a> = BytesKeys<'a>;

    fn keys(array: &dyn Array) -> Self::Keys<'_> {
        BytesKeys::new(array)
    }

    fn is_null(keys: &Self::Keys<'_>, row_idx: usize) -> bool {
        keys.is_null(row_idx)
    }

    fn should_replace(current: &Self::Storage, keys: &Self::Keys<'_>, row_idx: usize) -> bool {
        keys.value(row_idx) > current.as_str()
    }

    fn update(current: &mut Self::Storage, keys: &Self::Keys<'_>, row_idx: usize) {
        current.clear();
        current.push_str(keys.value(row_idx));
    }

    fn into_array(values: Vec<Self::Storage>, is_set: Vec<bool>, data_type: &DataType) -> ArrayRef {
        let iter = values
            .iter()
            .zip(is_set)
            .map(|(value, is_set)| is_set.then_some(value.as_str()));
        match data_type {
            DataType::LargeUtf8 => Arc::new(iter.collect::<GenericStringArray<i64>>()),
            DataType::Utf8View => Arc::new(iter.collect::<StringViewArray>()),
            _ => Arc::new(iter.collect::<GenericStringArray<i32>>()),
        }
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

fn primitive<T>(value_type: DataType, key_type: DataType) -> Box<dyn GroupsAccumulator>
where
    T: ArrowPrimitiveType + Send + Sync,
    T::Native: ArrowNativeTypeOp,
{
    Box::new(MaxByGroupsAccumulator::<PrimitiveKey<T>>::new(
        value_type, key_type,
    ))
}

fn bytes(value_type: DataType, key_type: DataType) -> Box<dyn GroupsAccumulator> {
    Box::new(MaxByGroupsAccumulator::<BytesKey>::new(
        value_type, key_type,
    ))
}

type GroupsAccumulatorCtor = fn(DataType, DataType) -> Box<dyn GroupsAccumulator>;

fn groups_key_ctor(key_type: &DataType) -> Option<GroupsAccumulatorCtor> {
    use DataType::*;
    use TimeUnit::*;
    Some(match key_type {
        Int8 => primitive::<Int8Type>,
        Int16 => primitive::<Int16Type>,
        Int32 => primitive::<Int32Type>,
        Int64 => primitive::<Int64Type>,
        UInt8 => primitive::<UInt8Type>,
        UInt16 => primitive::<UInt16Type>,
        UInt32 => primitive::<UInt32Type>,
        UInt64 => primitive::<UInt64Type>,
        Float16 => primitive::<Float16Type>,
        Float32 => primitive::<Float32Type>,
        Float64 => primitive::<Float64Type>,
        Decimal32(_, _) => primitive::<Decimal32Type>,
        Decimal64(_, _) => primitive::<Decimal64Type>,
        Decimal128(_, _) => primitive::<Decimal128Type>,
        Decimal256(_, _) => primitive::<Decimal256Type>,
        Date32 => primitive::<Date32Type>,
        Date64 => primitive::<Date64Type>,
        Time32(Second) => primitive::<Time32SecondType>,
        Time32(Millisecond) => primitive::<Time32MillisecondType>,
        Time64(Microsecond) => primitive::<Time64MicrosecondType>,
        Time64(Nanosecond) => primitive::<Time64NanosecondType>,
        Timestamp(Second, _) => primitive::<TimestampSecondType>,
        Timestamp(Millisecond, _) => primitive::<TimestampMillisecondType>,
        Timestamp(Microsecond, _) => primitive::<TimestampMicrosecondType>,
        Timestamp(Nanosecond, _) => primitive::<TimestampNanosecondType>,
        Utf8 | LargeUtf8 | Utf8View => bytes,
        _ => return None,
    })
}

pub(super) fn supports_groups_key(key_type: &DataType) -> bool {
    groups_key_ctor(key_type).is_some()
}

/// Returns a grouped accumulator when the key type has a native specialization.
pub(super) fn try_groups_accumulator(
    value_type: &DataType,
    key_type: &DataType,
) -> Option<Box<dyn GroupsAccumulator>> {
    Some(groups_key_ctor(key_type)?(
        value_type.clone(),
        key_type.clone(),
    ))
}
