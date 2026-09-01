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

use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::physical_expr::aggregate::utils::Hashable;
use datafusion::{arrow, common, error, logical_expr, scalar};
use std::{cmp, collections, fmt, hash, mem, sync};

#[derive(fmt::Debug)]
pub struct PrimitiveModeAccumulator<T>
where
    T: arrow::array::ArrowPrimitiveType + Send,
    T::Native: Eq + hash::Hash,
{
    value_counts: collections::HashMap<T::Native, i64>,
    data_type: arrow::datatypes::DataType,
}

impl<T> PrimitiveModeAccumulator<T>
where
    T: arrow::array::ArrowPrimitiveType + Send,
    T::Native: Eq + hash::Hash + Clone,
{
    pub fn new(data_type: &arrow::datatypes::DataType) -> Self {
        Self {
            value_counts: collections::HashMap::default(),
            data_type: data_type.clone(),
        }
    }
}

impl<T> logical_expr::Accumulator for PrimitiveModeAccumulator<T>
where
    T: arrow::array::ArrowPrimitiveType + Send + fmt::Debug,
    T::Native: Eq + hash::Hash + Clone + PartialOrd + fmt::Debug,
{
    fn update_batch(&mut self, values: &[arrow::array::ArrayRef]) -> error::Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = common::cast::as_primitive_array::<T>(&values[0])?;

        for value in arr.iter().flatten() {
            let counter = self.value_counts.entry(value).or_insert(0);
            *counter += 1;
        }

        Ok(())
    }

    fn state(&mut self) -> error::Result<Vec<scalar::ScalarValue>> {
        let values =
            arrow::array::PrimitiveArray::<T>::from_iter_values(self.value_counts.keys().copied())
                .with_data_type(self.data_type.clone());
        let counts =
            arrow::array::Int64Array::from_iter_values(self.value_counts.values().copied());

        Ok(vec![
            SingleRowListArrayBuilder::new(sync::Arc::new(values)).build_list_scalar(),
            SingleRowListArrayBuilder::new(sync::Arc::new(counts)).build_list_scalar(),
        ])
    }

    fn merge_batch(&mut self, states: &[arrow::array::ArrayRef]) -> error::Result<()> {
        super::for_each_state_row(states, |values, counts| {
            let values = common::cast::as_primitive_array::<T>(values)?;
            for (value, count) in values.iter().zip(counts.values()) {
                if let Some(value) = value {
                    *self.value_counts.entry(value).or_insert(0) += *count;
                }
            }
            Ok(())
        })
    }

    fn evaluate(&mut self) -> error::Result<scalar::ScalarValue> {
        let mut max_value: Option<T::Native> = None;
        let mut max_count: i64 = 0;

        self.value_counts.iter().for_each(|(value, &count)| {
            match count.cmp(&max_count) {
                cmp::Ordering::Greater => {
                    max_value = Some(*value);
                    max_count = count;
                }
                cmp::Ordering::Equal => {
                    // On ties the smallest value wins, matching PostgreSQL's mode().
                    max_value = match max_value {
                        Some(ref current_max_value) if value < current_max_value => Some(*value),
                        Some(ref current_max_value) => Some(*current_max_value),
                        None => Some(*value),
                    };
                }
                _ => {} // Do nothing if count is less than max_count
            }
        });

        scalar::ScalarValue::new_primitive::<T>(max_value, &self.data_type)
    }

    fn size(&self) -> usize {
        mem::size_of_val(&self.value_counts)
            + self.value_counts.len() * mem::size_of::<(T::Native, i64)>()
    }
}

#[derive(Debug)]
pub struct FloatModeAccumulator<T>
where
    T: arrow::array::ArrowPrimitiveType,
{
    value_counts: collections::HashMap<Hashable<T::Native>, i64>,
    data_type: arrow::datatypes::DataType,
}

impl<T> FloatModeAccumulator<T>
where
    T: arrow::array::ArrowPrimitiveType,
{
    pub fn new(data_type: &arrow::datatypes::DataType) -> Self {
        Self {
            value_counts: collections::HashMap::default(),
            data_type: data_type.clone(),
        }
    }
}

impl<T> logical_expr::Accumulator for FloatModeAccumulator<T>
where
    T: arrow::array::ArrowPrimitiveType + Send + fmt::Debug,
    T::Native: PartialOrd + fmt::Debug + Clone,
{
    fn update_batch(&mut self, values: &[arrow::array::ArrayRef]) -> error::Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = common::cast::as_primitive_array::<T>(&values[0])?;

        for value in arr.iter().flatten() {
            let counter = self.value_counts.entry(Hashable(value)).or_insert(0);
            *counter += 1;
        }

        Ok(())
    }

    fn state(&mut self) -> error::Result<Vec<scalar::ScalarValue>> {
        let values = arrow::array::PrimitiveArray::<T>::from_iter_values(
            self.value_counts.keys().map(|key| key.0),
        )
        .with_data_type(self.data_type.clone());
        let counts =
            arrow::array::Int64Array::from_iter_values(self.value_counts.values().copied());

        Ok(vec![
            SingleRowListArrayBuilder::new(sync::Arc::new(values)).build_list_scalar(),
            SingleRowListArrayBuilder::new(sync::Arc::new(counts)).build_list_scalar(),
        ])
    }

    fn merge_batch(&mut self, states: &[arrow::array::ArrayRef]) -> error::Result<()> {
        super::for_each_state_row(states, |values, counts| {
            let values = common::cast::as_primitive_array::<T>(values)?;
            for (value, count) in values.iter().zip(counts.values()) {
                if let Some(value) = value {
                    *self.value_counts.entry(Hashable(value)).or_insert(0) += *count;
                }
            }
            Ok(())
        })
    }

    fn evaluate(&mut self) -> error::Result<scalar::ScalarValue> {
        let mut max_value: Option<T::Native> = None;
        let mut max_count: i64 = 0;

        self.value_counts.iter().for_each(|(value, &count)| {
            match count.cmp(&max_count) {
                cmp::Ordering::Greater => {
                    max_value = Some(value.0);
                    max_count = count;
                }
                cmp::Ordering::Equal => {
                    // On ties the smallest value wins, matching PostgreSQL's mode().
                    max_value = match max_value {
                        Some(current_max_value) if value.0 < current_max_value => Some(value.0),
                        Some(current_max_value) => Some(current_max_value),
                        None => Some(value.0),
                    };
                }
                _ => {} // Do nothing if count is less than max_count
            }
        });

        scalar::ScalarValue::new_primitive::<T>(max_value, &self.data_type)
    }

    fn size(&self) -> usize {
        mem::size_of_val(&self.value_counts)
            + self.value_counts.len() * mem::size_of::<(Hashable<T::Native>, i64)>()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use datafusion::logical_expr::Accumulator;
    use std::sync;

    fn merge_from(dest: &mut impl Accumulator, src: &mut impl Accumulator) -> error::Result<()> {
        let arrays = src
            .state()?
            .iter()
            .map(|value| value.to_array())
            .collect::<error::Result<Vec<_>>>()?;
        dest.merge_batch(&arrays)
    }

    #[test]
    fn test_mode_accumulator_single_mode_int64() -> error::Result<()> {
        let mut acc = PrimitiveModeAccumulator::<arrow::datatypes::Int64Type>::new(
            &arrow::datatypes::DataType::Int64,
        );
        let values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::Int64Array::from(vec![1, 2, 2, 3, 3, 3]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            scalar::ScalarValue::new_primitive::<arrow::datatypes::Int64Type>(
                Some(3),
                &arrow::datatypes::DataType::Int64
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_with_nulls_int64() -> error::Result<()> {
        let mut acc = PrimitiveModeAccumulator::<arrow::datatypes::Int64Type>::new(
            &arrow::datatypes::DataType::Int64,
        );
        let values: arrow::array::ArrayRef = sync::Arc::new(arrow::array::Int64Array::from(vec![
            None,
            Some(1),
            Some(2),
            Some(2),
            Some(3),
            Some(3),
            Some(3),
        ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            scalar::ScalarValue::new_primitive::<arrow::datatypes::Int64Type>(
                Some(3),
                &arrow::datatypes::DataType::Int64
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_tie_case_int64() -> error::Result<()> {
        let mut acc = PrimitiveModeAccumulator::<arrow::datatypes::Int64Type>::new(
            &arrow::datatypes::DataType::Int64,
        );
        let values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::Int64Array::from(vec![1, 2, 2, 3, 3]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            scalar::ScalarValue::new_primitive::<arrow::datatypes::Int64Type>(
                Some(2),
                &arrow::datatypes::DataType::Int64
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_only_nulls_int64() -> error::Result<()> {
        let mut acc = PrimitiveModeAccumulator::<arrow::datatypes::Int64Type>::new(
            &arrow::datatypes::DataType::Int64,
        );
        let values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::Int64Array::from(vec![None, None, None, None]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            scalar::ScalarValue::new_primitive::<arrow::datatypes::Int64Type>(
                None,
                &arrow::datatypes::DataType::Int64
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_merge_overlapping_keys_int64() -> error::Result<()> {
        let mut left = PrimitiveModeAccumulator::<arrow::datatypes::Int64Type>::new(
            &arrow::datatypes::DataType::Int64,
        );
        let left_values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::Int64Array::from(vec![2, 2, 2]));
        left.update_batch(&[left_values])?;

        let mut right = PrimitiveModeAccumulator::<arrow::datatypes::Int64Type>::new(
            &arrow::datatypes::DataType::Int64,
        );
        // Right-only or replace-instead-of-add both pick 1. Summed counts pick 2.
        let right_values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::Int64Array::from(vec![1, 1, 1, 1, 2, 2]));
        right.update_batch(&[right_values])?;

        merge_from(&mut right, &mut left)?;
        let result = right.evaluate()?;
        assert_eq!(
            result,
            scalar::ScalarValue::new_primitive::<arrow::datatypes::Int64Type>(
                Some(2),
                &arrow::datatypes::DataType::Int64
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_single_mode_float64() -> error::Result<()> {
        let mut acc = FloatModeAccumulator::<arrow::datatypes::Float64Type>::new(
            &arrow::datatypes::DataType::Float64,
        );
        let values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::Float64Array::from(vec![
                1.0, 2.0, 2.0, 3.0, 3.0, 3.0,
            ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            scalar::ScalarValue::new_primitive::<arrow::datatypes::Float64Type>(
                Some(3.0),
                &arrow::datatypes::DataType::Float64
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_with_nulls_float64() -> error::Result<()> {
        let mut acc = FloatModeAccumulator::<arrow::datatypes::Float64Type>::new(
            &arrow::datatypes::DataType::Float64,
        );
        let values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::Float64Array::from(vec![
                None,
                Some(1.0),
                Some(2.0),
                Some(2.0),
                Some(3.0),
                Some(3.0),
                Some(3.0),
            ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            scalar::ScalarValue::new_primitive::<arrow::datatypes::Float64Type>(
                Some(3.0),
                &arrow::datatypes::DataType::Float64
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_tie_case_float64() -> error::Result<()> {
        let mut acc = FloatModeAccumulator::<arrow::datatypes::Float64Type>::new(
            &arrow::datatypes::DataType::Float64,
        );
        let values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::Float64Array::from(vec![
                1.0, 2.0, 2.0, 3.0, 3.0,
            ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            scalar::ScalarValue::new_primitive::<arrow::datatypes::Float64Type>(
                Some(2.0),
                &arrow::datatypes::DataType::Float64
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_only_nulls_float64() -> error::Result<()> {
        let mut acc = FloatModeAccumulator::<arrow::datatypes::Float64Type>::new(
            &arrow::datatypes::DataType::Float64,
        );
        let values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::Float64Array::from(vec![
                None, None, None, None,
            ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            scalar::ScalarValue::new_primitive::<arrow::datatypes::Float64Type>(
                None,
                &arrow::datatypes::DataType::Float64
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_merge_overlapping_keys_float64() -> error::Result<()> {
        let mut left = FloatModeAccumulator::<arrow::datatypes::Float64Type>::new(
            &arrow::datatypes::DataType::Float64,
        );
        let left_values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::Float64Array::from(vec![2.0, 2.0, 2.0]));
        left.update_batch(&[left_values])?;

        let mut right = FloatModeAccumulator::<arrow::datatypes::Float64Type>::new(
            &arrow::datatypes::DataType::Float64,
        );
        let right_values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::Float64Array::from(vec![
                1.0, 1.0, 1.0, 1.0, 2.0, 2.0,
            ]));
        right.update_batch(&[right_values])?;

        merge_from(&mut right, &mut left)?;
        let result = right.evaluate()?;
        assert_eq!(
            result,
            scalar::ScalarValue::new_primitive::<arrow::datatypes::Float64Type>(
                Some(2.0),
                &arrow::datatypes::DataType::Float64
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_single_mode_date64() -> error::Result<()> {
        let mut acc = PrimitiveModeAccumulator::<arrow::datatypes::Date64Type>::new(
            &arrow::datatypes::DataType::Date64,
        );
        let values: arrow::array::ArrayRef = sync::Arc::new(arrow::array::Date64Array::from(vec![
            1609459200000,
            1609545600000,
            1609545600000,
            1609632000000,
            1609632000000,
            1609632000000,
        ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            scalar::ScalarValue::new_primitive::<arrow::datatypes::Date64Type>(
                Some(1609632000000),
                &arrow::datatypes::DataType::Date64
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_with_nulls_date64() -> error::Result<()> {
        let mut acc = PrimitiveModeAccumulator::<arrow::datatypes::Date64Type>::new(
            &arrow::datatypes::DataType::Date64,
        );
        let values: arrow::array::ArrayRef = sync::Arc::new(arrow::array::Date64Array::from(vec![
            None,
            Some(1609459200000),
            Some(1609545600000),
            Some(1609545600000),
            Some(1609632000000),
            Some(1609632000000),
            Some(1609632000000),
        ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            scalar::ScalarValue::new_primitive::<arrow::datatypes::Date64Type>(
                Some(1609632000000),
                &arrow::datatypes::DataType::Date64
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_tie_case_date64() -> error::Result<()> {
        let mut acc = PrimitiveModeAccumulator::<arrow::datatypes::Date64Type>::new(
            &arrow::datatypes::DataType::Date64,
        );
        let values: arrow::array::ArrayRef = sync::Arc::new(arrow::array::Date64Array::from(vec![
            1609459200000,
            1609545600000,
            1609545600000,
            1609632000000,
            1609632000000,
        ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            scalar::ScalarValue::new_primitive::<arrow::datatypes::Date64Type>(
                Some(1609545600000),
                &arrow::datatypes::DataType::Date64
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_only_nulls_date64() -> error::Result<()> {
        let mut acc = PrimitiveModeAccumulator::<arrow::datatypes::Date64Type>::new(
            &arrow::datatypes::DataType::Date64,
        );
        let values: arrow::array::ArrayRef = sync::Arc::new(arrow::array::Date64Array::from(vec![
            None, None, None, None,
        ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            scalar::ScalarValue::new_primitive::<arrow::datatypes::Date64Type>(
                None,
                &arrow::datatypes::DataType::Date64
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_single_mode_time64() -> error::Result<()> {
        let mut acc = PrimitiveModeAccumulator::<arrow::datatypes::Time64MicrosecondType>::new(
            &arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
        );
        let values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::Time64MicrosecondArray::from(vec![
                3600000000,
                7200000000,
                7200000000,
                10800000000,
                10800000000,
                10800000000,
            ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            scalar::ScalarValue::new_primitive::<arrow::datatypes::Time64MicrosecondType>(
                Some(10800000000),
                &arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_with_nulls_time64() -> error::Result<()> {
        let mut acc = PrimitiveModeAccumulator::<arrow::datatypes::Time64MicrosecondType>::new(
            &arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
        );
        let values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::Time64MicrosecondArray::from(vec![
                None,
                Some(3600000000),
                Some(7200000000),
                Some(7200000000),
                Some(10800000000),
                Some(10800000000),
                Some(10800000000),
            ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            scalar::ScalarValue::new_primitive::<arrow::datatypes::Time64MicrosecondType>(
                Some(10800000000),
                &arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_tie_case_time64() -> error::Result<()> {
        let mut acc = PrimitiveModeAccumulator::<arrow::datatypes::Time64MicrosecondType>::new(
            &arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
        );
        let values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::Time64MicrosecondArray::from(vec![
                3600000000,
                7200000000,
                7200000000,
                10800000000,
                10800000000,
            ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            scalar::ScalarValue::new_primitive::<arrow::datatypes::Time64MicrosecondType>(
                Some(7200000000),
                &arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_only_nulls_time64() -> error::Result<()> {
        let mut acc = PrimitiveModeAccumulator::<arrow::datatypes::Time64MicrosecondType>::new(
            &arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
        );
        let values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::Time64MicrosecondArray::from(vec![
                None, None, None, None,
            ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            scalar::ScalarValue::new_primitive::<arrow::datatypes::Time64MicrosecondType>(
                None,
                &arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
            )?
        );
        Ok(())
    }
}
