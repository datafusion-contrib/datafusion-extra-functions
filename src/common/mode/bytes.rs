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

use datafusion::arrow::array::AsArray;
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::{arrow, common, error, logical_expr, scalar};
use std::{collections, mem, sync};

#[derive(Debug)]
pub struct BytesModeAccumulator {
    value_counts: collections::HashMap<String, i64>,
    data_type: arrow::datatypes::DataType,
}

impl BytesModeAccumulator {
    pub fn new(data_type: &arrow::datatypes::DataType) -> Self {
        Self {
            value_counts: collections::HashMap::new(),
            data_type: data_type.clone(),
        }
    }

    fn update_counts<'a, V>(&mut self, array: V)
    where
        V: arrow::array::ArrayAccessor<Item = &'a str>,
    {
        for value in arrow::array::ArrayIter::new(array).flatten() {
            // get_mut before insert avoids allocating a String for keys
            // that are already present.
            if let Some(count) = self.value_counts.get_mut(value) {
                *count += 1;
            } else {
                self.value_counts.insert(value.to_string(), 1);
            }
        }
    }
}

impl logical_expr::Accumulator for BytesModeAccumulator {
    fn update_batch(&mut self, values: &[arrow::array::ArrayRef]) -> error::Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        match &self.data_type {
            arrow::datatypes::DataType::Utf8View => {
                let array = values[0].as_string_view();
                self.update_counts(array);
            }
            _ => {
                let array = values[0].as_string::<i32>();
                self.update_counts(array);
            }
        };

        Ok(())
    }

    fn state(&mut self) -> error::Result<Vec<scalar::ScalarValue>> {
        let values = arrow::array::StringArray::from_iter_values(self.value_counts.keys());
        let counts =
            arrow::array::Int64Array::from_iter_values(self.value_counts.values().copied());

        Ok(vec![
            SingleRowListArrayBuilder::new(sync::Arc::new(values)).build_list_scalar(),
            SingleRowListArrayBuilder::new(sync::Arc::new(counts)).build_list_scalar(),
        ])
    }

    fn merge_batch(&mut self, states: &[arrow::array::ArrayRef]) -> error::Result<()> {
        super::for_each_state_row(states, |values, counts| {
            let values = common::cast::as_string_array(values)?;
            for (value, count) in values.iter().zip(counts.values()) {
                if let Some(value) = value {
                    *self.value_counts.entry(value.to_string()).or_insert(0) += *count;
                }
            }
            Ok(())
        })
    }

    fn evaluate(&mut self) -> error::Result<scalar::ScalarValue> {
        let mode = self
            .value_counts
            .iter()
            .max_by(|a, b| {
                // Highest count wins; on ties the smallest value wins,
                // matching PostgreSQL's mode() ordered-set aggregate.
                a.1.cmp(b.1).then_with(|| b.0.cmp(a.0))
            })
            .map(|(value, _)| value.to_string());

        match &self.data_type {
            arrow::datatypes::DataType::Utf8View => Ok(scalar::ScalarValue::Utf8View(mode)),
            _ => Ok(scalar::ScalarValue::Utf8(mode)),
        }
    }

    fn size(&self) -> usize {
        self.value_counts.capacity() * mem::size_of::<(String, i64)>()
            + mem::size_of_val(&self.data_type)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use datafusion::logical_expr::Accumulator;
    use std::sync;

    #[test]
    fn test_mode_accumulator_single_mode_utf8() -> error::Result<()> {
        let mut acc = BytesModeAccumulator::new(&arrow::datatypes::DataType::Utf8);
        let values: arrow::array::ArrayRef = sync::Arc::new(arrow::array::StringArray::from(vec![
            Some("apple"),
            Some("banana"),
            Some("apple"),
            Some("orange"),
            Some("banana"),
            Some("apple"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, scalar::ScalarValue::Utf8(Some("apple".to_string())));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_tie_utf8() -> error::Result<()> {
        let mut acc = BytesModeAccumulator::new(&arrow::datatypes::DataType::Utf8);
        let values: arrow::array::ArrayRef = sync::Arc::new(arrow::array::StringArray::from(vec![
            Some("apple"),
            Some("banana"),
            Some("apple"),
            Some("orange"),
            Some("banana"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, scalar::ScalarValue::Utf8(Some("apple".to_string())));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_all_nulls_utf8() -> error::Result<()> {
        let mut acc = BytesModeAccumulator::new(&arrow::datatypes::DataType::Utf8);
        let values: arrow::array::ArrayRef = sync::Arc::new(arrow::array::StringArray::from(vec![
            None as Option<&str>,
            None,
            None,
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, scalar::ScalarValue::Utf8(None));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_with_nulls_utf8() -> error::Result<()> {
        let mut acc = BytesModeAccumulator::new(&arrow::datatypes::DataType::Utf8);
        let values: arrow::array::ArrayRef = sync::Arc::new(arrow::array::StringArray::from(vec![
            Some("apple"),
            None,
            Some("banana"),
            Some("apple"),
            None,
            None,
            None,
            Some("banana"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, scalar::ScalarValue::Utf8(Some("apple".to_string())));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_single_mode_utf8view() -> error::Result<()> {
        let mut acc = BytesModeAccumulator::new(&arrow::datatypes::DataType::Utf8View);
        let values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::GenericByteViewArray::from(vec![
                Some("apple"),
                Some("banana"),
                Some("apple"),
                Some("orange"),
                Some("banana"),
                Some("apple"),
            ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(
            result,
            scalar::ScalarValue::Utf8View(Some("apple".to_string()))
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_tie_utf8view() -> error::Result<()> {
        let mut acc = BytesModeAccumulator::new(&arrow::datatypes::DataType::Utf8View);
        let values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::GenericByteViewArray::from(vec![
                Some("apple"),
                Some("banana"),
                Some("apple"),
                Some("orange"),
                Some("banana"),
            ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(
            result,
            scalar::ScalarValue::Utf8View(Some("apple".to_string()))
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_all_nulls_utf8view() -> error::Result<()> {
        let mut acc = BytesModeAccumulator::new(&arrow::datatypes::DataType::Utf8View);
        let values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::GenericByteViewArray::from(vec![
                None as Option<&str>,
                None,
                None,
            ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, scalar::ScalarValue::Utf8View(None));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_with_nulls_utf8view() -> error::Result<()> {
        let mut acc = BytesModeAccumulator::new(&arrow::datatypes::DataType::Utf8View);
        let values: arrow::array::ArrayRef =
            sync::Arc::new(arrow::array::GenericByteViewArray::from(vec![
                Some("apple"),
                None,
                Some("banana"),
                Some("apple"),
                None,
                None,
                None,
                Some("banana"),
            ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(
            result,
            scalar::ScalarValue::Utf8View(Some("apple".to_string()))
        );
        Ok(())
    }
}
