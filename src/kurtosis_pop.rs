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

// Copired from `datafusion/functions-aggregate/src/kurtosis_pop.rs`
// Originally authored by goldmedal

use datafusion::arrow::array::{Float64Array, UInt64Array};
use datafusion::{arrow, common, error, logical_expr, scalar};
use std::{fmt, mem};

make_udaf_expr_and_func!(
    KurtosisPopFunction,
    kurtosis_pop,
    x,
    "Calculates the excess kurtosis (Fisher’s definition) without bias correction.",
    kurtosis_pop_udaf
);

#[derive(Eq, Hash, PartialEq)]
pub struct KurtosisPopFunction {
    signature: logical_expr::Signature,
}

impl fmt::Debug for KurtosisPopFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KurtosisPopFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for KurtosisPopFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl KurtosisPopFunction {
    pub fn new() -> Self {
        Self {
            signature: logical_expr::Signature::exact(
                vec![arrow::datatypes::DataType::Float64],
                logical_expr::Volatility::Immutable,
            ),
        }
    }
}

impl logical_expr::AggregateUDFImpl for KurtosisPopFunction {
    fn name(&self) -> &str {
        "kurtosis_pop"
    }

    fn signature(&self) -> &logical_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[arrow::datatypes::DataType],
    ) -> error::Result<arrow::datatypes::DataType> {
        Ok(arrow::datatypes::DataType::Float64)
    }

    fn state_fields(
        &self,
        _args: logical_expr::function::StateFieldsArgs,
    ) -> error::Result<Vec<arrow::datatypes::FieldRef>> {
        Ok(vec![
            arrow::datatypes::Field::new("count", arrow::datatypes::DataType::UInt64, true).into(),
            arrow::datatypes::Field::new("sum", arrow::datatypes::DataType::Float64, true).into(),
            arrow::datatypes::Field::new("sum_sqr", arrow::datatypes::DataType::Float64, true)
                .into(),
            arrow::datatypes::Field::new("sum_cub", arrow::datatypes::DataType::Float64, true)
                .into(),
            arrow::datatypes::Field::new("sum_four", arrow::datatypes::DataType::Float64, true)
                .into(),
        ])
    }

    fn accumulator(
        &self,
        _acc_args: logical_expr::function::AccumulatorArgs,
    ) -> error::Result<Box<dyn logical_expr::Accumulator>> {
        Ok(Box::new(KurtosisPopAccumulator::new()))
    }
}

/// Accumulator for calculating the excess kurtosis (Fisher’s definition) without bias correction.
/// This implementation follows the [DuckDB implementation]:
/// <https://github.com/duckdb/duckdb/blob/main/src/core_functions/aggregate/distributive/kurtosis.cpp>
#[derive(Debug, Default)]
pub struct KurtosisPopAccumulator {
    count: u64,
    sum: f64,
    sum_sqr: f64,
    sum_cub: f64,
    sum_four: f64,
}

impl KurtosisPopAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            sum_sqr: 0.0,
            sum_cub: 0.0,
            sum_four: 0.0,
        }
    }
}

impl logical_expr::Accumulator for KurtosisPopAccumulator {
    fn update_batch(&mut self, values: &[arrow::array::ArrayRef]) -> error::Result<()> {
        let array = common::cast::as_float64_array(&values[0])?;
        for value in array.iter().flatten() {
            self.count += 1;
            self.sum += value;
            self.sum_sqr += value.powi(2);
            self.sum_cub += value.powi(3);
            self.sum_four += value.powi(4);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[arrow::array::ArrayRef]) -> error::Result<()> {
        let counts = common::downcast_value!(states[0], UInt64Array);
        let sums = common::downcast_value!(states[1], Float64Array);
        let sum_sqrs = common::downcast_value!(states[2], Float64Array);
        let sum_cubs = common::downcast_value!(states[3], Float64Array);
        let sum_fours = common::downcast_value!(states[4], Float64Array);

        for i in 0..counts.len() {
            let c = counts.value(i);
            if c == 0 {
                continue;
            }
            self.count += c;
            self.sum += sums.value(i);
            self.sum_sqr += sum_sqrs.value(i);
            self.sum_cub += sum_cubs.value(i);
            self.sum_four += sum_fours.value(i);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> error::Result<scalar::ScalarValue> {
        if self.count < 1 {
            return Ok(scalar::ScalarValue::Float64(None));
        }

        let count_64 = 1_f64 / self.count as f64;
        let m4 = count_64
            * (self.sum_four - 4.0 * self.sum_cub * self.sum * count_64
                + 6.0 * self.sum_sqr * self.sum.powi(2) * count_64.powi(2)
                - 3.0 * self.sum.powi(4) * count_64.powi(3));

        let m2 = (self.sum_sqr - self.sum.powi(2) * count_64) * count_64;
        if m2 <= 0.0 {
            return Ok(scalar::ScalarValue::Float64(None));
        }

        let target = m4 / (m2.powi(2)) - 3.0;
        Ok(scalar::ScalarValue::Float64(Some(target)))
    }

    fn size(&self) -> usize {
        mem::size_of_val(self)
    }

    fn state(&mut self) -> error::Result<Vec<scalar::ScalarValue>> {
        Ok(vec![
            scalar::ScalarValue::from(self.count),
            scalar::ScalarValue::from(self.sum),
            scalar::ScalarValue::from(self.sum_sqr),
            scalar::ScalarValue::from(self.sum_cub),
            scalar::ScalarValue::from(self.sum_four),
        ])
    }
}
