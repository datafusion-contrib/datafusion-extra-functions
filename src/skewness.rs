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
use datafusion::{arrow, logical_expr, scalar};
use std::ops::{Div, Mul, Sub};
use std::{fmt, mem};

make_udaf_expr_and_func!(
    SkewnessFunc,
    skewness,
    x,
    "Computes the skewness value.",
    skewness_udaf
);

#[derive(Eq, Hash, PartialEq)]
pub struct SkewnessFunc {
    name: String,
    signature: logical_expr::Signature,
}

impl fmt::Debug for SkewnessFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SkewnessFunc")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for SkewnessFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SkewnessFunc {
    pub fn new() -> Self {
        Self {
            name: "skewness".to_string(),
            signature: logical_expr::Signature::exact(
                vec![arrow::datatypes::DataType::Float64],
                logical_expr::Volatility::Immutable,
            ),
        }
    }
}

impl logical_expr::AggregateUDFImpl for SkewnessFunc {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &logical_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[arrow::datatypes::DataType],
    ) -> datafusion::common::Result<arrow::datatypes::DataType> {
        Ok(arrow::datatypes::DataType::Float64)
    }

    fn accumulator(
        &self,
        _acc_args: logical_expr::function::AccumulatorArgs,
    ) -> datafusion::common::Result<Box<dyn logical_expr::Accumulator>> {
        Ok(Box::new(SkewnessAccumulator::new()))
    }

    fn state_fields(
        &self,
        _args: logical_expr::function::StateFieldsArgs,
    ) -> datafusion::common::Result<Vec<arrow::datatypes::FieldRef>> {
        Ok(vec![
            arrow::datatypes::Field::new("count", arrow::datatypes::DataType::UInt64, true).into(),
            arrow::datatypes::Field::new("sum", arrow::datatypes::DataType::Float64, true).into(),
            arrow::datatypes::Field::new("sum_sqr", arrow::datatypes::DataType::Float64, true)
                .into(),
            arrow::datatypes::Field::new("sum_cub", arrow::datatypes::DataType::Float64, true)
                .into(),
        ])
    }
}

/// Accumulator for calculating the skewness
/// This implementation follows the DuckDB implementation:
/// <https://github.com/duckdb/duckdb/blob/main/src/core_functions/aggregate/distributive/skew.cpp>
#[derive(Debug)]
pub struct SkewnessAccumulator {
    count: u64,
    sum: f64,
    sum_sqr: f64,
    sum_cub: f64,
}

impl SkewnessAccumulator {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0f64,
            sum_sqr: 0f64,
            sum_cub: 0f64,
        }
    }
}

impl logical_expr::Accumulator for SkewnessAccumulator {
    fn update_batch(
        &mut self,
        values: &[arrow::array::ArrayRef],
    ) -> datafusion::common::Result<()> {
        let array = values[0].as_primitive::<arrow::datatypes::Float64Type>();
        for val in array.iter().flatten() {
            self.count += 1;
            self.sum += val;
            self.sum_sqr += val.powi(2);
            self.sum_cub += val.powi(3);
        }
        Ok(())
    }
    fn evaluate(&mut self) -> datafusion::common::Result<scalar::ScalarValue> {
        if self.count <= 2 {
            return Ok(scalar::ScalarValue::Float64(None));
        }
        let count = self.count as f64;
        let t1 = 1f64 / count;
        let p = (t1 * (self.sum_sqr - self.sum * self.sum * t1))
            .powi(3)
            .max(0f64);
        let div = p.sqrt();
        if div == 0f64 {
            return Ok(scalar::ScalarValue::Float64(None));
        }
        let t2 = count.mul(count.sub(1f64)).sqrt().div(count.sub(2f64));
        let res = t2
            * t1
            * (self.sum_cub - 3f64 * self.sum_sqr * self.sum * t1
                + 2f64 * self.sum.powi(3) * t1 * t1)
            / div;
        Ok(scalar::ScalarValue::Float64(Some(res)))
    }

    fn size(&self) -> usize {
        mem::size_of_val(self)
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<scalar::ScalarValue>> {
        Ok(vec![
            scalar::ScalarValue::from(self.count),
            scalar::ScalarValue::from(self.sum),
            scalar::ScalarValue::from(self.sum_sqr),
            scalar::ScalarValue::from(self.sum_cub),
        ])
    }

    fn merge_batch(&mut self, states: &[arrow::array::ArrayRef]) -> datafusion::common::Result<()> {
        let counts = states[0].as_primitive::<arrow::datatypes::UInt64Type>();
        let sums = states[1].as_primitive::<arrow::datatypes::Float64Type>();
        let sum_sqrs = states[2].as_primitive::<arrow::datatypes::Float64Type>();
        let sum_cubs = states[3].as_primitive::<arrow::datatypes::Float64Type>();

        for i in 0..counts.len() {
            let c = counts.value(i);
            if c == 0 {
                continue;
            }
            self.count += c;
            self.sum += sums.value(i);
            self.sum_sqr += sum_sqrs.value(i);
            self.sum_cub += sum_cubs.value(i);
        }
        Ok(())
    }
}
