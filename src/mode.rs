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

use crate::common;

use datafusion::{arrow, common as df_common, error, logical_expr};
use std::fmt;

make_udaf_expr_and_func!(
    ModeFunction,
    mode,
    x,
    "Calculates the most frequent value.",
    mode_udaf
);

/// The `ModeFunction` calculates the mode (most frequent value) from a set of values.
///
/// - Null values are ignored during the calculation.
/// - If multiple values have the same frequency, the MAX value with the highest frequency is returned.
#[derive(Eq, Hash, PartialEq)]
pub struct ModeFunction {
    signature: logical_expr::Signature,
}

impl fmt::Debug for ModeFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModeFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for ModeFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl ModeFunction {
    pub fn new() -> Self {
        Self {
            signature: logical_expr::Signature::variadic_any(logical_expr::Volatility::Immutable),
        }
    }
}

impl logical_expr::AggregateUDFImpl for ModeFunction {
    fn name(&self) -> &str {
        "mode"
    }

    fn signature(&self) -> &logical_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> error::Result<arrow::datatypes::DataType> {
        Ok(arg_types[0].clone())
    }

    fn state_fields(
        &self,
        args: logical_expr::function::StateFieldsArgs,
    ) -> error::Result<Vec<arrow::datatypes::FieldRef>> {
        let value_type = args.input_fields[0].data_type().clone();

        Ok(vec![
            arrow::datatypes::Field::new("values", value_type, true).into(),
            arrow::datatypes::Field::new("frequencies", arrow::datatypes::DataType::UInt64, true)
                .into(),
        ])
    }

    fn accumulator(
        &self,
        acc_args: logical_expr::function::AccumulatorArgs,
    ) -> error::Result<Box<dyn logical_expr::Accumulator>> {
        let data_type = &acc_args.exprs[0].data_type(acc_args.schema)?;

        let accumulator: Box<dyn logical_expr::Accumulator> = match data_type {
            arrow::datatypes::DataType::Int8 => Box::new(common::mode::PrimitiveModeAccumulator::<
                arrow::datatypes::Int8Type,
            >::new(data_type)),
            arrow::datatypes::DataType::Int16 => {
                Box::new(common::mode::PrimitiveModeAccumulator::<
                    arrow::datatypes::Int16Type,
                >::new(data_type))
            }
            arrow::datatypes::DataType::Int32 => {
                Box::new(common::mode::PrimitiveModeAccumulator::<
                    arrow::datatypes::Int32Type,
                >::new(data_type))
            }
            arrow::datatypes::DataType::Int64 => {
                Box::new(common::mode::PrimitiveModeAccumulator::<
                    arrow::datatypes::Int64Type,
                >::new(data_type))
            }
            arrow::datatypes::DataType::UInt8 => {
                Box::new(common::mode::PrimitiveModeAccumulator::<
                    arrow::datatypes::UInt8Type,
                >::new(data_type))
            }
            arrow::datatypes::DataType::UInt16 => {
                Box::new(common::mode::PrimitiveModeAccumulator::<
                    arrow::datatypes::UInt16Type,
                >::new(data_type))
            }
            arrow::datatypes::DataType::UInt32 => {
                Box::new(common::mode::PrimitiveModeAccumulator::<
                    arrow::datatypes::UInt32Type,
                >::new(data_type))
            }
            arrow::datatypes::DataType::UInt64 => {
                Box::new(common::mode::PrimitiveModeAccumulator::<
                    arrow::datatypes::UInt64Type,
                >::new(data_type))
            }

            arrow::datatypes::DataType::Date32 => {
                Box::new(common::mode::PrimitiveModeAccumulator::<
                    arrow::datatypes::Date32Type,
                >::new(data_type))
            }
            arrow::datatypes::DataType::Date64 => {
                Box::new(common::mode::PrimitiveModeAccumulator::<
                    arrow::datatypes::Date64Type,
                >::new(data_type))
            }
            arrow::datatypes::DataType::Time32(arrow::datatypes::TimeUnit::Millisecond) => {
                Box::new(common::mode::PrimitiveModeAccumulator::<
                    arrow::datatypes::Time32MillisecondType,
                >::new(data_type))
            }
            arrow::datatypes::DataType::Time32(arrow::datatypes::TimeUnit::Second) => {
                Box::new(common::mode::PrimitiveModeAccumulator::<
                    arrow::datatypes::Time32SecondType,
                >::new(data_type))
            }
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond) => {
                Box::new(common::mode::PrimitiveModeAccumulator::<
                    arrow::datatypes::Time64MicrosecondType,
                >::new(data_type))
            }
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond) => {
                Box::new(common::mode::PrimitiveModeAccumulator::<
                    arrow::datatypes::Time64NanosecondType,
                >::new(data_type))
            }
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
                Box::new(common::mode::PrimitiveModeAccumulator::<
                    arrow::datatypes::TimestampMicrosecondType,
                >::new(data_type))
            }
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
                Box::new(common::mode::PrimitiveModeAccumulator::<
                    arrow::datatypes::TimestampMillisecondType,
                >::new(data_type))
            }
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
                Box::new(common::mode::PrimitiveModeAccumulator::<
                    arrow::datatypes::TimestampNanosecondType,
                >::new(data_type))
            }
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => {
                Box::new(common::mode::PrimitiveModeAccumulator::<
                    arrow::datatypes::TimestampSecondType,
                >::new(data_type))
            }

            arrow::datatypes::DataType::Float16 => Box::new(common::mode::FloatModeAccumulator::<
                arrow::datatypes::Float16Type,
            >::new(data_type)),
            arrow::datatypes::DataType::Float32 => Box::new(common::mode::FloatModeAccumulator::<
                arrow::datatypes::Float32Type,
            >::new(data_type)),
            arrow::datatypes::DataType::Float64 => Box::new(common::mode::FloatModeAccumulator::<
                arrow::datatypes::Float64Type,
            >::new(data_type)),

            arrow::datatypes::DataType::Utf8
            | arrow::datatypes::DataType::Utf8View
            | arrow::datatypes::DataType::LargeUtf8 => {
                Box::new(common::mode::BytesModeAccumulator::new(data_type))
            }
            _ => {
                return df_common::not_impl_err!(
                    "Unsupported data type: {:?} for mode function",
                    data_type
                );
            }
        };

        Ok(accumulator)
    }
}
