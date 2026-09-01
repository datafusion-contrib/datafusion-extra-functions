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

use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, Field, Float16Type, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use datafusion::{arrow, common as df_common, error, logical_expr};
use std::{fmt, hash};

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
/// - If multiple values share the highest frequency, the smallest value is
///   returned, matching PostgreSQL's `mode()` ordered-set aggregate.
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

    fn return_type(&self, arg_types: &[DataType]) -> error::Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn state_fields(
        &self,
        args: logical_expr::function::StateFieldsArgs,
    ) -> error::Result<Vec<arrow::datatypes::FieldRef>> {
        let value_type = match args.input_fields[0].data_type() {
            DataType::Utf8View => DataType::Utf8,
            other => other.clone(),
        };

        Ok(vec![
            Field::new_list("values", Field::new_list_field(value_type, true), true).into(),
            Field::new_list(
                "frequencies",
                Field::new_list_field(DataType::Int64, true),
                true,
            )
            .into(),
        ])
    }

    fn accumulator(
        &self,
        acc_args: logical_expr::function::AccumulatorArgs,
    ) -> error::Result<Box<dyn logical_expr::Accumulator>> {
        fn primitive<T>(data_type: &DataType) -> Box<dyn logical_expr::Accumulator>
        where
            T: arrow::array::ArrowPrimitiveType + Send + fmt::Debug,
            T::Native: Eq + hash::Hash + Clone + PartialOrd + fmt::Debug,
        {
            Box::new(common::mode::PrimitiveModeAccumulator::<T>::new(data_type))
        }

        fn float<T>(data_type: &DataType) -> Box<dyn logical_expr::Accumulator>
        where
            T: arrow::array::ArrowPrimitiveType + Send + fmt::Debug,
            T::Native: PartialOrd + fmt::Debug + Clone,
        {
            Box::new(common::mode::FloatModeAccumulator::<T>::new(data_type))
        }

        let data_type = &acc_args.exprs[0].data_type(acc_args.schema)?;

        Ok(match data_type {
            DataType::Int8 => primitive::<Int8Type>(data_type),
            DataType::Int16 => primitive::<Int16Type>(data_type),
            DataType::Int32 => primitive::<Int32Type>(data_type),
            DataType::Int64 => primitive::<Int64Type>(data_type),
            DataType::UInt8 => primitive::<UInt8Type>(data_type),
            DataType::UInt16 => primitive::<UInt16Type>(data_type),
            DataType::UInt32 => primitive::<UInt32Type>(data_type),
            DataType::UInt64 => primitive::<UInt64Type>(data_type),

            DataType::Date32 => primitive::<Date32Type>(data_type),
            DataType::Date64 => primitive::<Date64Type>(data_type),
            DataType::Time32(TimeUnit::Second) => primitive::<Time32SecondType>(data_type),
            DataType::Time32(TimeUnit::Millisecond) => {
                primitive::<Time32MillisecondType>(data_type)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                primitive::<Time64MicrosecondType>(data_type)
            }
            DataType::Time64(TimeUnit::Nanosecond) => primitive::<Time64NanosecondType>(data_type),
            DataType::Timestamp(TimeUnit::Second, _) => primitive::<TimestampSecondType>(data_type),
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                primitive::<TimestampMillisecondType>(data_type)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                primitive::<TimestampMicrosecondType>(data_type)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                primitive::<TimestampNanosecondType>(data_type)
            }

            DataType::Float16 => float::<Float16Type>(data_type),
            DataType::Float32 => float::<Float32Type>(data_type),
            DataType::Float64 => float::<Float64Type>(data_type),

            DataType::Utf8 | DataType::Utf8View => {
                Box::new(common::mode::BytesModeAccumulator::new(data_type))
            }
            _ => {
                return df_common::not_impl_err!(
                    "Unsupported data type: {data_type:?} for mode function"
                );
            }
        })
    }
}
