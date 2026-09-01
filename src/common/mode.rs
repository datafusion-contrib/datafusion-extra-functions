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

mod bytes;
mod native;

pub use bytes::BytesModeAccumulator;
pub use native::FloatModeAccumulator;
pub use native::PrimitiveModeAccumulator;

use datafusion::{arrow, common, error};

/// Unwraps the two list-typed state columns (`values`, `frequencies`) row by
/// row and passes each row's inner arrays to `f`.
pub(crate) fn for_each_state_row(
    states: &[arrow::array::ArrayRef],
    mut f: impl FnMut(&arrow::array::ArrayRef, &arrow::array::Int64Array) -> error::Result<()>,
) -> error::Result<()> {
    if states.is_empty() {
        return Ok(());
    }

    let values = common::cast::as_list_array(&states[0])?;
    let counts = common::cast::as_list_array(&states[1])?;

    for (values, counts) in values.iter().zip(counts.iter()) {
        if let (Some(values), Some(counts)) = (values, counts) {
            let counts = common::cast::as_primitive_array::<arrow::datatypes::Int64Type>(&counts)?;
            f(&values, counts)?;
        }
    }

    Ok(())
}
