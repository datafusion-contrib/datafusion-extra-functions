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

use arrow::util::bench_util::{create_primitive_array, create_string_array};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::{arrow, logical_expr::Accumulator};
use datafusion_extra_functions::mode::{BytesModeAccumulator, PrimitiveModeAccumulator};
use std::{hint, slice, sync};

fn prepare_primitive_mode_accumulator() -> Box<dyn Accumulator> {
    Box::new(
        PrimitiveModeAccumulator::<arrow::datatypes::Int32Type>::new(
            &arrow::datatypes::DataType::Int32,
        ),
    )
}

fn prepare_bytes_mode_accumulator() -> Box<dyn Accumulator> {
    Box::new(BytesModeAccumulator::new(&arrow::datatypes::DataType::Utf8))
}

fn mode_bench_primitive(c: &mut Criterion, name: &str, values: arrow::array::ArrayRef) {
    let mut accumulator = prepare_primitive_mode_accumulator();
    c.bench_function(name, |b| {
        b.iter(|| {
            accumulator.update_batch(slice::from_ref(&values)).unwrap();
            hint::black_box(accumulator.evaluate().unwrap());
        });
    });
}

fn mode_bench_bytes(c: &mut Criterion, name: &str, values: arrow::array::ArrayRef) {
    let mut accumulator = prepare_bytes_mode_accumulator();
    c.bench_function(name, |b| {
        b.iter(|| {
            accumulator.update_batch(slice::from_ref(&values)).unwrap();
            hint::black_box(accumulator.evaluate().unwrap());
        });
    });
}

fn mode_benchmark(c: &mut Criterion) {
    let sizes = [100_000, 1_000_000];
    let null_percentages = [0.0, 0.3, 0.7];

    for &size in &sizes {
        for &null_percentage in &null_percentages {
            let values = sync::Arc::new(create_primitive_array::<arrow::datatypes::Int32Type>(
                size,
                null_percentage,
            )) as arrow::array::ArrayRef;
            let name = format!(
                "PrimitiveModeAccumulator: {} elements, {}% nulls",
                size,
                null_percentage * 100.0
            );
            mode_bench_primitive(c, &name, values);
        }
    }

    for &size in &sizes {
        for &null_percentage in &null_percentages {
            let values = sync::Arc::new(create_string_array::<i32>(size, null_percentage))
                as arrow::array::ArrayRef;
            let name = format!(
                "BytesModeAccumulator: {} elements, {}% nulls",
                size,
                null_percentage * 100.0
            );
            mode_bench_bytes(c, &name, values);
        }
    }
}

criterion_group!(benches, mode_benchmark);
criterion_main!(benches);
