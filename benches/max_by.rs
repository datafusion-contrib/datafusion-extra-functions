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

use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::MemTable;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion_extra_functions::max_min_by::max_by_udaf;

const BATCH_SIZE: usize = 8192;
const BATCHES_PER_PARTITION: usize = 32;
const NUM_PARTITIONS: usize = 4;
const NUM_GROUPS: usize = 1024;

#[derive(Clone, Copy)]
enum KeyType {
    Int64,
    Utf8,
}

impl KeyType {
    fn name(self) -> &'static str {
        match self {
            Self::Int64 => "int64_key",
            Self::Utf8 => "utf8_key",
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::Int64 => DataType::Int64,
            Self::Utf8 => DataType::Utf8,
        }
    }
}

fn schema(key_type: KeyType) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("k", key_type.data_type(), false),
    ]))
}

fn mixed(row: usize) -> u64 {
    let mut value = row as u64;
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn batch(key_type: KeyType, row_offset: usize, schema: SchemaRef) -> RecordBatch {
    let groups = Arc::new(Int64Array::from_iter_values(
        (0..BATCH_SIZE).map(|i| ((row_offset + i) % NUM_GROUPS) as i64),
    )) as ArrayRef;
    let values = Arc::new(Int64Array::from_iter_values(
        (0..BATCH_SIZE).map(|i| (row_offset + i) as i64),
    )) as ArrayRef;
    let keys = match key_type {
        KeyType::Int64 => Arc::new(Int64Array::from_iter_values(
            (0..BATCH_SIZE).map(|i| mixed(row_offset + i) as i64),
        )) as ArrayRef,
        KeyType::Utf8 => Arc::new(StringArray::from_iter_values(
            (0..BATCH_SIZE).map(|i| format!("{:016x}", mixed(row_offset + i))),
        )) as ArrayRef,
    };
    RecordBatch::try_new(schema, vec![groups, values, keys]).unwrap()
}

fn partitions(key_type: KeyType, schema: SchemaRef) -> Vec<Vec<RecordBatch>> {
    (0..NUM_PARTITIONS)
        .map(|partition| {
            (0..BATCHES_PER_PARTITION)
                .map(|batch_idx| {
                    let row_offset = (partition * BATCHES_PER_PARTITION + batch_idx) * BATCH_SIZE;
                    batch(key_type, row_offset, Arc::clone(&schema))
                })
                .collect()
        })
        .collect()
}

fn plan(
    key_type: KeyType,
) -> (
    tokio::runtime::Runtime,
    Arc<dyn ExecutionPlan>,
    SessionContext,
) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let context = SessionContext::new_with_config(
        SessionConfig::new().with_target_partitions(NUM_PARTITIONS),
    );
    context.register_udaf(max_by_udaf().as_ref().clone());

    let schema = schema(key_type);
    let table = MemTable::try_new(Arc::clone(&schema), partitions(key_type, schema)).unwrap();
    context.register_table("t", Arc::new(table)).unwrap();
    let physical_plan = runtime.block_on(async {
        context
            .sql("SELECT g, max_by(v, k) FROM t GROUP BY g")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    });
    (runtime, physical_plan, context)
}

fn max_by_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("max_by_grouped_1m_rows");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(5));

    for key_type in [KeyType::Int64, KeyType::Utf8] {
        let (runtime, plan, context) = plan(key_type);
        let output = runtime
            .block_on(collect(
                reset_plan_states(Arc::clone(&plan)).unwrap(),
                context.task_ctx(),
            ))
            .unwrap();
        assert_eq!(
            output.iter().map(RecordBatch::num_rows).sum::<usize>(),
            NUM_GROUPS
        );
        group.bench_with_input(
            BenchmarkId::new("execute", key_type.name()),
            &key_type,
            |bencher, _| {
                bencher.iter(|| {
                    let plan = reset_plan_states(Arc::clone(&plan)).unwrap();
                    runtime.block_on(collect(plan, context.task_ctx())).unwrap()
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, max_by_benchmark);
criterion_main!(benches);
