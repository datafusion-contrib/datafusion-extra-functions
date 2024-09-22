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

use crate::utils::TestExecution;

mod utils;

static TEST_TABLE: &str = r#"
CREATE TABLE test_table (
    utf8_col VARCHAR,
    int64_col BIGINT,
    float64_col DOUBLE,
    date64_col DATE,
    time64_col TIME
) AS VALUES
    ('apple', 1, 1.0, DATE '2021-01-01', TIME '01:00:00'),
    ('banana', 2, 2.0, DATE '2021-01-02', TIME '02:00:00'),
    ('apple', 2, 2.0, DATE '2021-01-02', TIME '02:00:00'),
    ('orange', 3, 3.0, DATE '2021-01-03', TIME '03:00:00'),
    ('banana', 3, 3.0, DATE '2021-01-03', TIME '03:00:00'),
    ('apple', 3, 3.0, DATE '2021-01-03', TIME '03:00:00'),
    (NULL, NULL, NULL, NULL, NULL);
"#;

#[tokio::test]
async fn test_mode_utf8() {
    let mut execution = TestExecution::new().await.unwrap().with_setup(TEST_TABLE).await;

    let actual = execution.run_and_format("SELECT MODE(utf8_col) FROM test_table").await;

    insta::assert_yaml_snapshot!(actual, @r###"
          - +---------------------------+
          - "| mode(test_table.utf8_col) |"
          - +---------------------------+
          - "| apple                     |"
          - +---------------------------+
    "###);
}
#[tokio::test]
async fn test_mode_int64() {
    let mut execution = TestExecution::new().await.unwrap().with_setup(TEST_TABLE).await;

    let actual = execution.run_and_format("SELECT MODE(int64_col) FROM test_table").await;

    insta::assert_yaml_snapshot!(actual, @r###"
          - +----------------------------+
          - "| mode(test_table.int64_col) |"
          - +----------------------------+
          - "| 3                          |"
          - +----------------------------+
    "###);
}

#[tokio::test]
async fn test_mode_float64() {
    let mut execution = TestExecution::new().await.unwrap().with_setup(TEST_TABLE).await;

    let actual = execution
        .run_and_format("SELECT MODE(float64_col) FROM test_table")
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
          - +------------------------------+
          - "| mode(test_table.float64_col) |"
          - +------------------------------+
          - "| 3.0                          |"
          - +------------------------------+
    "###);
}

#[tokio::test]
async fn test_mode_date64() {
    let mut execution = TestExecution::new().await.unwrap().with_setup(TEST_TABLE).await;

    let actual = execution
        .run_and_format("SELECT MODE(date64_col) FROM test_table")
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
          - +-----------------------------+
          - "| mode(test_table.date64_col) |"
          - +-----------------------------+
          - "| 2021-01-03                  |"
          - +-----------------------------+
    "###);
}

#[tokio::test]
async fn test_mode_time64() {
    let mut execution = TestExecution::new().await.unwrap().with_setup(TEST_TABLE).await;

    let actual = execution
        .run_and_format("SELECT MODE(time64_col) FROM test_table")
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
          - +-----------------------------+
          - "| mode(test_table.time64_col) |"
          - +-----------------------------+
          - "| 03:00:00                    |"
          - +-----------------------------+
    "###);
}
