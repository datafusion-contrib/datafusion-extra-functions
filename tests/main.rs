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
async fn test_mode() {
    let mut execution = utils::TestExecution::new()
        .await
        .unwrap()
        .with_setup(TEST_TABLE)
        .await;

    let actual = execution
        .run_and_format("SELECT MODE(utf8_col) FROM test_table")
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
          - +---------------------------+
          - "| mode(test_table.utf8_col) |"
          - +---------------------------+
          - "| apple                     |"
          - +---------------------------+
    "###);

    let actual = execution
        .run_and_format("SELECT MODE(int64_col) FROM test_table")
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
          - +----------------------------+
          - "| mode(test_table.int64_col) |"
          - +----------------------------+
          - "| 3                          |"
          - +----------------------------+
    "###);

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
    let mut execution = TestExecution::new()
        .await
        .unwrap()
        .with_setup(TEST_TABLE)
        .await;

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

#[tokio::test]
async fn test_max_by_and_min_by() {
    let mut execution = TestExecution::new().await.unwrap();

    // Test max_by with numbers
    let actual = execution
        .run_and_format(
            "SELECT max_by(x, y) FROM VALUES (1, 10), (2, 5), (3, 15), (4, 8) as tab(x, y);",
        )
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +---------------------+
    - "| max_by(tab.x,tab.y) |"
    - +---------------------+
    - "| 3                   |"
    - +---------------------+
    "###);

    // Test min_by with numbers
    let actual = execution
        .run_and_format(
            "SELECT min_by(x, y) FROM VALUES (1, 10), (2, 5), (3, 15), (4, 8) as tab(x, y);",
        )
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +---------------------+
    - "| min_by(tab.x,tab.y) |"
    - +---------------------+
    - "| 2                   |"
    - +---------------------+
    "###);

    // Test max_by with strings
    let actual = execution
        .run_and_format("SELECT max_by(name, length(name)) FROM VALUES ('Alice'), ('Bob'), ('Charlie') as tab(name);")
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +---------------------------------------------+
    - "| max_by(tab.name,character_length(tab.name)) |"
    - +---------------------------------------------+
    - "| Charlie                                     |"
    - +---------------------------------------------+
    "###);

    // Test min_by with strings
    let actual = execution
        .run_and_format("SELECT min_by(name, length(name)) FROM VALUES ('Alice'), ('Bob'), ('Charlie') as tab(name);")
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +---------------------------------------------+
    - "| min_by(tab.name,character_length(tab.name)) |"
    - +---------------------------------------------+
    - "| Bob                                         |"
    - +---------------------------------------------+
    "###);

    // Test max_by with null values
    let actual = execution
        .run_and_format(
            "SELECT max_by(x, y) FROM VALUES (1, 10), (2, null), (3, 15), (null, 8) as tab(x, y);",
        )
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +---------------------+
    - "| max_by(tab.x,tab.y) |"
    - +---------------------+
    - "| 3                   |"
    - +---------------------+
    "###);

    // Test min_by with null values
    let actual = execution
        .run_and_format(
            "SELECT min_by(x, y) FROM VALUES (1, 10), (2, null), (3, 15), (null, 8) as tab(x, y);",
        )
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +---------------------+
    - "| min_by(tab.x,tab.y) |"
    - +---------------------+
    - "|                     |"
    - +---------------------+
    "###);

    // Test max_by with a single value
    let actual = execution
        .run_and_format("SELECT max_by(x, y) FROM VALUES (1, 10) as tab(x, y);")
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +---------------------+
    - "| max_by(tab.x,tab.y) |"
    - +---------------------+
    - "| 1                   |"
    - +---------------------+
    "###);

    // Test min_by with a single value
    let actual = execution
        .run_and_format("SELECT min_by(x, y) FROM VALUES (1, 10) as tab(x, y);")
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +---------------------+
    - "| min_by(tab.x,tab.y) |"
    - +---------------------+
    - "| 1                   |"
    - +---------------------+
    "###);

    // Test max_by with an empty set
    let actual = execution
        .run_and_format(
            "SELECT max_by(x, y) FROM (SELECT * FROM (VALUES (1, 10)) WHERE 1=0) as tab(x, y);",
        )
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +---------------------+
    - "| max_by(tab.x,tab.y) |"
    - +---------------------+
    - "|                     |"
    - +---------------------+
    "###);

    // Test min_by with an empty set
    let actual = execution
        .run_and_format(
            "SELECT min_by(x, y) FROM (SELECT * FROM (VALUES (1, 10)) WHERE 1=0) as tab(x, y);",
        )
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +---------------------+
    - "| min_by(tab.x,tab.y) |"
    - +---------------------+
    - "|                     |"
    - +---------------------+
    "###);
}

#[tokio::test]
async fn test_kurtosis_pop() {
    let mut execution = TestExecution::new()
        .await
        .unwrap()
        .with_setup(TEST_TABLE)
        .await;

    // Test with int64
    let actual = execution
        .run_and_format("SELECT kurtosis_pop(int64_col) FROM test_table")
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
        - +------------------------------------+
        - "| kurtosis_pop(test_table.int64_col) |"
        - +------------------------------------+
        - "| -0.9599999999999755                |"
        - +------------------------------------+
    "###);

    // Test with float64
    let actual = execution
        .run_and_format("SELECT kurtosis_pop(float64_col) FROM test_table")
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +--------------------------------------+
    - "| kurtosis_pop(test_table.float64_col) |"
    - +--------------------------------------+
    - "| -0.9599999999999755                  |"
    - +--------------------------------------+
"###);

    let actual = execution
        .run_and_format("SELECT kurtosis_pop(col) FROM VALUES (1.0) as tab(col)")
        .await;
    insta::assert_yaml_snapshot!(actual, @r###"
    - +-----------------------+
    - "| kurtosis_pop(tab.col) |"
    - +-----------------------+
    - "|                       |"
    - +-----------------------+
"###);

    let actual = execution.run_and_format("SELECT kurtosis_pop(1.0)").await;
    insta::assert_yaml_snapshot!(actual, @r###"
    - +--------------------------+
    - "| kurtosis_pop(Float64(1)) |"
    - +--------------------------+
    - "|                          |"
    - +--------------------------+
"###);

    let actual = execution.run_and_format("SELECT kurtosis_pop(null)").await;
    insta::assert_yaml_snapshot!(actual, @r###"
- +--------------------+
- "| kurtosis_pop(NULL) |"
- +--------------------+
- "|                    |"
- +--------------------+
"###);
}

#[tokio::test]
async fn test_skewness() {
    let mut execution = TestExecution::new()
        .await
        .unwrap()
        .with_setup(TEST_TABLE)
        .await;

    // Test with int64
    let actual = execution
        .run_and_format("SELECT skewness(int64_col) FROM test_table")
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
        - +--------------------------------+
        - "| skewness(test_table.int64_col) |"
        - +--------------------------------+
        - "| -0.8573214099741201            |"
        - +--------------------------------+
    "###);

    // Test with float64
    let actual = execution
        .run_and_format("SELECT skewness(float64_col) FROM test_table")
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +----------------------------------+
    - "| skewness(test_table.float64_col) |"
    - +----------------------------------+
    - "| -0.8573214099741201              |"
    - +----------------------------------+
"###);

    // Test with single value
    let actual = execution.run_and_format("SELECT skewness(1.0)").await;

    insta::assert_yaml_snapshot!(actual, @r###"
        - +----------------------+
        - "| skewness(Float64(1)) |"
        - +----------------------+
        - "|                      |"
        - +----------------------+
    "###);

    let actual = execution
        .run_and_format("SELECT skewness(col) FROM VALUES (1.0), (2.0) as tab(col)")
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
        - +-------------------+
        - "| skewness(tab.col) |"
        - +-------------------+
        - "|                   |"
        - +-------------------+
    "###);
}

#[tokio::test]
async fn test_kurtosis() {
    let mut execution = TestExecution::new().await.unwrap();

    let actual = execution
        .run_and_format(
            "SELECT kurtosis(col) FROM VALUES (1.0), (10.0), (100.0), (10.0), (1.0) as tab(col);",
        )
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
          - +-------------------+
          - "| kurtosis(tab.col) |"
          - +-------------------+
          - "| 4.777292927667962 |"
          - +-------------------+
    "###);

    // TODO: waiting for TypeSignatureClass::Number to be implemented
    // let actual = execution
    //     .run_and_format("SELECT kurtosis(col) FROM VALUES ('1'), ('10'), ('100'), ('10'), ('1') as tab(col);")
    //     .await;
    //
    // insta::assert_yaml_snapshot!(actual, @r###"
    //       - +-------------------+
    //       - "| kurtosis(tab.col) |"
    //       - +-------------------+
    //       - "| 4.777292927667962 |"
    //       - +-------------------+
    // "###);

    let actual = execution
        .run_and_format("SELECT kurtosis(col) FROM VALUES (1.0), (2.0), (3.0) as tab(col);")
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
          - +-------------------+
          - "| kurtosis(tab.col) |"
          - +-------------------+
          - "|                   |"
          - +-------------------+
    "###);

    let actual = execution.run_and_format("SELECT kurtosis(1);").await;

    insta::assert_yaml_snapshot!(actual, @r###"
          - +--------------------+
          - "| kurtosis(Int64(1)) |"
          - +--------------------+
          - "|                    |"
          - +--------------------+
    "###);

    let actual = execution.run_and_format("SELECT kurtosis(1.0);").await;

    insta::assert_yaml_snapshot!(actual, @r###"
          - +----------------------+
          - "| kurtosis(Float64(1)) |"
          - +----------------------+
          - "|                      |"
          - +----------------------+
    "###);

    let actual = execution.run_and_format("SELECT kurtosis(null);").await;

    insta::assert_yaml_snapshot!(actual, @r###"
          - +----------------+
          - "| kurtosis(NULL) |"
          - +----------------+
          - "|                |"
          - +----------------+
    "###);
}
