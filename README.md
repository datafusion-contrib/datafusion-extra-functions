# datafusion-extra-functions

[![CI](https://github.com/datafusion-contrib/datafusion-extra-functions/actions/workflows/ci.yml/badge.svg?event=push)](https://github.com/datafusion-contrib/datafusion-extra-functions/actions/workflows/ci.yml?query=branch%3Amain)
[![Crates.io](https://img.shields.io/crates/v/datafusion-extra-functions?color=green)](https://crates.io/crates/datafusion-extra-functions)

Extra aggregate functions for [Apache DataFusion](https://datafusion.apache.org/). This is not an official Apache Software Foundation release.

Version `0.5.2` supports DataFusion `54.0` and Rust edition 2024.

## Installation

```sh
cargo add datafusion-extra-functions
```

Register all functions with your session context:

```rust
datafusion_extra_functions::register_all_extra_functions(&mut ctx)?;
```

## Available functions

| Function | Description |
| --- | --- |
| `mode(expression)` | Returns most frequent value. |
| `max_by(value, key)` | Returns value at maximum key. |
| `min_by(value, key)` | Returns value at minimum key. |
| `skewness(expression)` | Computes skewness. |
| `kurtosis_pop(expression)` | Computes population excess kurtosis. |
| `kurtosis(expression)` | Computes sample excess kurtosis. |

```sql
SELECT mode(city) FROM visits;
SELECT max_by(user_id, score) FROM results;
SELECT min_by(user_id, score) FROM results;
SELECT skewness(measurement) FROM readings;
SELECT kurtosis_pop(measurement) FROM readings;
SELECT kurtosis(measurement) FROM readings;
```

## Testing

```sh
cargo test
```
