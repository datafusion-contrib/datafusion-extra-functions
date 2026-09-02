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

//! Runs every `tests/sqllogictest/*.slt` file through DataFusion's
//! sqllogictest engine with the extra functions registered.
//!
//! To regenerate the expected results after a behavior change, run:
//!
//! ```sh
//! COMPLETE=1 cargo test --test sqllogictests
//! ```
//!
//! `COMPLETE` must be a truthy value (`1`, `true`, `yes`, or `on`).
//! `COMPLETE=0` runs the tests without rewriting files.

use std::path::{Path, PathBuf};

use datafusion::execution::context::SessionContext;
use datafusion_extra_functions::register_all_extra_functions;
use datafusion_sqllogictest::{DataFusion, df_value_validator, value_normalizer};
use indicatif::ProgressBar;
use sqllogictest::strict_column_validator;

/// Matches DataFusion's sqllogictest env handling: only `1` / `true` / `yes` /
/// `on` (case-insensitive, surrounding whitespace ignored) enable complete mode.
fn is_env_truthy(name: &str) -> bool {
    std::env::var_os(name)
        .and_then(|value| value.into_string().ok())
        .is_some_and(|value| is_truthy(&value))
}

fn is_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn context_with_extra_functions() -> SessionContext {
    let mut ctx = SessionContext::new();
    register_all_extra_functions(&mut ctx).expect("registering extra functions");
    ctx
}

fn test_files() -> Vec<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/sqllogictest");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("reading tests/sqllogictest")
        .map(|entry| entry.expect("reading dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "slt"))
        .collect();
    files.sort();
    assert!(
        !files.is_empty(),
        "no .slt files found in {}",
        dir.display()
    );
    files
}

#[tokio::test]
async fn sqllogictests() -> Result<(), String> {
    let complete = is_env_truthy("COMPLETE");
    let mut failures = Vec::new();

    for path in test_files() {
        let relative = path
            .strip_prefix(env!("CARGO_MANIFEST_DIR"))
            .expect("test file under manifest dir")
            .to_path_buf();

        // Each file runs against a fresh context so files stay independent.
        let ctx = context_with_extra_functions();
        let make_relative = relative.clone();
        let mut runner = sqllogictest::Runner::new(move || {
            let ctx = ctx.clone();
            let relative = make_relative.clone();
            async move { Ok(DataFusion::new(ctx, relative, ProgressBar::hidden())) }
        });
        runner.with_column_validator(strict_column_validator);
        runner.with_normalizer(value_normalizer);
        runner.with_validator(df_value_validator);

        if complete {
            runner
                .update_test_file(
                    &path,
                    " ",
                    df_value_validator,
                    value_normalizer,
                    strict_column_validator,
                )
                .await
                .map_err(|e| format!("error completing {}: {e}", relative.display()))?;
        } else if let Err(e) = runner.run_file_async(&path).await {
            failures.push(format!("{}: {e}", relative.display()));
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!("sqllogictest failures:\n{}", failures.join("\n")))
    }
}

#[cfg(test)]
mod truthy_env {
    use super::is_truthy;

    #[test]
    fn accepts_truthy_values() {
        for value in ["1", "true", "TRUE", " yes ", "On"] {
            assert!(is_truthy(value), "{value}");
        }
    }

    #[test]
    fn rejects_falsy_values() {
        for value in ["", "0", "false", "no", "off", "COMPLETE"] {
            assert!(!is_truthy(value), "{value}");
        }
    }
}
