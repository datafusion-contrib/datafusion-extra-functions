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

//! PyCapsule bindings for `datafusion-extra-functions`.
//!
//! Exposes each aggregate UDF through datafusion-python's
//! `__datafusion_aggregate_udf__` protocol:
//!
//! ```python
//! from datafusion import udaf
//! import datafusion_extra_functions as extra
//!
//! mode = udaf(extra.udaf_by_name("mode"))
//! df.aggregate([], [mode(col("x"))])
//! ```

use std::sync::Arc;

use datafusion_expr::AggregateUDF;
use datafusion_ffi::udaf::FFI_AggregateUDF;
use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

#[pyclass(name = "ExtraAggregateUDF", module = "datafusion_extra_functions")]
pub struct ExtraAggregateUDF {
    inner: Arc<AggregateUDF>,
}

#[pymethods]
impl ExtraAggregateUDF {
    fn name(&self) -> String {
        self.inner.name().to_string()
    }

    fn __repr__(&self) -> String {
        format!("ExtraAggregateUDF({})", self.inner.name())
    }

    fn __datafusion_aggregate_udf__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let provider = FFI_AggregateUDF::from(Arc::clone(&self.inner));
        PyCapsule::new_with_value(py, provider, cr"datafusion_aggregate_udf")
    }
}

#[pyfunction]
fn list_functions() -> Vec<String> {
    datafusion_extra_functions::all_extra_aggregate_functions()
        .into_iter()
        .map(|f| f.name().to_string())
        .collect()
}

#[pyfunction]
fn udaf_by_name(name: &str) -> PyResult<ExtraAggregateUDF> {
    datafusion_extra_functions::all_extra_aggregate_functions()
        .into_iter()
        .find(|f| f.name() == name)
        .map(|inner| ExtraAggregateUDF { inner })
        .ok_or_else(|| {
            PyKeyError::new_err(format!(
                "No aggregate function named {name:?} in datafusion-extra-functions"
            ))
        })
}

#[pymodule]
#[pyo3(name = "datafusion_extra_functions")]
fn extra_functions_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ExtraAggregateUDF>()?;
    m.add_function(wrap_pyfunction!(list_functions, m)?)?;
    m.add_function(wrap_pyfunction!(udaf_by_name, m)?)?;
    Ok(())
}
