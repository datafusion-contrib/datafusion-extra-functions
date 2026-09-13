# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import datafusion_extra_functions as extra
import pytest

datafusion = pytest.importorskip("datafusion")

from datafusion import SessionContext, col, udaf

# Wheels are compiled against DataFusion 55; an older datafusion-python
# major is an ABI mismatch and must not be exercised.
_major = int(datafusion.__version__.split(".", 1)[0])
if _major != 55:
    pytest.skip(
        f"datafusion-python {_major} cannot load DataFusion 55 FFI capsules",
        allow_module_level=True,
    )


def test_mode_aggregate() -> None:
    ctx = SessionContext()
    df = ctx.from_pydict({"a": [1, 2, 2, 3]})
    mode = udaf(extra.udaf_by_name("mode"))
    result = df.aggregate([], [mode(col("a")).alias("m")]).to_pydict()
    assert result["m"] == [2]


def test_skewness_aggregate() -> None:
    ctx = SessionContext()
    df = ctx.from_pydict({"a": [1.0, 2.0, 2.0, 9.0]})
    skew = udaf(extra.udaf_by_name("skewness"))
    result = df.aggregate([], [skew(col("a")).alias("s")]).to_pydict()
    # bias-corrected sample skewness: g1 * sqrt(n*(n-1)) / (n-2)
    assert result["s"][0] == pytest.approx(1.9001038154942962, rel=1e-9)


def test_kurtosis_aggregates() -> None:
    ctx = SessionContext()
    df = ctx.from_pydict({"a": [1.0, 2.0, 3.0, 4.0]})
    exprs = [
        udaf(extra.udaf_by_name(name))(col("a")).alias(name)
        for name in ("kurtosis", "kurtosis_pop")
    ]
    result = df.aggregate([], exprs).to_pydict()
    assert result["kurtosis"][0] is not None
    assert result["kurtosis_pop"][0] is not None


def test_max_min_by_aggregate() -> None:
    ctx = SessionContext()
    df = ctx.from_pydict({"value": [10, 20, 30], "key": [2, 3, 1]})
    max_by = udaf(extra.udaf_by_name("max_by"))
    min_by = udaf(extra.udaf_by_name("min_by"))
    result = df.aggregate(
        [],
        [
            max_by(col("value"), col("key")).alias("max_value"),
            min_by(col("value"), col("key")).alias("min_value"),
        ],
    ).to_pydict()
    assert result["max_value"] == [20]
    assert result["min_value"] == [30]


def test_sql_registration() -> None:
    ctx = SessionContext()
    ctx.from_pydict({"a": [1, 2, 2, 3]}, name="t")
    ctx.register_udaf(udaf(extra.udaf_by_name("mode")))
    result = ctx.sql("SELECT mode(a) AS m FROM t").to_pydict()
    assert result["m"] == [2]
