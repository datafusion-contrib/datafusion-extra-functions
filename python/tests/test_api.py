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


def test_list_functions() -> None:
    names = extra.list_functions()
    assert {
        "mode",
        "skewness",
        "kurtosis",
        "kurtosis_pop",
        "max_by",
        "min_by",
    } <= set(names)


def test_udaf_by_name_unknown() -> None:
    with pytest.raises(KeyError, match="no_such_function"):
        extra.udaf_by_name("no_such_function")


def test_repr_and_name() -> None:
    fn = extra.udaf_by_name("mode")
    assert fn.name() == "mode"
    assert repr(fn) == "ExtraAggregateUDF(mode)"
