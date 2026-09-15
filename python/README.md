# datafusion-extra-functions (Python)

Python wheels for [`datafusion-extra-functions`](https://github.com/datafusion-contrib/datafusion-extra-functions), exposing each aggregate through datafusion-python's `__datafusion_aggregate_udf__` PyCapsule protocol.

## Usage

```python
from datafusion import SessionContext, col, udaf
import datafusion_extra_functions as extra

extra.list_functions()
# ['mode', 'max_by', 'min_by', 'kurtosis', 'skewness', 'kurtosis_pop']

skew = udaf(extra.udaf_by_name("skewness"))

ctx = SessionContext()
df = ctx.from_pydict({"a": [1.0, 2.0, 2.0, 9.0]})
df.aggregate([], [skew(col("a"))]).show()
```

Or register for SQL:

```python
ctx.register_udaf(udaf(extra.udaf_by_name("mode")))
ctx.sql("SELECT mode(a) FROM my_table").show()
```

## Version compatibility

The DataFusion FFI ABI is tied to the `datafusion` major version this package was compiled against. The wheel pins `datafusion>=55,<56`. A mismatch fails at the FFI boundary rather than with a clean error.

## Building from source

From this `python/` directory, with a Rust toolchain (see `rust-version` in `Cargo.toml`):

```sh
pip install maturin
maturin develop --release
```

## License

Apache-2.0, same as the crate it binds.
