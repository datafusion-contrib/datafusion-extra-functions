use datafusion::logical_expr::AggregateUDFImpl;
use datafusion::{arrow, common, error, functions_aggregate, logical_expr};
use std::ops::Deref;
use std::{any, fmt};

make_udaf_expr_and_func!(
    MaxByFunction,
    max_by,
    x y,
    "Returns the value of the first column corresponding to the maximum value in the second column.",
    max_by_udaf
);

#[derive(Eq, Hash, PartialEq)]
pub struct MaxByFunction {
    null_first: bool,
    signature: logical_expr::Signature,
}

impl fmt::Debug for MaxByFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("MaxBy")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}
impl Default for MaxByFunction {
    fn default() -> Self {
        Self::new(true)
    }
}

impl MaxByFunction {
    pub fn new(null_first: bool) -> Self {
        Self {
            null_first,
            signature: logical_expr::Signature::user_defined(logical_expr::Volatility::Immutable),
        }
    }
}

fn get_min_max_by_result_type(
    input_types: &[arrow::datatypes::DataType],
) -> error::Result<Vec<arrow::datatypes::DataType>> {
    match &input_types[0] {
        arrow::datatypes::DataType::Dictionary(_, dict_value_type) => {
            // x add checker, if the value type is complex data type
            let mut result = vec![dict_value_type.deref().clone()];
            // Preserve all other argument types
            result.extend_from_slice(&input_types[1..]);
            Ok(result)
        }
        _ => Ok(input_types.to_vec()),
    }
}

impl logical_expr::AggregateUDFImpl for MaxByFunction {
    fn as_any(&self) -> &dyn any::Any {
        self
    }

    fn name(&self) -> &str {
        "max_by"
    }

    fn signature(&self) -> &logical_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> error::Result<arrow::datatypes::DataType> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(
        &self,
        _acc_args: logical_expr::function::AccumulatorArgs,
    ) -> error::Result<Box<dyn logical_expr::Accumulator>> {
        common::exec_err!("should not reach here")
    }

    fn coerce_types(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> error::Result<Vec<arrow::datatypes::DataType>> {
        get_min_max_by_result_type(arg_types)
    }

    fn simplify(&self) -> Option<logical_expr::function::AggregateFunctionSimplification> {
        let null_first = self.null_first;
        let simplify = move |mut aggr_func: logical_expr::expr::AggregateFunction,
                             _: &dyn logical_expr::simplify::SimplifyInfo| {
            let mut order_by = aggr_func.params.order_by;
            let (second_arg, first_arg) = (
                aggr_func.params.args.remove(1),
                aggr_func.params.args.remove(0),
            );
            let sort = logical_expr::expr::Sort::new(second_arg, true, null_first);
            order_by.push(sort);
            let func = logical_expr::expr::AggregateFunction::new_udf(
                functions_aggregate::first_last::last_value_udaf(),
                vec![first_arg],
                aggr_func.params.distinct,
                aggr_func.params.filter,
                order_by,
                aggr_func.params.null_treatment,
            );
            let func = logical_expr::expr::Expr::AggregateFunction(func);
            Ok(func)
        };
        Some(Box::new(simplify))
    }
}

make_udaf_expr_and_func!(
    MinByFunction,
    min_by,
    x y,
    "Returns the value of the first column corresponding to the minimum value in the second column.",
    min_by_udaf
);

#[derive(Eq, Hash, PartialEq)]
pub struct MinByFunction {
    null_first: bool,
    signature: logical_expr::Signature,
}

impl fmt::Debug for MinByFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("MinBy")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}

impl Default for MinByFunction {
    fn default() -> Self {
        Self::new(true)
    }
}

impl MinByFunction {
    pub fn new(null_first: bool) -> Self {
        Self {
            null_first,
            signature: logical_expr::Signature::user_defined(logical_expr::Volatility::Immutable),
        }
    }
}

impl logical_expr::AggregateUDFImpl for MinByFunction {
    fn as_any(&self) -> &dyn any::Any {
        self
    }

    fn name(&self) -> &str {
        "min_by"
    }

    fn signature(&self) -> &logical_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> error::Result<arrow::datatypes::DataType> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(
        &self,
        _acc_args: logical_expr::function::AccumulatorArgs,
    ) -> error::Result<Box<dyn logical_expr::Accumulator>> {
        common::exec_err!("should not reach here")
    }

    fn coerce_types(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> error::Result<Vec<arrow::datatypes::DataType>> {
        get_min_max_by_result_type(arg_types)
    }

    fn simplify(&self) -> Option<logical_expr::function::AggregateFunctionSimplification> {
        let null_first = self.null_first;
        let simplify = move |mut aggr_func: logical_expr::expr::AggregateFunction,
                             _: &dyn logical_expr::simplify::SimplifyInfo| {
            let mut order_by = aggr_func.params.order_by;
            let (second_arg, first_arg) = (
                aggr_func.params.args.remove(1),
                aggr_func.params.args.remove(0),
            );

            let sort = logical_expr::expr::Sort::new(second_arg, false, null_first);
            order_by.push(sort); // false for ascending sort
            let func = logical_expr::expr::AggregateFunction::new_udf(
                functions_aggregate::first_last::last_value_udaf(),
                vec![first_arg],
                aggr_func.params.distinct,
                aggr_func.params.filter,
                order_by,
                aggr_func.params.null_treatment,
            );
            let func = logical_expr::expr::Expr::AggregateFunction(func);
            Ok(func)
        };
        Some(Box::new(simplify))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::ArrayAccessor;
    use datafusion::{arrow, datasource, error, prelude};
    use std::sync;

    const TEST_TABLE_NAME: &str = "types";
    const STRING_COLUMN_NAME: &str = "string";
    const DICTIONARY_COLUMN_NAME: &str = "dict_string";
    const INT64_COLUMN_NAME: &str = "int64";
    const FLOAT64_COLUMN_NAME: &str = "float64";

    const MIN_STRING_VALUE: &str = "a";
    const MID_STRING_VALUE: &str = "b";
    const MAX_STRING_VALUE: &str = "c";
    const MIN_FLOAT_VALUE: f64 = 0.25;
    const MID_FLOAT_VALUE: f64 = 0.5;
    const MAX_FLOAT_VALUE: f64 = 0.75;
    const MIN_INT_VALUE: i64 = -1;
    const MID_INT_VALUE: i64 = 0;
    const MAX_INT_VALUE: i64 = 1;
    const MIN_DICTIONARY_VALUE: &str = "a";
    const MID_DICTIONARY_VALUE: &str = "b";
    const MAX_DICTIONARY_VALUE: &str = "c";

    fn test_schema() -> sync::Arc<arrow::datatypes::Schema> {
        sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(
                STRING_COLUMN_NAME,
                arrow::datatypes::DataType::Utf8,
                false,
            ),
            arrow::datatypes::Field::new_dictionary(
                DICTIONARY_COLUMN_NAME,
                arrow::datatypes::DataType::Int32,
                arrow::datatypes::DataType::Utf8,
                false,
            ),
            arrow::datatypes::Field::new(
                INT64_COLUMN_NAME,
                arrow::datatypes::DataType::Int64,
                false,
            ),
            arrow::datatypes::Field::new(
                FLOAT64_COLUMN_NAME,
                arrow::datatypes::DataType::Float64,
                false,
            ),
        ]))
    }

    fn test_data(
        schema: sync::Arc<arrow::datatypes::Schema>,
    ) -> Vec<arrow::record_batch::RecordBatch> {
        vec![
            arrow::record_batch::RecordBatch::try_new(
                schema,
                vec![
                    sync::Arc::new(arrow::array::StringArray::from(vec![
                        MID_STRING_VALUE,
                        MIN_STRING_VALUE,
                        MAX_STRING_VALUE,
                    ])),
                    sync::Arc::new(
                        vec![
                            Some(MID_DICTIONARY_VALUE),
                            Some(MIN_DICTIONARY_VALUE),
                            Some(MAX_DICTIONARY_VALUE),
                        ]
                        .into_iter()
                        .collect::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>(),
                    ),
                    sync::Arc::new(arrow::array::Int64Array::from(vec![
                        MID_INT_VALUE,
                        MIN_INT_VALUE,
                        MAX_INT_VALUE,
                    ])),
                    sync::Arc::new(arrow::array::Float64Array::from(vec![
                        MID_FLOAT_VALUE,
                        MIN_FLOAT_VALUE,
                        MAX_FLOAT_VALUE,
                    ])),
                ],
            )
            .unwrap(),
        ]
    }

    fn test_ctx() -> datafusion::common::Result<prelude::SessionContext> {
        let schema = test_schema();
        let data = test_data(schema.clone());
        let table = datasource::MemTable::try_new(schema, vec![data])?;
        let ctx = prelude::SessionContext::new();
        ctx.register_table(TEST_TABLE_NAME, sync::Arc::new(table))?;
        Ok(ctx)
    }

    async fn extract_single_value<T, A>(df: prelude::DataFrame) -> error::Result<T>
    where
        A: arrow::array::Array + 'static,
        for<'a> &'a A: arrow::array::ArrayAccessor,
        for<'a> <&'a A as arrow::array::ArrayAccessor>::Item: Into<T>,
    {
        let results = df.collect().await?;
        let col = results[0].column(0);
        let v1 = col.as_any().downcast_ref::<A>().unwrap();
        let value = v1.value(0).into();
        Ok(value)
    }

    #[cfg(test)]
    mod max_by {

        use super::*;

        #[tokio::test]
        async fn test_max_by_string_int() -> error::Result<()> {
            let query = format!(
                "SELECT max_by({}, {}) FROM {}",
                STRING_COLUMN_NAME, INT64_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<String, arrow::array::StringArray>(df).await?;
            assert_eq!(result, MAX_STRING_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_max_by_string_float() -> error::Result<()> {
            let query = format!(
                "SELECT max_by({}, {}) FROM {}",
                STRING_COLUMN_NAME, FLOAT64_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<String, arrow::array::StringArray>(df).await?;
            assert_eq!(result, MAX_STRING_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_max_by_float_string() -> error::Result<()> {
            let query = format!(
                "SELECT max_by({}, {}) FROM {}",
                FLOAT64_COLUMN_NAME, STRING_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<f64, arrow::array::Float64Array>(df).await?;
            assert_eq!(result, MAX_FLOAT_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_max_by_int_string() -> error::Result<()> {
            let query = format!(
                "SELECT max_by({}, {}) FROM {}",
                INT64_COLUMN_NAME, STRING_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<i64, arrow::array::Int64Array>(df).await?;
            assert_eq!(result, MAX_INT_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_max_by_dictionary_int() -> error::Result<()> {
            let query = format!(
                "SELECT max_by({}, {}) FROM {}",
                DICTIONARY_COLUMN_NAME, INT64_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<String, arrow::array::StringArray>(df).await?;
            assert_eq!(result, MAX_DICTIONARY_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_max_by_ignores_nulls() -> error::Result<()> {
            let query = r#"
                SELECT max_by(v, k)
                FROM (
                    VALUES
                        ('a', 1),
                        ('b', CAST(NULL AS INT)),
                        ('c', 2)
                ) AS t(v, k)
            "#;
            let df = ctx()?.sql(query).await?;
            let result = extract_single_value::<String, arrow::array::StringArray>(df).await?;
            assert_eq!(result, "c", "max_by should ignore NULLs");
            Ok(())
        }

        fn ctx() -> error::Result<prelude::SessionContext> {
            let ctx = test_ctx()?;
            let max_by_udaf = MaxByFunction::default();
            ctx.register_udaf(max_by_udaf.into());
            Ok(ctx)
        }
    }

    #[cfg(test)]
    mod min_by {

        use super::*;

        #[tokio::test]
        async fn test_min_by_string_int() -> error::Result<()> {
            let query = format!(
                "SELECT min_by({}, {}) FROM {}",
                STRING_COLUMN_NAME, INT64_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<String, arrow::array::StringArray>(df).await?;
            assert_eq!(result, MIN_STRING_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_min_by_string_float() -> error::Result<()> {
            let query = format!(
                "SELECT min_by({}, {}) FROM {}",
                STRING_COLUMN_NAME, FLOAT64_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<String, arrow::array::StringArray>(df).await?;
            assert_eq!(result, MIN_STRING_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_min_by_float_string() -> error::Result<()> {
            let query = format!(
                "SELECT min_by({}, {}) FROM {}",
                FLOAT64_COLUMN_NAME, STRING_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<f64, arrow::array::Float64Array>(df).await?;
            assert_eq!(result, MIN_FLOAT_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_min_by_int_string() -> error::Result<()> {
            let query = format!(
                "SELECT min_by({}, {}) FROM {}",
                INT64_COLUMN_NAME, STRING_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<i64, arrow::array::Int64Array>(df).await?;
            assert_eq!(result, MIN_INT_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_min_by_dictionary_int() -> error::Result<()> {
            let query = format!(
                "SELECT min_by({}, {}) FROM {}",
                DICTIONARY_COLUMN_NAME, INT64_COLUMN_NAME, TEST_TABLE_NAME
            );
            let df = ctx()?.sql(&query).await?;
            let result = extract_single_value::<String, arrow::array::StringArray>(df).await?;
            assert_eq!(result, MIN_DICTIONARY_VALUE);
            Ok(())
        }

        #[tokio::test]
        async fn test_min_by_ignores_nulls() -> error::Result<()> {
            let query = r#"
                SELECT min_by(v, k)
                FROM (
                    VALUES
                        ('a', 1),
                        ('b', CAST(NULL AS INT)),
                        ('c', 2)
                ) AS t(v, k)
            "#;
            let df = ctx()?.sql(query).await?;
            let result = extract_single_value::<String, arrow::array::StringArray>(df).await?;
            assert_eq!(result, "a", "min_by should ignore NULLs");
            Ok(())
        }

        fn ctx() -> error::Result<prelude::SessionContext> {
            let ctx = test_ctx()?;
            let min_by_udaf = MinByFunction::default();
            ctx.register_udaf(min_by_udaf.into());
            Ok(ctx)
        }
    }
}
