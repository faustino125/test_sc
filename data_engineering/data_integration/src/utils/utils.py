from functools import reduce
from typing import List, Tuple

from prefect import task
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import coalesce, col, lit, row_number, when


@task(name="join_dataframes_task")
def join_dataframes_task(
    p_base_columns: list, p_dataframes: list, p_how="left"
) -> DataFrame:
    """
    Recursively joins dataframes while respecting the order of p_dataframes

    This task joins each dataframe in order from the position 0 of
    'p_dataframes' to N, using as join reference 'p_base_columns', which need to
    already exist inside each dataframe of 'p_dataframes' in order to work as
    expected.

    Args:
        p_base_columns (list): The columns in which the join will be based.
        p_dataframes (list): The list of dataframes that will be joined.
        p_how (str): The join strategy that will be used, set by default to
        'left'.

    Returns:
        DataFrame: The final joined dataframe in order from position 0
        of p_dataframes to position N.
    """

    if len(p_dataframes) < 3:
        return p_dataframes[0].join(p_dataframes[1], on=p_base_columns, how=p_how)
    else:
        return join_dataframes_task(p_base_columns, p_dataframes[:-1], p_how).join(
            p_dataframes[-1],
            on=p_base_columns,
            how=p_how,
        )


@task(name="add_suffix_to_columns_task")
def add_suffix_to_columns_task(p_data: DataFrame, p_suffix: str, p_exclusions=[]):
    """
    Function that gets all the columns ending with an specified
    `p_suffix` substring.

    Args:
        p_data (DataFrame): DataFrame containing the data to proccess.
        p_suffix (str): Suffix to be added in the `p_data` columns.
        p_exclusions (str): Columns that are not being renamed.

    Returns:
        DataFrame: `p_data` containing renamed columns.
    """
    return p_data.withColumnsRenamed(
        {col: f"{col}{p_suffix}" for col in p_data.columns if col not in p_exclusions}
    )


@task(name="compute_sum_with_fixed_and_variants_task")
def compute_sum_with_fixed_and_variants(
    p_df: DataFrame,
    p_new_col: str,
    p_fixed_substring: str,
    p_variable_substrings: List[str],
) -> DataFrame:
    """
    Adds a new column as the sum of all integer columns whose names contain:
    - a fixed substring (e.g., 'prestamos_cnt')
    - and at least one variable substring (e.g., ['en_mora', 'juridico']).

    Keeps all original columns and appends the new one.
    If 'p_new_col' already exists, it is dropped before being recreated.
    If no matching columns are found, the new column will contain 0.

    Args:
        p_df (DataFrame): Source DataFrame.
        p_new_col (str): Name of the new column to create.
        p_fixed_substring (str): Fixed substring required in column names.
        p_variable_substrings (List[str]): Variable substrings to match.

    Returns:
        DataFrame: DataFrame with the new column added.
    """

    def match(p_name: str) -> bool:
        return (p_fixed_substring in p_name) and any(
            v in p_name for v in p_variable_substrings
        )

    matching_cols = [
        coalesce(col(c).cast("int"), lit(0)) for c in p_df.columns if match(c)
    ]

    if p_new_col in p_df.columns:
        p_df = p_df.drop(p_new_col)

    total_expr = sum(matching_cols, lit(0)).cast("int")

    return p_df.withColumn(p_new_col, total_expr)


@task(name="get_top_by_order_task")
def get_top_by_order(
    p_df: DataFrame,
    p_partition_cols: List[str],
    p_order_cols: List[str],
    p_order_type: str,
) -> DataFrame:
    """
    Returns the top-1 row per partition based on the specified order.

    Args:
        p_df (DataFrame): Input DataFrame.
        p_partition_cols (List[str]): Columns to partition by.
        p_order_cols (List[str]): Columns to order by.
        p_order_type (str): "asc" or "desc" to define the ordering.

    Returns:
        DataFrame: DataFrame with only the top-1 row per partition.
    """
    order_exprs = [
        col(c).asc() if p_order_type == "asc" else col(c).desc() for c in p_order_cols
    ]
    w = Window.partitionBy(*p_partition_cols).orderBy(*order_exprs)

    return (
        p_df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")
    )


@task(name="remove_prefix_from_columns_task")
def remove_prefix_from_columns_task(p_df: DataFrame, p_prefix: str) -> DataFrame:
    """
    Renames all the columns from `p_df` and removes the `p_prefix` prefix.

    Iterates over the columns, slicing its names if the prefix `p_prefix`
    exist at the start of the string of the column name, if the previous,
    condition is not met, the function will leave the column name as it
    is.

    Args:
        p_df (DataFrame): The input DataFrame with a prefix to be removed
        from certain columns.
        p_prefix (DataFrame): The prefix to be removed from `p_df` column
        names.

    Returns:
        DataFrame: The DataFrame with the renamed columns
    """
    renamed_cols = {
        col: col[len(p_prefix) :] for col in p_df.columns if col.startswith(p_prefix)
    }

    return p_df.select(
        [p_df[col].alias(renamed_cols.get(col, col)) for col in p_df.columns]
    )


@task(name="remove_suffix_from_columns_task")
def remove_suffix_from_columns_task(p_df: DataFrame, p_suffix: str) -> DataFrame:
    """
    Renames all the columns from `p_df` and removes the `p_suffix` suffix.

    Iterates over the columns, slicing its names if the suffix `p_suffix`
    exist at the end of the string of the column name, if the previous,
    condition is not met, the function will leave the column name as it
    is.

    Args:
        p_df (DataFrame): The input DataFrame with a suffix to be removed
        from certain columns.
        p_suffix (DataFrame): The suffix to be removed from `p_df` column
        names.

    Returns:
        DataFrame: The DataFrame with the renamed columns
    """
    renamed_cols = {
        col: col[: -len(p_suffix)] for col in p_df.columns if col.endswith(p_suffix)
    }

    return p_df.select(
        [p_df[col].alias(renamed_cols.get(col, col)) for col in p_df.columns]
    )


@task(name="fill_na_task")
def fill_na(p_df: DataFrame) -> DataFrame:
    """
    Replaces null values in numeric columns with zero.

    This task scans the input DataFrame and identifies columns with numeric
    data types ('int', 'bigint', 'float', 'double','decimal'). All null values in these
    columns are replaced with '0'.

    Args:
        p_df (DataFrame): The input DataFrame containing potential null values.

    Returns:
        DataFrame: The DataFrame with null values in numeric columns replaced
        by zero.
    """
    numeric_cols = [
        c
        for c, t in p_df.dtypes
        if t in ("int", "bigint", "float", "double", "decimal")
    ]
    p_df = p_df.fillna(0, subset=numeric_cols)
    return p_df


@task(name="sum_features_across_dataframes_task")
def sum_features_across_dataframes_task(
    p_features_to_sum: List[Tuple[DataFrame, str]],
    p_join_keys: List[str],
    p_result_col: str,
    p_how: str = "full",
    p_replace_negative: bool = False,
) -> DataFrame:
    """
    Joins multiple DataFrames and sums the specified feature columns
    into a single new column.

    For each (DataFrame, column_name) pair in 'p_features_to_sum', the
    function performs a join between all DataFrames using 'p_join_keys',
    then sums all the feature columns using coalesce to treat nulls as zero.
    If all feature columns are null for a given row, the result will be null.

    Args:
        p_features_to_sum (List[Tuple[DataFrame, str]]): List of tuples where
            each tuple contains a DataFrame and the name of the column to sum.
        p_join_keys (List[str]): Column names used as join keys
            (e.g. ["id_cliente", "_observ_end_dt"]).
        p_result_col (str): Name of the resulting column containing the sum.
        p_how (str): Join strategy. Defaults to "full" to preserve all clients
            across all DataFrames regardless of which sources they appear in.
        p_replace_negative (bool): If True, negative values in the result
            column are replaced with 0. Defaults to False.

    Returns:
        DataFrame: Joined DataFrame with a new column 'p_result_col'
        containing the total sum of all feature columns. If all feature
        columns are null, the result will be null.

    """
    result_df, _ = p_features_to_sum[0]

    for df, col_name in p_features_to_sum[1:]:
        result_df = result_df.join(
            df.select(*p_join_keys, col_name),
            on=p_join_keys,
            how=p_how,
        )

    sum_expr = sum(coalesce(col(col_name), lit(0)) for _, col_name in p_features_to_sum)

    all_null_condition = reduce(
        lambda a, b: a & b,
        [col(col_name).isNull() for _, col_name in p_features_to_sum],
    )

    if p_result_col in result_df.columns:
        result_df = result_df.drop(p_result_col)

    result_df = result_df.withColumn(p_result_col, sum_expr).withColumn(
        p_result_col,
        when(all_null_condition, lit(None)).otherwise(col(p_result_col)),
    )

    if p_replace_negative:
        result_df = result_df.withColumn(
            p_result_col,
            when(col(p_result_col) < 0, lit(0)).otherwise(col(p_result_col)),
        )

    return result_df.select(*p_join_keys, p_result_col)
