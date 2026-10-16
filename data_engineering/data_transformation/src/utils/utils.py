from typing import Any

from prefect import task
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f


@task(name="join_dataframes_task")
def join_dataframes_task(
    p_base_columns: list, p_dataframes: list, p_how="left"
) -> DataFrame:
    """
    Recursively joins dataframes while respecting the order of p_dataframes

    This task joins each dataframe in order from the position 0 of
    `p_dataframes` to N, using as join reference `p_base_columns`, which need to
    already exist inside each dataframe of `p_dataframes` in order to work as
    expected.

    Args:
        p_base_columns (list): The columns in which the join will be based.
        p_dataframes (list): The list of dataframes that will be joined.
        p_how (str): The joi strategy that will be used, set by default to
        `left`.

    Returns:
        DataFrame: The final joined dataframe in order from position 0
        of p_dataframes to position N.
    """

    if len(p_dataframes) < 3:
        return p_dataframes[0].join(p_dataframes[1], on=p_base_columns, how=p_how)
    else:
        return join_dataframes_task(p_base_columns, p_dataframes[:-1]).join(
            p_dataframes[-1],
            on=p_base_columns,
            how=p_how,
        )


@task(name="split_currency_task")
def split_currency_task(
    p_value_col: str, p_currency_col: str, p_using_description: str = False
) -> list:
    """
    Generates columns for GTQ and USD currencies in a given DataFrame.

    This task process the value of `p_value_col` to split it into two
    columns with suffix _usd or _gtq based on the currency id given by
    `p_currency_col`.

    Args:
        p_value_col (DataFrame): Name of the column we want to split.
        p_currency_col (DataFrame): Name of the column that has the currency
        id in it
    Returns:
        list: List of the splitted columns with its corresponding suffixes.
    """

    currency_map = (
        {"gtq": "_gtq", "usd": "_usd"}
        if p_using_description
        else {1: "_gtq", 2: "_usd"}
    )

    return [
        f.when(f.col(p_currency_col) == key, f.col(p_value_col))
        .otherwise(0)
        .cast("float")
        .alias(f"{p_value_col}{suffix}")
        for key, suffix in currency_map.items()
    ]


@task(name="get_min_max_date_task")
def get_min_max_date_task(
    p_date_data: DataFrame,
    p_date_field: str,
    p_date_alias: str,
    p_operation="min",
    p_group_columns=["id_cliente", "_observ_end_dt"],
) -> DataFrame:
    """
    Function that gets the first or last date of a groupby dataframe.

    Args:
        p_date_data (DataFrame): DataFrame containing the data to proccess..
        p_date_field (str): Name of field date in dataframe.
        p_date_alias (str): New field alias for the calculated column.
        p_operation (str): Optional parameter for getting min or max date, it is set to
        `min`.
        by default but it can be changed to `max`.
        p_group_columns (list): Optional parameter for the column names that will be
        used as base for the aggregation, if not changed, it will do the group by
        clause based on `id_cliente` and `_observ_end_dt`.

    Returns:
        DataFrame: Field with first or last date of a dataframe.
    """

    VALID_OPERATIONS = {"min": f.min, "max": f.max}
    if p_operation not in VALID_OPERATIONS:
        raise ValueError(
            f"Invalid operation. Expected one of {list(VALID_OPERATIONS.keys())}"
        )

    p_date_data = p_date_data.groupBy([f.col(col) for col in p_group_columns]).agg(
        VALID_OPERATIONS[p_operation](f.col(p_date_field)).alias(p_date_alias)
    )

    return p_date_data


@task(name="suffix_columns_task")
def suffix_columns_task(p_data: DataFrame, p_suffix: str):
    """
    Function that gets all the columns ending with an specified
    `p_suffix` substring.

    Args:
        p_data (DataFrame): DataFrame containing the data to proccess.
        p_suffix (str): Suffix to be searched in the `p_data` columns.

    Returns:
        list: List containing dataframe columns that match the specified suffix.
    """
    return [f.col(c) for c in p_data.columns if c.endswith(p_suffix)]


@task(name="unified_currency_col_task")
def unified_currency_col_task(
    p_currency_id: str, p_transaction_amount: str, p_exchange_rate: str
) -> Column:
    """
    Computes a unified amount in GTQ based on the USD currency exchange rate.

    Parameters:
    p_currency_id (str): Column name that represents the currency ID.
    p_transaction_amount (str): Column name that represents the transaction amount.
    p_exchange_rate (str): Column name that represents the exchange rate.

    Returns:
    Column: A new column with the computed unified amount.
    """
    return f.when(
        (f.col(p_currency_id) == 1) | (f.col(p_currency_id) == "gtq"),
        f.col(p_transaction_amount),
    ).otherwise(f.col(p_transaction_amount) * f.col(p_exchange_rate))


@task(name="add_exchange_rate_task")
def add_exchange_rate_task(
    p_base_data: DataFrame, p_exchange_rate: DataFrame, p_snapshot=False
) -> DataFrame:
    """
    Adds the exchange rate information to a dataframe based on the `p_snapshot`
    to detect whether the join clause needs to be done based on `_observ_end_dt`
    or `fecha_transaccion`.

    Parameters:
    p_base_data (DataFrame): Column name that represents the currency ID.
    p_exchange_rate (DataFrame): Dataframe containing the daily exchange rate.
    p_snapshot (bool): Flag set by default to `False` that decides the join strategy.

    Returns:
    DataFrame: `p_base_data` Dataframe with the daily exchange rate aadditioned inside
    the `tasa_cambio` column.
    """

    df_result = p_base_data.join(
        p_exchange_rate.drop("_observ_end_dt"),
        on=(
            p_base_data._observ_end_dt == p_exchange_rate.fecha_transaccion
            if p_snapshot
            else "fecha_transaccion"
        ),
        how="left",
    )

    return df_result.drop("fecha_transaccion") if p_snapshot else df_result


@task(name="sum_cond_task", tags=["data transformation", "task", "helper"])
def sum_cond_task(p_cond: Column, p_col: Any) -> Column:
    """
    Generalization of an abstract condition where a `sum` aggregation
    when a `when` clause is needed.

    Parameters:
    p_cond (Column): Conditional clause that will delimit the `sum` clause
    p_col (Any): Column value that will be added to the result if the conditional
    clause is True.

    Returns:
    Column: A new column with the computed conditional sum aggregation.
    """
    return f.sum(f.when(p_cond, p_col).otherwise(0))


def create_currency_col_task(
    p_currency_col: str,
    p_col_name: str,
    p_exchange_rate: str,
    p_using_description: bool = False,
) -> list:
    """
    Creates a list of columns by splitting the currency column.

    Args:
        p_currency_col (str): The name of the column containing currency information.
        p_col_name (str): The name of the column containing amounts.
        p_exchange_rate (str): The name of the column containing exchange rates.
        p_using_description (bool, optional): Whether to use the description.

    Returns:
        list: A list of columns resulting from the task.
    """
    split_columns = split_currency_task(p_col_name, p_currency_col, p_using_description)
    unified_column = unified_currency_col_task(
        p_currency_col, p_col_name, p_exchange_rate
    ).alias(f"{p_col_name}_quetzalizado")
    result = split_columns + [unified_column]

    return result


@task(
    name="convert_currency_to_gtq_task", tags=["data transformation", "task", "helper"]
)
def convert_currency_to_gtq_task(
    col_name: str, exchange_col_name: str, col_cast: str = "float"
) -> DataFrame:
    """
    Convert a column in USD to GTQ.

    Parameters:
        col_name (str): Other currency column name.
        exchange_col_name (str): Column with exchange rate
        col_cast  (str): Type to cast the column.

    Returns:
        DataFrame: dataframe with new flag column.
    """
    single_currency_col = (f.col(col_name) * f.col(exchange_col_name)).cast(col_cast)

    return single_currency_col


@task(name="fillna_flags_task")
def fillna_flags_task(df: DataFrame, flag_columns: list = None) -> DataFrame:
    """
    Fill null values with 0 for specified flag columns or all columns
    ending with '_flag'.

    Args:
        df (DataFrame): DataFrame to process.
        flag_columns (list, optional): List of flag column names. If None,
        all columns ending with '_flag' are used.

    Returns:
        DataFrame: DataFrame with nulls in flag columns replaced by 0.
    """
    if flag_columns is None:
        flag_columns = [c for c in df.columns if c.endswith("_flag")]
    return df.fillna(0, subset=flag_columns)
