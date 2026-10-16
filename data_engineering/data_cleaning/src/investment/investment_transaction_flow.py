from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    convert_to_hex_task,
    replace_null_or_empty_values,
)


@task(name="investment_transaction_task", tags=["investment transactions"])
def investment_transaction_task(
    p_investment: DataFrame, p_type_investment: str
) -> DataFrame:
    """
    Prepares the funds DataFrame by renaming columns, selecting specific columns.

    Args:
        p_investment (DataFrame): The input DataFrame containing raw investment data.
        p_type_investment (str): The type of investment.

    Returns:
        DataFrame: A DataFrame with renamed columns, selected fields.
    """
    if p_type_investment == "fixed_term":
        dict_transaction = {
            "deposito": [1],
            "nota_credito": [2],
            "retiro": [3],
            "nota_debito": [4],
        }
    else:
        dict_transaction = {
            "nota_credito": [1],
            "nota_debito": [2],
        }

    std_transaction_description_udf = f.udf(
        lambda code: next(
            (key for key, val in dict_transaction.items() if code in val), None
        ),
        StringType(),
    )

    df_investment = p_investment.select(
        convert_to_hex_task(f.col("DW_CODIGO_CLIENTE"), "cliente"),
        convert_to_hex_task(f.col("DW_CUENTA_CORPORATIVA"), "cuenta"),
        f.trim(f.col("DW_CODIGO_EMPRESA").cast("int")).alias("id_empresa"),
        f.lower(f.trim(f.col("DW_EMPRESA_DESCRIPCION").cast("string"))).alias(
            "descripcion_empresa"
        ),
        f.trim(f.col("DW_CODIGO_MONEDA").cast("int")).alias("id_moneda"),
        f.lower(f.trim(f.col("DW_MONEDA_DESCRIPCION").cast("string"))).alias(
            "descripcion_moneda"
        ),
        f.trim(f.col("DW_TIPO_TRANSACCION").cast("int")).alias("id_transaccion"),
        f.lower(
            f.regexp_replace(
                f.trim(f.col("DW_TIPO_TRANSACCION_DESCRIPCION")), "\\s+", " "
            )
        ).alias("descripcion_transaccion"),
        std_transaction_description_udf(f.col("DW_TIPO_TRANSACCION").cast("int")).alias(
            "descripcion_operacion"
        ),
        f.col("DW_MONTO").cast("double").alias("monto"),
        f.trim(f.col("DW_SECUENCIA").cast("int")).alias("id_secuencia"),
        f.trim(f.col("DW_NUMERO_LEGAJO").cast("int")).alias("id_legajo"),
        f.col("fecha_transaccion"),
    )
    return df_investment


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_transaccion", "id_cliente", "cuenta_corporativa"])
@task(
    name="clean_investment_transactions_task",
)
def clean_investment_transactions_task(
    p_raw_fixed_term_transactions,
    p_raw_scheduled_future_transactions,
    p_raw_golden_investment_transactions,
):
    """
    Cleans and arranges columns for raw investment transactions data.

    Args:
        p_raw_fixed_term_transactions (DataFrame): Raw fixed-term.
        p_raw_scheduled_future_transactions (DataFrame): Raw scheduled future.
        p_raw_golden_investment_transactions (DataFrame): Raw golden investment.

    Returns:
        DataFrame: A cleaned and arranged DataFrame containing merged investment.

    Steps:
        1. Processes fixed-term transactions using `investment_transaction_task`.
        2. Processes scheduled future transactions using `investment_transaction_task`.
        3. Processes golden investment transactions using `investment_transaction_task`.
        4. Merges the processed DataFrames into a single DataFrame
    """
    df_fixed_term = investment_transaction_task(
        p_raw_fixed_term_transactions, "fixed_term"
    )
    df_scheduled_future = investment_transaction_task(
        p_raw_scheduled_future_transactions, "scheduled_future"
    )
    df_gold_plan = investment_transaction_task(
        p_raw_golden_investment_transactions, "golden_investment"
    )
    df_investment_transaction = df_fixed_term.union(df_scheduled_future).union(
        df_gold_plan
    )

    return df_investment_transaction


@flow(name="investment_transaction_flow")
def investment_transaction_flow():
    """
    Loads, processes, and saves data in datalake.
    The flow performs the following operations:
    1. Loads raw data using the specified date range.
    2. Cleans and processes the all data.
    3. Saves the processed data to the appropriate environment using the specified
    overwrite strategy.
    Returns:
        None: This flow does not return a value, but it saves the processed data.
    """
    raw_data = load_raw_data_flow()
    df_fixed_term_transactions = raw_data["raw_fixed_term_transactions"]
    df_scheduled_future_transactions = raw_data["raw_scheduled_future_transactions"]
    df_golden_investment_transactions = raw_data["raw_golden_investment_transactions"]
    df_investment_transactions_final = clean_investment_transactions_task(
        df_fixed_term_transactions,
        df_scheduled_future_transactions,
        df_golden_investment_transactions,
    )
    save_data_flow(df_investment_transactions_final)
