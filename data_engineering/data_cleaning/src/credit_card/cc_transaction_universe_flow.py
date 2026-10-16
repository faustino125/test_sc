from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    convert_to_hex_task,
)


@arrange_columns(p_start_cols=["fecha_transaccion", "id_cliente", "cuenta_corporativa"])
@task(
    name="clean_cc_transaction_universe_task", tags=["data cleaning", "preprocessing"]
)
def clean_cc_transaction_universe_task(p_raw_transactions: DataFrame) -> DataFrame:
    """
    Cleans and processes credit card transaction universe data.

    This task processes the raw credit card transaction universe data by
    applying transformations and cleaning steps.
    It standardizes certain fields, creates new calculated columns,
    and fills empty values with
    default values.

    Args:
        p_raw_transactions (DataFrame): Raw credit card transaction universe data.

    Returns:
        DataFrame: Processed credit card transaction universe DataFrame.
    """
    df_final = p_raw_transactions.select(
        f.col("fecha_transaccion"),
        convert_to_hex_task(f.col("cuenta_corporativa"), "cuenta"),
        f.col("Codigo_Moneda").cast("int").alias("id_moneda"),
        f.trim("DW_CODIGO_MONEDA_DESCRIPCION").alias("descripcion_moneda"),
        f.col("Valor_Dos").cast("float").alias("monto_transaccion2"),
        f.col("codigo_transaccion").cast("int").alias("id_tipo_transaccion"),
        f.trim(f.col("DW_CODIGO_TRANSACCION_DESCRIPCION")).alias(
            "descripcion_tipo_transaccion"
        ),
        f.substring(f.col("DW_CODIGO_TRANSACCION_DESCRIPCION"), 1, 1).alias(
            "operador_tipo_transaccion"
        ),
    )

    return df_final


@flow(name="cc_transaction_universe_flow")
def cc_transaction_universe_flow():
    """
    Load, process, and save credit card transaction universe data in the data lake.

    This flow performs the following operations:
    1. Loads raw credit card transaction universe data using the specified date range.
    2. Cleans and processes the credit card transaction universe data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed credit
        card transaction universe data.
    """
    raw_data = load_raw_data_flow()

    df_cc_transactions = raw_data["raw_cc_transaction_universe"]

    df_cc_transactions = clean_cc_transaction_universe_task(df_cc_transactions)

    save_data_flow(df_cc_transactions)
