from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    convert_to_hex_task,
    replace_null_or_empty_values,
)


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_transaccion", "id_cliente", "cuenta_corporativa"])
@task(
    name="clean_saving_account_transaction_task",
    tags=["data cleaning", "preprocessing", "saving account trx"],
)
def clean_saving_account_transaction_task(
    p_raw_saving_account_transaction: DataFrame,
) -> DataFrame:
    """
    Cleans and processes saving account transaction raw data.

    This task processes the saving accounts transaction raw data,
    by applying transformations and cleaning steps.
    It standardizes certain fields, creates new calculated columns, and fills
    empty values with default values.

    Args:
        p_raw_saving_account_transaction (DataFrame):
        saving account transaction raw data.

    Returns:
        DataFrame: Processed saving account transactions DataFrame.
    """
    df_saving_account_trx = p_raw_saving_account_transaction.select(
        convert_to_hex_task(f.col("CODIGO_CLIENTE"), "cliente"),
        convert_to_hex_task(f.col("Cuenta_Corporativa"), "cuenta"),
        f.lower(f.trim("TIPO_TRANSACCION_DESCRIPCION")).alias(
            "descripcion_tipo_transaccion"
        ),
        f.lower(f.trim("Descripcion_Legajo")).alias("descripcion_legajo"),
        f.trim("NUM_LEGAJO").cast("int").alias("id_legajo"),
        f.col("DW_MONEDA_CODIGO").alias("id_moneda"),
        f.trim(f.upper("DW_MONEDA_DESCRIPCION")).alias("descripcion_moneda"),
        f.round("dw_valor_transaccion", 2).cast("float").alias("monto_transaccion"),
        f.lower(
            f.regexp_replace(
                f.split(f.trim(f.col("DW_Tipo_Transaccion_descripcion")), "[+-]")[1],
                " ",
                "_",
            )
        ).alias("descripcion_transaccion"),
        f.col("fecha_transaccion"),
    )

    return df_saving_account_trx


@flow(name="saving_account_transaction_flow")
def saving_account_transaction_flow():
    """
    Load, process, and save saving account transaction data in the data lake.

    The flow performs the following operations:
    1. Loads saving account transaction raw data using the specified date range.
    2. Cleans and processes the saving account transaction data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        saving account transaction data.
    """
    raw_data = load_raw_data_flow()

    df_saving_account_trx = raw_data["raw_saving_account_transaction"]

    df_saving_account_trx_final = clean_saving_account_transaction_task(
        df_saving_account_trx
    )

    save_data_flow(df_saving_account_trx_final)
