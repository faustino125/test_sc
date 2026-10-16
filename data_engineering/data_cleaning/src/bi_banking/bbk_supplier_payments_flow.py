from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    clean_currency_task,
    convert_to_hex_task,
    replace_null_or_empty_values,
)


@replace_null_or_empty_values()
@arrange_columns(
    p_start_cols=["fecha_transaccion", "id_cliente_origen", "cuenta_origen"]
)
@task(name="clean_supplier_payments_task", tags=["data cleaning", "Preprocesing"])
def clean_supplier_payments_task(p_raw_bbk_supplier_trx: DataFrame) -> DataFrame:
    """Clean Data From BiBanking supplier

    Args:
        p_raw_bbk_supplier_trx (DataFrame): Raw data Supplier Transactions

    Returns:
        DataFrame: Processed BiBanking supplier transactions
    """
    df_bbk_supplier = p_raw_bbk_supplier_trx.select(
        convert_to_hex_task(f.col("CODIGO_CLIENTE_ORIGEN"), "id_cliente_origen"),
        convert_to_hex_task(f.col("CUENTA_ORIGEN"), "cuenta_origen"),
        convert_to_hex_task(f.col("CODIGO_CLIENTE"), "id_cliente_destino"),
        convert_to_hex_task(f.col("CUENTA_DESTINO"), "cuenta_destino"),
        clean_currency_task(f.col("MONEDA")).alias("descripcion_moneda"),
        f.col("DESCRIP_OPERACION").alias("descripcion_operacion"),
        f.col("MONTO_CREDITO").cast("float").alias("monto_credito"),
        f.when((f.col("MONTO_CREDITO")) > 0, f.lit(1))
        .otherwise(0)
        .cast("int")
        .alias("monto_credito_proveedor_flag"),
        f.col("fecha_transaccion"),
    ).drop_duplicates()

    return df_bbk_supplier


@flow(name="bbk_supplier_payments_flow")
def bbk_supplier_payments_flow():
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

    df_raw_bbk_supplier = raw_data["raw_bbk_supplier_payments"]

    df_bbk_supplier_transaction_final = clean_supplier_payments_task(
        df_raw_bbk_supplier,
    )

    save_data_flow(df_bbk_supplier_transaction_final)
