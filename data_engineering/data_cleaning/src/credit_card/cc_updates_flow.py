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
@task(name="clean_cc_updates_task", tags=["data cleaning", "preprocessing"])
def clean_cc_updates_task(p_raw_updates: DataFrame) -> DataFrame:
    """
    Cleans and processes credit card updates data.

    This task processes the raw credit card updates data by
    applying transformations and cleaning steps.
    It standardizes certain fields and fills empty values with
    default values.

    Args:
        p_raw_updates (DataFrame): Raw credit card updates data.

    Returns:
        DataFrame: Processed credit card updates DataFrame.
    """
    df_updates = p_raw_updates.select(
        f.col("fecha_transaccion"),
        convert_to_hex_task(f.col("Dw_Codigo_cliente"), "cliente"),
        convert_to_hex_task(f.col("DW_CUENTA_CORPORATIVA"), "cuenta"),
        f.col("tipo_gestion").alias("id_proceso"),
        f.trim(f.col("descripcion_tipo_gestion")).alias("descripcion_proceso"),
        f.col("dato_antes").cast("int").alias("estatus_anterior"),
        f.col("dw_descripcion_dato_antes").alias("descripcion_estatus_anterior"),
        f.col("dato_despues").cast("int").alias("estatus_actual"),
        f.col("dw_descripcion_dato_despues").alias("descripcion_estatus_actual"),
    ).fillna(0)

    return df_updates


@flow(name="cc_updates_flow")
def cc_updates_flow():
    """
    Load, process, and save credit card updates data in the data lake.

    This flow performs the following operations:
    1. Loads raw credit card updates data using the specified date range.
    2. Cleans and processes the credit card updates data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed credit
        card updates data.
    """
    raw_data = load_raw_data_flow()

    df_cc_updates = raw_data["raw_cc_updates"]

    df_cc_updates = clean_cc_updates_task(df_cc_updates)

    save_data_flow(df_cc_updates)
