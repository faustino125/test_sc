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
@arrange_columns(p_start_cols=["fecha_transaccion", "id_instalacion"])
@task(
    name="clean_bel_app_master_task",
    tags=["data cleaning", "preprocesing"],
)
def clean_bel_app_master_task(p_raw_bel_app_master: DataFrame) -> DataFrame:
    """Clean raw data from Bel app master

    Args:
        p_raw_bel_app_master (DataFrame): Raw data about app Master.

    Returns:
        DataFrame: Processed and cleaned all data.
    """
    df_app_master = p_raw_bel_app_master.filter(
        f.col("codigoUsuario").isNotNull()
    ).select(
        convert_to_hex_task(f.col("codigoUsuario"), "id_instalacion"),
        f.lower(f.col("tipo")).alias("descripcion_operacion"),
        f.col("fecha_transaccion"),
    )

    return df_app_master


@flow(name="app_master_flow")
def app_master_flow():
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

    raw_data_bel_users = load_raw_data_flow()

    df_raw_bel_app_master = raw_data_bel_users["raw_bel_app_master"]
    df_bel_app_mtr_trx_final = clean_bel_app_master_task(df_raw_bel_app_master)

    save_data_flow(df_bel_app_mtr_trx_final)
