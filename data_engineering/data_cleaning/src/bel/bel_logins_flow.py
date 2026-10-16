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
@arrange_columns(p_start_cols=["fecha_informacion", "id_instalacion"])
@task(
    name="clean_bel_logins_task",
    tags=["data cleaning", "preprocesing"],
)
def clean_bel_logins_task(raw_bel_logins: DataFrame) -> DataFrame:
    """Clean all raw data about BEL Logins

    Args:
        raw_bel_logins (DataFrame): DataFrame with raw Data.

    Returns:
        DataFrame: Processed and Cleaned all data.
    """
    df_bel_logins = raw_bel_logins.select(
        convert_to_hex_task(f.col("Instalacion"), "id_instalacion"),
        f.col("DW_FECHA_CREACION").alias("fecha_creacion"),
        f.col("DW_FECHA_ULTIMO_LOGIN").alias("ultimo_inicio_sesion"),
        f.col("fecha_informacion"),
    )

    return df_bel_logins


@flow(name="bel_logins_flow")
def bel_logins_flow():
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

    df_raw_bel_logins = raw_data_bel_users["raw_bel_logins"]
    df_bel_log_final = clean_bel_logins_task(df_raw_bel_logins)

    save_data_flow(df_bel_log_final)
