from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    clean_flag_task,
    convert_to_hex_task,
    replace_null_or_empty_values,
)


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_catalogo", "id_cliente"])
@task(name="clean_main_address_task", tags=["data cleaning", "customer"])
def clean_main_address_task(p_raw_main_address: DataFrame) -> DataFrame:
    """
    Ingest costumer raw main address data into costumer data cleaning layer.

    Args:
        p_raw_main_address (DataFrame): Customer's main address raw data.

    Returns:
        DataFrame: Processed customer's main address DataFrame.
    """
    df_main_address = p_raw_main_address.select(
        convert_to_hex_task(f.col("Codigo_Cliente"), "cliente"),
        f.initcap(f.col("DW_departamento_Descripcion")).alias(
            "departamento_residencia"
        ),
        f.initcap(f.col("DW_municipio_Descripcion")).alias("municipio_residencia"),
        clean_flag_task(f.col("DW_ZONA_ROJA"), "flag_zona_roja"),
        f.col("dw_fecha_informacion").alias("fecha_catalogo"),
    ).dropDuplicates()

    return df_main_address


@flow(name="main_address_flow")
def main_address_flow():
    """
    Loads, processes, and saves addresses data in datalake.

    The flow performs the following operations:
    1. Loads raw addresses data using the specified date range.
    2. Cleans and processes the addresses data.
    3. Saves the processed data to the appropriate environment
       using the specified overwrite strategy.

    Returns:
        None: This flow does not return a value,
        but it saves the processed addresses data.
    """
    raw_data = load_raw_data_flow()

    df_main_address = raw_data["raw_main_address"]

    df_main_address_final = clean_main_address_task(df_main_address)

    save_data_flow(df_main_address_final)
