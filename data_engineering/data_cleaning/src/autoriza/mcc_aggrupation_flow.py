from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    replace_null_or_empty_values,
)


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_catalogo", "mcc"])
@task(name="clean_mcc_aggrupation_task", tags=["data cleaning", "preprocessing"])
def clean_mcc_aggrupation_task(p_raw_mcc_data: DataFrame) -> DataFrame:
    """
    Cleans and processes credit and debit card mcc aggrupation
    categories data.

    This task processes the raw mcc aggrupation categories data by
    applying transformations and cleaning steps.
    It standardizes certain fields, creates new calculated columns,
    and fills empty values with default values.

    Args:
        p_raw_mcc_data (DataFrame): Raw mcc aggrupation categories data

    Returns:
        DataFrame: Processed mcc aggrupation categories DataFrame.
    """

    df_mcc_catalog = (
        p_raw_mcc_data.filter(f.col("agrupacion") != "pruebas")
        .groupBy(
            f.col("mcc").cast("int").alias("mcc"),
            f.col("agrupacion").alias("agrupacion_mcc"),
            f.col("fecha_catalogo"),
        )
        .agg(f.last(f.col("descripcion")).alias("descripcion_mcc"))
    )

    return df_mcc_catalog


@flow(name="mcc_aggrupation_flow")
def mcc_aggrupation_flow():
    """
    Load, process, and save credit and debit card mcc aggrupation categories
    data catalog into the data lake.

    This flow performs the following operations:
    1. Loads raw mcc aggrupation categories data.
    2. Cleans and processes the data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed mcc
        aggrupation categories data.
    """
    raw_data = load_raw_data_flow()

    df_mcc_aggrupation = raw_data["raw_mcc_aggrupation"]

    df_mcc_aggrupation = clean_mcc_aggrupation_task(df_mcc_aggrupation)

    save_data_flow(df_mcc_aggrupation)
