from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    add_zeros_to_column,
    replace_null_or_empty_values,
)


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_catalogo", "codigo_agencia"])
@task(name="clean_qflow_branches_task", tags=["data cleaning", "preprocessing"])
def clean_qflow_branches_task(p_raw_data_qflow_branches: DataFrame) -> DataFrame:
    """
    Ingest qflow branches data into physical channel data cleaning layer.

    Args:
        p_raw_data_qflow_branches (DataFrame): Qflow branches data.

    Returns:
        DataFrame: Processed Qflow branches DataFrame.
    """

    return p_raw_data_qflow_branches.select(
        add_zeros_to_column(f.col("codigo_agencia"), 3).alias("id_agencia"),
        f.lower(f.col("descripcion_agencia")).alias("descripcion_agencia"),
        f.lit("ATP").alias("descripcion_origen"),
        f.col("fecha_catalogo"),
    )


@flow(name="qflow_branches_flow")
def qflow_branches_flow():
    """
    Loads, processes, and saves qflow branches data in datalake.

    The flow performs the following operations:
    1. Loads raw qflow branches data.
    2. Cleans and processes the qflow branches data.
    3. Saves the processed data to the appropriate environment
       using the specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but it saves the physical channel data.
    """
    raw_data = load_raw_data_flow()

    df_qflow_branches = raw_data["raw_qflow_branches"]

    df_qflow_branches_final = clean_qflow_branches_task(df_qflow_branches)

    save_data_flow(df_qflow_branches_final)
