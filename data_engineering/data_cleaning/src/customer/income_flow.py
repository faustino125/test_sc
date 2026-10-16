from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import DecimalType

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    convert_to_hex_task,
)


@arrange_columns(p_start_cols=["fecha_informacion", "id_cliente"])
@task(name="clean_income_task", tags=["data cleaning", "customer"])
def clean_income_task(p_raw_data_income: DataFrame) -> DataFrame:
    """
    Ingest costumer raw income data into costumer data_cleaning layer.

    Args:
        p_raw_data_income (DataFrame): Raw customer data.

    Returns:
        DataFrame: Processed customer DataFrame.
    """
    df_income = p_raw_data_income.select(
        convert_to_hex_task(f.col("Cif_Empleado"), "cliente"),
        f.col("monto_promedio").cast(DecimalType(10, 2)).alias("monto_promedio"),
        f.col("fecha_informacion"),
    ).drop_duplicates()

    return df_income


@flow(name="income_flow")
def income_flow():
    """
    Load, process, and save income data in the data lake.

    The flow performs the following operations:
    1. Loads raw income data using the specified date range.
    2. Cleans and processes the income data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        income data.
    """
    raw_data = load_raw_data_flow()

    df_income = raw_data["raw_income"]

    df_income_final = clean_income_task(df_income)

    save_data_flow(
        df_income_final,
    )
