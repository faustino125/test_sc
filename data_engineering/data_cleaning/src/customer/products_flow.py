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
@arrange_columns(p_start_cols=["fecha_informacion", "id_cliente", "cuenta_corporativa"])
@task(name="clean_products_task", tags=["data cleaning", "preprocessing"])
def clean_products_task(p_raw_customer_products: DataFrame) -> DataFrame:
    """
    Cleans and processes customer products data.

    This task processes the raw customer products data by applying transformations and
    cleaning steps. It standardizes certain fields, creates new calculated columns, and
    fills empty values with default values.

    Args:
        p_raw_customer_products (DataFrame): Raw customer products data.

    Returns:
        DataFrame: Processed customer products DataFrame.
    """

    df_products = p_raw_customer_products.select(
        convert_to_hex_task("Codigo_Cliente", "cliente"),
        convert_to_hex_task("Cuenta_Corporativa", "cuenta"),
        f.col("Dw_Aplicacion_Codigo").alias("id_aplicacion"),
        f.lower(
            f.regexp_replace(
                f.regexp_replace(f.trim(f.col("Dw_Aplicacion_Descripcion")), " ", "_"),
                "-",
                "",
            )
        ).alias("descripcion_aplicacion"),
        f.col("fecha_informacion"),
    ).dropDuplicates()

    return df_products


@flow(name="products_flow")
def products_flow():
    """
    Load, process, and save customer data in the data lake.

    The flow performs the following operations:
    1. Loads raw customer data using the specified date range.
    2. Cleans and processes the customer products data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        customer data.
    """
    raw_data = load_raw_data_flow()

    df_products = raw_data["raw_products"]

    df_products_final = clean_products_task(df_products)

    save_data_flow(df_products_final)
