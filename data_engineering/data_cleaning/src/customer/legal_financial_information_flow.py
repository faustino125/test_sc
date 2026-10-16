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
@arrange_columns(p_start_cols=["fecha_catalogo", "id_cliente"])
@task(name="clean_legal_financial_information_task", tags=["data cleaning", "customer"])
def clean_legal_financial_information_task(
    p_raw_legal_financial_information: DataFrame,
) -> DataFrame:
    """
    Ingest costumer raw legal financial information.

    Args:
        p_raw_legal_financial_information (DataFrame): Customer's legal financial.

    Returns:
        DataFrame: Processed legal customer's financial information DataFrame.
    """
    df_legal_financial_information = p_raw_legal_financial_information.select(
        convert_to_hex_task(f.col("dw_codigo_cliente"), "cliente"),
        f.coalesce(f.col("ingresos_anuales_moodys"), f.lit("0"))
        .cast("float")
        .alias("ingresos_anuales_moodys_gtq"),
        f.coalesce(f.col("ingresos_anuales_sib"), f.lit("0"))
        .cast("float")
        .alias("ingresos_anuales_sib_gtq"),
        f.coalesce(f.col("ingresos_anuales_e83"), f.lit("0"))
        .cast("float")
        .alias("ingresos_anuales_e83_gtq"),
        f.coalesce(f.col("ingresos_anuales_ire"), f.lit("0"))
        .cast("float")
        .alias("ingresos_anuales_ire_gtq"),
        f.coalesce(f.col("ingresos_anuales_monetarios"), f.lit("0")).alias(
            "ingresos_anuales_monetarios_gtq"
        ),
        f.year("fecha_catalogo").alias("anio_informacion"),
        f.col("fecha_catalogo"),
    )

    return df_legal_financial_information


@flow(name="legal_financial_information_flow")
def legal_financial_information_flow():
    """
    Loads, processes, and saves legal client's financial information data in datalake.

    The flow performs the following operations:
    1. Loads raw financial information data.
    2. Cleans and processes financial information data.
    3. Saves the processed data to the appropriate environment
       using the specified overwrite strategy.

    Returns:
        None: This flow does not return a value,
        but it saves the processed financial information data.
    """
    raw_data = load_raw_data_flow()

    df_legal_financial_information = raw_data["raw_legal_financial_information"]

    df_legal_financial_information_final = clean_legal_financial_information_task(
        df_legal_financial_information
    )

    save_data_flow(df_legal_financial_information_final)
