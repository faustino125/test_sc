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
@arrange_columns(p_start_cols=["fecha_transaccion"])
@task(
    name="clean_external_bureau_task",
    tags=["data cleaning", "preprocessing", "equifax Loan"],
)
def clean_external_bureau_task(
    p_raw_external_bureau: DataFrame,
) -> DataFrame:
    """
    Cleans and processes equifax's external bureau raw data.

    This task processes the equifax's external bureau raw
    data, by applying transformations and cleaning steps. It standardizes
    certain fields, creates new calculated columns, and fills empty values
    with default values.

    Args:
        p_raw_external_bureau (DataFrame): external bureau raw data.

    Returns:
        DataFrame: Processed equifax's external bureau DataFrame.
    """
    df_consultation_master = p_raw_external_bureau.select(
        convert_to_hex_task(f.col("be_id"), "id_buro_externo"),
        convert_to_hex_task(f.col("codigo_cliente_persona"), "id_cliente_persona"),
        convert_to_hex_task(f.col("codigo_cliente_solper"), "id_cliente_solper"),
        convert_to_hex_task(f.col("numero_solicitud"), "id_numero_solicitud"),
        f.lower("buroref").alias("buro_referencia"),
        f.col("fecha_transaccion"),
    )

    return df_consultation_master


@flow(name="eqfLoan_external_bureau_flow")
def eql_external_bureau_flow():
    """
    Load, process, and save external bureau data in the data lake.

    The flow performs the following operations:
    1. Loads external bureau raw data using the specified date range.
    2. Cleans and processes external bureau data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
         external bureau data.
    """
    raw_data = load_raw_data_flow()

    df_external_bureau = raw_data["raw_eql_external_bureau"]

    df_external_bureau_final = clean_external_bureau_task(df_external_bureau)

    save_data_flow(df_external_bureau_final)
