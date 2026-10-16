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
@arrange_columns(p_start_cols=["id_caso_crm", "cuenta_corporativa"])
@task(name="clean_loan_case_task", tags=["data cleaning", "loan"])
def clean_loan_case_task(p_raw_loan_case: DataFrame) -> DataFrame:
    """
    Cleans and processes loan case data.

    This task processes raw loan case data by applying transformations and
    cleaning procedures. It standardizes specific fields, and fills any
    missing or null values with appropriate default values.

    Args:
        p_raw_loan_case (DataFrame): Raw customer loan case.

    Returns:
        DataFrame: Processed loan DataFrame.
    """
    df_loan = p_raw_loan_case.select(
        convert_to_hex_task(f.col("numero_caso_crm"), "id_crm"),
        convert_to_hex_task(f.col("dw_cuenta_corporativa"), "cuenta"),
        f.to_date(f.col("dw_fecha_transaccion")).alias("fecha_carga"),
        f.col("codigo_empresa").cast("int").alias("id_empresa"),
        f.col("codigo_estado_crm").cast("int").alias("id_estado_crm"),
        f.col("descripcion_estado_crm"),
        f.last_day(f.add_months(f.current_date(), -1)).alias("fecha_catalogo"),
    ).drop_duplicates()

    return df_loan


@flow(name="loan_case_flow")
def loan_case_flow():
    """
    Load, process, and save loan case data in the data lake.

    The flow performs the following operations:
    1. Loads raw loan case data using the specified date range.
    2. Cleans and processes the loan case data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        loan case data.
    """
    raw_data = load_raw_data_flow()
    df_loan_case = raw_data["raw_loan_case"]

    df_loan_case_final = clean_loan_case_task(df_loan_case)

    save_data_flow(df_loan_case_final)
