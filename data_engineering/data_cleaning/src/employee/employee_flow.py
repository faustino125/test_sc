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
@arrange_columns(p_start_cols=["fecha_informacion", "id_cliente"])
@task(name="clean_employee_task", tags=["data cleaning", "preprocessing"])
def clean_employee_task(p_raw_data_employee: DataFrame) -> DataFrame:
    """
    Cleans and processes employee data.

    This task processes the raw employee data by applying transformations and cleaning
    steps. It standardizes column names, transforms encrypted columns, and deletes
    dupplicate rows.

    Args:
        p_raw_data_employee (DataFrame): Raw employee data.

    Returns:
        DataFrame: Processed employee DataFrame.
    """
    df_employee = (
        p_raw_data_employee.filter(f.col("Codigo_Cliente").isNotNull())
        .select(
            convert_to_hex_task(f.col("Codigo_Cliente"), "cliente"),
            f.col("cia_codigo").alias("id_empresa"),
            f.col("cia_des").alias("nombre_empresa"),
            f.col("estado").alias("estatus_empleado"),
            clean_flag_task(f.col("estado"), "flag_empleado_activo"),
            f.col("pai_nacionalidad").alias("nacionalidad"),
            f.col("plz_nombre").alias("plaza"),
            f.col("pue_codigo").alias("id_puesto"),
            f.col("pue_nombre").alias("puesto"),
            f.col("fecha_informacion"),
        )
        .drop_duplicates()
    )

    return df_employee


@flow(name="employee_flow")
def employee_flow():
    """
    Load, process, and save employee data in the data lake.

    The flow performs the following operations:
    1. Loads raw employee data using the specified date range.
    2. Cleans and processes the employee data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        employee data.
    """
    raw_data = load_raw_data_flow()

    df_employee = raw_data["raw_employee"]

    df_employee_final = clean_employee_task(df_employee)

    save_data_flow(df_employee_final)
