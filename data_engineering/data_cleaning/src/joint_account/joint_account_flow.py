from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    convert_to_hex_task,
    replace_null_or_empty_values,
    std_situation_account_task,
)


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_informacion", "id_cliente", "cuenta_corporativa"])
@task(
    name="clean_joint_account_task",
    tags=["data cleaning", "preprocessing", "joint_account"],
)
def clean_joint_account_task(
    p_raw_joint_account: DataFrame,
) -> DataFrame:
    """Cleans and processes raw joint account data.

    Parameters:
        p_raw_joint_account: Input DataFrame containing raw joint account data.

    Returns:
        DataFrame with cleaned and structured joint account data containing.
    """
    df_joint_account = p_raw_joint_account.select(
        convert_to_hex_task(f.col("DW_CODIGO_CLIENTE"), "cliente"),
        convert_to_hex_task(f.col("DW_CUENTA_CORPORATIVA"), "cuenta"),
        f.trim(f.lower(f.col("DW_CLASE_CLIENTE"))).alias("clase_cliente"),
        f.col("DW_APLICACION_CODIGO").alias("id_producto"),
        f.trim(f.lower("DW_APLICACION_DESCRIPCION")).alias("descripcion_producto"),
        f.col("DW_SALDO_CUENTA").cast("float").alias("saldo"),
        f.col("DW_MONEDA_CODIGO").alias("id_moneda"),
        f.col("DW_PORCENTAJE").alias("porcentaje"),
        f.col("DW_ESTADO_CODIGO").alias("id_situacion_cuenta"),
        f.trim(f.lower("DW_ESTADO_DESCRIPCION")).alias("situacion_cuenta"),
        std_situation_account_task(f.col("DW_ESTADO_DESCRIPCION")),
        f.lit("S").alias("cliente_mancomunado_flag"),
        f.col("fecha_informacion"),
    )

    return df_joint_account


@flow(name="joint_account_flow")
def joint_account_flow():
    """
    Load, process, and save customer joint_account account data in the data lake.

    The flow performs the following operations:
    1. Loads customers joint_account account raw data using the specified date range.
    2. Cleans and processes the customers joint_account account data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        customers joint_account account data.
    """
    raw_data = load_raw_data_flow()

    raw_joint_account = raw_data["raw_joint_account"]

    df_joint_account_final = clean_joint_account_task(
        raw_joint_account,
    )

    save_data_flow(df_joint_account_final)
