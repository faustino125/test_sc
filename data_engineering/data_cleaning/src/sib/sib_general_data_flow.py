from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    convert_to_hex_task,
)


@arrange_columns(p_start_cols=["fecha_transaccion", "id_persona"])
@task(
    name="clean_sib_general_data_task",
    tags=["data cleaning", "transactional", "sib"],
)
def clean_sib_general_data_task(
    p_general_data: DataFrame,
) -> DataFrame:
    """
    Cleans and processes SIB summary data.

    This task processes the SIB summary raw data,
    by applying transformations and cleaning steps.
    It select the necessary fields.

    Args:
        p_general_data (DataFrame): SIB summary raw data.

    Returns:
        DataFrame: Processed SIB summary DataFrame.
    """
    return p_general_data.select(
        convert_to_hex_task("id_persona", "id_persona"),
        f.col("creditos_cancelados_directos"),
        f.col("creditos_cancelados_indirectos"),
        f.col("creditos_vigentes_directos"),
        f.col("creditos_vigentes_indirectos"),
        f.col("dw_fecha_saldos").alias("fecha_saldos"),
        f.col("flag_mora_cuatro_meses_ultimos_doce_meses"),
        f.col("flag_mora_dos_meses_ultimos_doce_meses"),
        f.col("flag_mora_dos_meses_ultimos_dos_meses"),
        f.col("flag_mora_tres_meses_ultimos_doce_meses"),
        f.col("total_deuda_contingencia"),
        f.col("total_deuda_directa"),
        f.col("total_deuda_indirecta"),
        f.col("fecha_transaccion"),
    )


@flow(name="sib_general_data_flow")
def sib_general_data_flow():
    """
    Load, process, and save SIB summary data
    in the data lake.

    The flow performs the following operations:
    1. Loads SIB summary raw data.
    2. Cleans and processes the SIB summary data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        SIB summary data.
    """
    raw_data = load_raw_data_flow()

    df_general_data_final = clean_sib_general_data_task(
        raw_data["raw_sib_general_data"]
    )

    save_data_flow(df_general_data_final)
