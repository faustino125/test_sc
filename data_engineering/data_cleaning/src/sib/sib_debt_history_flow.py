from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.external_bureau_utils import (
    clean_risk_category_task,
)
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    convert_to_hex_task,
)


@arrange_columns(p_start_cols=["fecha_transaccion", "id_persona"])
@task(
    name="clean_sib_debt_history_task",
    tags=["data cleaning", "transactional", "sib"],
)
def clean_sib_debt_history_task(
    p_debt_history: DataFrame,
) -> DataFrame:
    """
    Cleans and processes SIB debt history data.

    This task processes the SIB debt history raw data,
    by applying transformations and cleaning steps.
    It select the necessary fields.

    Args:
        p_debt_history (DataFrame): SIB debt history raw data.

    Returns:
        DataFrame: Processed SIB debt history DataFrame.
    """
    return p_debt_history.select(
        convert_to_hex_task("id_persona", "id_persona"),
        f.col("id_tipo_deuda"),
        f.col("descripcion_tipo_deuda"),
        f.col("id_hist_deuda").alias("id_historial_deuda"),
        f.col("dw_fecha_deuda").alias("fecha_deuda"),
        f.col("max_mora_capital"),
        f.col("max_mora_intereses"),
        clean_risk_category_task(f.col("peor_calificacion")).alias("peor_calificacion"),
        f.col("total_deuda"),
        f.col("total_mora_capital"),
        f.col("total_mora_intereses"),
        f.col("fecha_transaccion"),
    )


@flow(name="sib_debt_history_flow")
def sib_debt_history_flow():
    """
    Load, process, and save SIB debt history data
    in the data lake.

    The flow performs the following operations:
    1. Loads SIB debt history raw data.
    2. Cleans and processes the SIB debt history data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        SIB debt history data.
    """
    raw_data = load_raw_data_flow()

    df_debt_history_final = clean_sib_debt_history_task(
        raw_data["raw_sib_debt_history"]
    )

    save_data_flow(df_debt_history_final)
