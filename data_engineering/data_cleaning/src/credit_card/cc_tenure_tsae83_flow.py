import pyspark.sql.functions as f
from prefect import flow, task
from pyspark.sql import DataFrame

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    convert_to_hex_task,
)


@arrange_columns(p_start_cols=["fecha_informacion", "cuenta_corporativa"])
@task(name="clean_cc_tenure_tsae83_task", tags=["data cleaning", "preprocessing"])
def clean_cc_tenure_tsae83_task(
    p_raw_cc_tenure_tsae83: DataFrame,
) -> DataFrame:
    """
    Cleans and processes credit card tenure tsae83 data.

    This task processes the raw credit card tenure tsae83 data by
    applying transformations and cleaning steps.
    It standardizes certain fields, creates new calculated columns,
    and fills empty values with
    default values.

    Args:
        p_raw_cc_tenure_tsae83 (DataFrame): Raw credit card tenure tsae83 data.

    Returns:
        DataFrame: Processed credit card tenure tsae83 DataFrame.
    """
    tenure_tsae83_df = p_raw_cc_tenure_tsae83.select(
        f.col("fecha_informacion"),
        convert_to_hex_task(f.col("DW_CUENTA_CORPORATIVA"), "cuenta"),
        f.col("mor_cap_m").cast("int").alias("ciclo_mora"),
        f.col("mor_int_m").alias("ciclo_mora_intereses"),
        f.when(
            (f.col("d_moras").isNull()) | (f.col("d_moras") == 0),
            f.col("mor_Cap_m").cast("int") * 30,
        )
        .otherwise(f.col("d_moras"))
        .alias("dias_mora"),
        (f.col("MOR_CAP_M").cast("int") * 30).alias("dias_mora_capital"),
        (f.col("mor_int_m") * 30).alias("dias_mora_intereses"),
        f.col("codigo_situacion_act_cred").cast("int").alias("id_situacion_mora"),
        f.col("CODIGO_MONEDA").alias("id_moneda"),
        f.col("DW_MONEDA_DESCRIPCION").alias("descripcion_moneda"),
        f.col("MOR_CAP").cast("double").alias("monto_mora_capital"),
        f.col("MTO_PAG_ME").cast("double").alias("saldo_por_pagar"),
    )

    return tenure_tsae83_df


@flow(name="cc_tenure_tsae83_flow")
def cc_tenure_tsae83_flow():
    """
    Load, process, and save credit card tenure tsae83 data in the data lake.

    This flow performs the following operations:
    1. Loads raw credit card tenure tsae83 data using the specified date range.
    2. Cleans and processes the credit card tenure tsae83 data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed credit
        card tenure tsae83 data.
    """
    raw_data = load_raw_data_flow()

    df_cc_tenure_tsae83 = raw_data["raw_cc_tenure_tsae83"]

    df_cc_tenure_tsae83 = clean_cc_tenure_tsae83_task(df_cc_tenure_tsae83)

    save_data_flow(df_cc_tenure_tsae83)
