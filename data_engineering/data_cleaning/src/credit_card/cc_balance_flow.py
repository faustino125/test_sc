from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    convert_to_hex_task,
    sum_fields_task,
)


@arrange_columns(p_start_cols=["fecha_informacion", "cuenta_corporativa"])
@task(name="clean_cc_balance_task", tags=["data cleaning", "preprocessing"])
def clean_cc_balance_task(p_raw_balance: DataFrame) -> DataFrame:
    """
    Cleans and processes credit card balance data.

    This task processes the raw credit card balance data by
    applying transformations and cleaning steps.
    It standardizes certain fields and creates new calculated columns.

    Args:
        p_raw_balance (DataFrame): Raw credit card balance data.

    Returns:
        DataFrame: Processed credit card balance DataFrame.
    """
    balance_df = p_raw_balance.select(
        convert_to_hex_task(f.col("DW_CUENTA_CORPORATIVA"), "cuenta"),
        f.round(f.col("SALDO_CAPITAL_Q").cast("float"), 3).alias("saldo_capital_gtq"),
        f.round(f.col("SALDO_INTERES_Q").cast("float"), 3).alias("saldo_intereses_gtq"),
        f.round(f.col("SALDO_OTROS_Q").cast("float"), 3).alias("saldo_otros_gtq"),
        f.round(f.col("SALDO_PPC_Q").cast("float"), 3).alias("saldo_ppc_gtq"),
        f.round(f.col("SALDO_CAPITAL_D").cast("float"), 3).alias("saldo_capital_usd"),
        f.round(f.col("SALDO_INTERES_D").cast("float"), 3).alias("saldo_intereses_usd"),
        f.round(f.col("SALDO_OTROS_D").cast("float"), 3).alias("saldo_otros_usd"),
        f.round(f.col("SALDO_PPC_D").cast("float"), 3).alias("saldo_ppc_usd"),
        f.round(f.col("CUOTA_MIN_PAGO_Q").cast("float"), 3).alias(
            "cuota_minima_pago_gtq"
        ),
        f.round(f.col("CUOTA_MIN_PAGO_D").cast("float"), 3).alias(
            "cuota_minima_pago_usd"
        ),
        f.col("fecha_informacion"),
    )

    balance_df = balance_df.select(
        f.col("fecha_informacion"),
        f.col("cuenta_corporativa"),
        f.col("saldo_capital_gtq"),
        f.col("saldo_intereses_gtq"),
        f.col("saldo_otros_gtq"),
        sum_fields_task(
            [
                f.col("saldo_capital_gtq"),
                f.col("saldo_intereses_gtq"),
                f.col("saldo_otros_gtq"),
                f.col("saldo_ppc_gtq"),
            ]
        ).alias("saldo_total_gtq"),
        f.col("saldo_capital_usd"),
        f.col("saldo_intereses_usd"),
        sum_fields_task(
            [
                f.col("saldo_capital_usd"),
                f.col("saldo_intereses_usd"),
                f.col("saldo_otros_usd"),
                f.col("saldo_ppc_usd"),
            ]
        ).alias("saldo_total_usd"),
        f.col("cuota_minima_pago_gtq"),
        f.col("cuota_minima_pago_usd"),
    )

    return balance_df


@flow(name="cc_balance_flow")
def cc_balance_flow():
    """
    Load, process, and save credit card balance data in the data lake.

    This flow performs the following operations:
    1. Loads raw credit card balance data using the specified date range.
    2. Cleans and processes the credit card balance data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed credit
        card balance data.
    """
    raw_data = load_raw_data_flow()

    df_cc_balance = raw_data["raw_cc_balance"]

    df_cc_balance = clean_cc_balance_task(df_cc_balance)

    save_data_flow(df_cc_balance)
