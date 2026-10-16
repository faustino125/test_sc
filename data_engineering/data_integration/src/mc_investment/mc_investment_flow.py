import pyspark.sql.functions as f  # type: ignore
from prefect import flow, task  # type: ignore
from pyspark.sql import DataFrame  # type: ignore

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_integration.src.utils.utils import (
    fill_na,
    join_dataframes_task,
)


@task(name="get_atp_investment")
def get_atp_investment(df: DataFrame) -> DataFrame:
    """
    Transformations:
      1) Adds atp_investment_flag:
         - 'S' if inv_saldo_quetzalizado_val > 200000
         - 'N' otherwise
      2) Adds producto_desc (CASE WHEN on inv_id_producto):
         - 5  -> 'pdi'
         - 11 -> 'pfu'
         - 13 -> 'pf'

    Args:
        df (DataFrame): Must include:
            - inv_saldo_quetzalizado_val (numeric)
            - inv_id_producto (integer)

    Returns:
        DataFrame: Original columns +:
            - atp_investment_flag (string)
            - producto_desc (string)
    """

    flag_expr = f.when(
        f.col("inv_saldo_quetzalizado_val") > 200000, f.lit("S")
    ).otherwise("N")

    prod_expr = (
        f.when(f.col("inv_id_producto") == 5, "pdi")
        .when(f.col("inv_id_producto") == 11, "pfu")
        .when(f.col("inv_id_producto") == 13, "pf")
        .otherwise(None)
    )

    df_out = df.select(
        df["*"],
        flag_expr.alias("atp_investment_flag"),
        prod_expr.alias("producto_desc"),
    )
    return df_out


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente"])
@task(name="mc_investment_task", tags=["data integration", "processing"])
def mc_investment_task(
    p_transformed_investment: DataFrame,
) -> DataFrame:
    """Processes and integrates customer data.

    This task integrates the resulting data from the Data Transformation Layer,
    using features from bi investment.

    Args:
        p_transformed_investment (DataFrame): Transformed investment data.

    Returns:
        DataFrame: Unified and processed Dataframe.
    """
    df_investment_atp = get_atp_investment(p_transformed_investment)

    df_investment_piv = (
        df_investment_atp.groupBy(
            f.col("id_cliente"),
            f.col("_observ_end_dt"),
        )
        .pivot("producto_desc")
        .agg(
            f.max("inv_plazo").cast("int").alias("plazo_max_val"),
            f.min("inv_plazo").cast("int").alias("plazo_min_val"),
            f.sum("inv_plazo").cast("int").alias("plazo_sum_val"),
            f.floor(f.avg("inv_plazo")).cast("int").alias("plazo_avg_val"),
        )
    )

    df_inv_mc = df_investment_atp.groupBy(
        f.col("id_cliente"), f.col("_observ_end_dt")
    ).agg(
        f.sum(f.col("inv_saldo_gtq_val")).alias("saldo_gtq_sum"),
        f.sum(f.col("inv_saldo_usd_val")).alias("saldo_usd_sum"),
        f.sum(f.col("inv_saldo_quetzalizado_val")).alias("saldo_quetzalizado_sum"),
        f.max("atp_investment_flag").alias("atp_investment_flag"),
    )

    df_final = join_dataframes_task(
        ["id_cliente", "_observ_end_dt"],
        [
            df_inv_mc,
            df_investment_piv,
        ],
    )

    return fill_na(df_final)


@flow(name="mc_investment_flow")
def mc_investment_flow():
    """
    Load, integrate  and saves result data in data lake.

    The flow performs the following operations:
    1. Loads data using the specified date range.
    2. Processes and integrate features data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        features data.
    """
    data_transformation = load_raw_data_flow()

    df_master_investment = mc_investment_task(
        data_transformation["transformed_investment"]
    )

    save_data_flow(df_master_investment)
