import pyspark.sql.functions as f  # type: ignore
from prefect import flow, task  # type: ignore
from pyspark.sql import DataFrame  # type: ignore

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_integration.src.utils.utils import fill_na


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente"])
@task(name="mc_bel_task", tags=["data integration", "processing"])
def mc_bel_task(
    p_transformed_bel: DataFrame,
) -> DataFrame:
    """Processes and integrates customer data.

    This task integrates the resulting data from the Data Transformation Layer,
    using features from bel.

    Args:
        p_transformed_bel (DataFrame): Transformed  bel data

    Returns:
        DataFrame: Unified and processed Dataframe.
    """
    df_bel = p_transformed_bel.groupBy(
        f.col("id_cliente"), f.col("_observ_end_dt")
    ).agg(
        f.sum(f.col("bel_app_transferencias_ach_enviadas_monto_gtq_val")).alias(
            "app_transferencias_ach_enviadas_monto_gtq_sum"
        ),
        f.sum(f.col("bel_app_transferencias_ach_enviadas_monto_usd_val")).alias(
            "app_transferencias_ach_enviadas_monto_usd_sum"
        ),
        f.sum(f.col("bel_app_transferencias_ach_enviadas_gtq_cnt")).alias(
            "app_transferencias_ach_enviadas_gtq_cnt"
        ),
        f.sum(f.col("bel_app_transferencias_terceros_enviadas_monto_gtq_val")).alias(
            "app_transferencias_terceros_enviadas_monto_gtq"
        ),
        f.sum(f.col("bel_app_transferencias_terceros_enviadas_gtq_cnt")).alias(
            "app_transferencias_terceros_enviadas_gtq_cnt"
        ),
        f.sum(f.col("bel_app_monto_transacciones_gtq_val")).alias(
            "app_monto_transacciones_gtq"
        ),
        f.sum(f.col("bel_app_transacciones_gtq_cnt")).alias(
            "app_transacciones_gtq_cnt"
        ),
        f.sum(f.col("bel_web_transferencias_ach_enviadas_monto_gtq_val")).alias(
            "web_transferencias_ach_enviadas_monto_gtq"
        ),
        f.sum(f.col("bel_web_transferencias_ach_enviadas_gtq_cnt")).alias(
            "web_transferencias_ach_enviadas_gtq_cnt"
        ),
        f.sum(f.col("bel_web_monto_transacciones_gtq_val")).alias(
            "web_monto_transacciones_gtq"
        ),
        f.sum(f.col("bel_web_transacciones_gtq_cnt")).alias(
            "web_transacciones_gtq_cnt"
        ),
    )

    return fill_na(df_bel)


@flow(name="mc_bel_flow")
def mc_bel_flow():
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

    df_master_bel = mc_bel_task(data_transformation["transformed_bel"])

    save_data_flow(df_master_bel)
