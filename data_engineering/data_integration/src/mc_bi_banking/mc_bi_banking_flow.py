import pyspark.sql.functions as f  # type: ignore
from prefect import flow, task  # type: ignore
from pyspark.sql import DataFrame  # type: ignore

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente"])
@task(name="mc_bi_banking_task", tags=["data integration", "processing"])
def mc_bi_banking_task(
    p_transformed_bibanking: DataFrame,
) -> DataFrame:
    """Processes and integrates customer data.

    This task integrates the resulting data from the Data Transformation Layer,
    using features from bi banking.

    Args:
        p_transformed_bibanking (DataFrame): Transformed  bi banking data.

    Returns:
        DataFrame: Unified and processed Dataframe.
    """

    df_bbk = p_transformed_bibanking.groupBy(
        f.col("id_cliente"), f.col("_observ_end_dt")
    ).agg(
        f.sum(f.col("bbk_ach_pago_planilla_quetzalizado_val")).alias(
            "ach_pago_planilla_quetzalizado_sum"
        ),
        f.sum(f.col("bbk_transacciones_cnt")).alias("transacciones_cnt"),
        f.sum(f.col("bbk_ach_concentracion_fondos_bi_quetzalizado_val")).alias(
            "ach_concentracion_fondos_bi_quetzalizado_sum"
        ),
    )

    return df_bbk


@flow(name="mc_bi_banking_flow")
def mc_bi_banking_flow():
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

    df_master_bi_banking = mc_bi_banking_task(
        data_transformation["transformed_bibanking"]
    )

    save_data_flow(df_master_bi_banking)
