from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_transformation.src.utils.utils import (
    split_currency_task,
    unified_currency_col_task,
)


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente"])
@task(
    name="transform_physical_channel_task", tags=["data transformation", "processing"]
)
def transform_physical_channel_task(
    p_cleaned_branch_operations: DataFrame,
    p_cleaned_qflow_branches: DataFrame,
    p_cleaned_exchange_rate: DataFrame,
) -> DataFrame:
    """
    Transforms and processes physical_channel information to create features.

    This task processes the clean branches and physical transactions data by
    applying transformation steps. It creates new columns based on joins,
    creates new data based on transformations, and joins them into a
    unnified dataset.

    Args:
        p_cleaned_branch_operations (DataFrame): Cleaned branch operations data.
        p_cleaned_qflow_branches (DataFrame): Cleaned branches data.

    Returns:
        DataFrame: Physical_channel features DataFrame.
    """
    df_jn_exch_operations = p_cleaned_branch_operations.join(
        p_cleaned_exchange_rate,
        on=[
            p_cleaned_branch_operations.fecha_transaccion
            == p_cleaned_exchange_rate.fecha_transaccion
        ],
    ).select(p_cleaned_branch_operations["*"], f.col("tasa_cambio"))

    df_jn_branch_op = (
        df_jn_exch_operations.join(p_cleaned_qflow_branches, on=["id_agencia"])
        .filter(f.col("id_moneda") != 0)
        .select(
            f.col("id_cliente"),
            f.col("id_agencia"),
            f.col("cuenta_corporativa"),
            f.col("id_aplicacion"),
            f.col("descripcion_aplicacion"),
            f.col("autorizacion"),
            f.col("id_moneda"),
            *split_currency_task("total_operacion", "id_moneda"),
            unified_currency_col_task(
                "id_moneda", "total_operacion", "tasa_cambio"
            ).alias("total_operacion_quetzalizado"),
            f.col("descripcion_moneda"),
            f.col("total_operacion"),
            p_cleaned_branch_operations._observ_end_dt,
        )
    )

    pv_branch_op = (
        df_jn_branch_op.groupBy(
            f.col("id_cliente"), f.col("cuenta_corporativa"), f.col("_observ_end_dt")
        )
        .pivot("descripcion_aplicacion")
        .agg(f.count(f.col("cuenta_corporativa")).alias("_cant"))
    ).fillna(0)

    df_gby_phy_op = df_jn_branch_op.groupBy(
        f.col("id_cliente"), f.col("cuenta_corporativa"), f.col("_observ_end_dt")
    ).agg(
        f.max(f.col("id_aplicacion")).alias("id_aplicacion"),
        f.last(f.col("descripcion_aplicacion")).alias("descripcion_aplicacion"),
        f.count(f.col("autorizacion")).alias("transaciones_agencia_qflow_cnt"),
        f.sum(f.col("total_operacion_gtq")).alias("monto_transaccion_gtq"),
        f.sum(f.col("total_operacion_usd")).alias("monto_transaccion_usd"),
        f.sum(f.col("total_operacion_quetzalizado")).alias(
            "monto_transaccion_quetzalizado"
        ),
        f.last(f.col("id_agencia")).alias("id_agencia"),
        f.max(f.col("id_moneda")).alias("id_moneda"),
        f.last(f.col("descripcion_moneda")).alias("descripcion_moneda"),
    )

    jn_pv_gpby_final = df_gby_phy_op.join(
        pv_branch_op, ["id_cliente", "cuenta_corporativa", "_observ_end_dt"]
    )

    return jn_pv_gpby_final


@flow(name="physical_channel_flow")
def physical_channel_flow():
    """
    Loads, transforms and saves branch operations features in the data lake.

    The flow performs the following operations:
    1. Loads raw branch operations data using the specified date range.
    2. Transforms and processes cleaned data to create features data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        features data.
    """
    cleaned_data = load_raw_data_flow()
    df_phy_ch_features = transform_physical_channel_task(
        cleaned_data["cleaned_branch_operations"],
        cleaned_data["cleaned_qflow_branches"],
        cleaned_data["cleaned_exchange_rate"],
    )

    save_data_flow(df_phy_ch_features)
