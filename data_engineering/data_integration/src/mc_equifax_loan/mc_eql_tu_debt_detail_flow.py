from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_integration.src.utils.utils import (
    remove_prefix_from_columns_task,
    remove_suffix_from_columns_task,
)


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente_bi"])
@task(name="mc_debt_detail_task", tags=["data integration", "processing"])
def mc_debt_detail_task(
    p_transformed_debt_detail: DataFrame,
) -> DataFrame:
    """Processes and integrates equifax tu loan debt detail data

    This task integrates the resulting data from the Data Transformation Layer,
    using features from equifax tu debt detail

    Args:
        p_transformed_debt_detail (DataFrame): Transformed  equifax tu debt detail
        data.

    Returns:
        DataFrame: Unified and processed Dataframe.
    """

    window_spec = Window.partitionBy(
        "eqpr_id_cliente_bi",
        "_observ_end_dt",
        "eqpr_agrupacion_tipo_activo",
        "eqpr_entidad",
        "eqpr_peor_estado_deuda_descripcion",
        "eqpr_fecha_referencia_deuda",
    ).orderBy(f.col("eqpr_fecha_transaccion").desc())

    df_debt_detail_rank = p_transformed_debt_detail.withColumn(
        "n_rank", f.row_number().over(window_spec)
    )

    df_debt_detail = remove_suffix_from_columns_task(
        df_debt_detail_rank.filter(f.col("n_rank") == 1).drop(
            "n_rank",
            "eqpr_id_tipo_deuda",
            "eqpr_id_numero_solicitud",
            "eqpr_id_cliente_persona",
            "eqpr_id_buro_externo",
            "eqpr_id_entidad",
            "eqpr_descripcion_entidad",
            "eqpr_agrupacion_entidad",
        ),
        "_val",
    )

    return remove_prefix_from_columns_task(df_debt_detail, "eqpr_")


@flow(name="mc_eql_tu_debt_detail_flow")
def mc_eql_tu_debt_detail_flow():
    """
    Loads and integrates granular product based information and converts
    it into client based information that is saved in data lake.

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

    df_master_debt_detail = mc_debt_detail_task(
        data_transformation["transformed_eql_tu_debt_detail"]
    )

    save_data_flow(df_master_debt_detail)
