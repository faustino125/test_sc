from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_transformation.src.utils.external_bureau_utils import (
    sib_entry_point_mapping,
)


@task(name="process_debt_history_task", tags=["data transformation", "processing"])
def process_debt_history_task(
    p_cleaned_sib_debt_history: DataFrame,
    p_cleaned_sib_debt_detail: DataFrame,
    p_cleaned_sib_risk_category: DataFrame,
) -> DataFrame:
    """
    Transforms and processes SIB historical data.

    This task processes the clean SIB debt detail and debt history data
    by applying transformations and transforming steps. It standardizes column names,
    creates new columns based on transformations and aggregations.

    Args:
        p_cleaned_sib_debt_history (DataFrame): Cleaned SIB
        debt history data.
        p_cleaned_sib_debt_detail (DataFrame): Cleaned SIB
        debt detail data.
        p_cleaned_sib_risk_category (DataFrame): Cleaned SIB
        risk categories.

    Returns:
        DataFrame: Transformed SIB history DataFrame.
    """
    windowCat = Window.partitionBy(
        f.col("_observ_end_dt"), f.col("id_tipo_deuda"), f.col("id_cliente")
    ).orderBy(f.col("fecha_deuda").desc())

    df_sib_debt_history = p_cleaned_sib_debt_history.withColumn(
        "ultimo_registro", f.row_number().over(windowCat)
    ).filter(f.col("ultimo_registro") <= 18)

    p_cleaned_sib_risk_category = p_cleaned_sib_risk_category.select(
        "id_categoria_riesgo", "descripcion_categoria_riesgo"
    )

    latest_worst_risk_type = (
        p_cleaned_sib_debt_detail.groupBy(
            f.col("_observ_end_dt"),
            f.col("id_tipo_deuda"),
            f.col("id_cliente"),
        )
        .agg(f.max(f.col("id_categoria_riesgo")).alias("id_categoria_riesgo"))
        .alias("d")
        .join(p_cleaned_sib_risk_category, on="id_categoria_riesgo")
        .select(
            f.col("d.*"),
            f.col("descripcion_categoria_riesgo").alias(
                "descripcion_peor_categoria_riesgo_deuda"
            ),
        )
    )

    df_sib_debt_history = (
        df_sib_debt_history.join(
            p_cleaned_sib_risk_category,
            on=[
                df_sib_debt_history["peor_calificacion"]
                == p_cleaned_sib_risk_category["id_categoria_riesgo"]
            ],
            how="left",
        )
        .alias("o")
        .join(
            latest_worst_risk_type.alias("r"),
            on=["_observ_end_dt", "id_tipo_deuda", "id_cliente"],
            how="left",
        )
        .select(
            f.col("o._observ_end_dt"),
            f.col("o.id_cliente"),
            f.col("o.id_persona"),
            f.col("o.fecha_deuda").alias("fecha_historial_deuda"),
            f.col("o.id_tipo_deuda"),
            f.col("o.descripcion_tipo_deuda"),
            f.col("o.total_deuda"),
            f.col("o.max_mora_capital").alias("max_dias_mora_capital"),
            f.col("o.max_mora_intereses").alias("max_dias_mora_intereses"),
            f.col("o.total_mora_capital"),
            f.col("o.total_mora_intereses"),
            f.when(
                f.col("o.ultimo_registro") != 1,
                f.col("o.peor_calificacion"),
            )
            .otherwise(f.col("r.id_categoria_riesgo"))
            .alias("id_peor_categoria_riesgo_deuda"),
            f.when(
                f.col("o.ultimo_registro") != 1,
                f.col("o.descripcion_categoria_riesgo"),
            )
            .otherwise(f.col("r.descripcion_peor_categoria_riesgo_deuda"))
            .alias("descripcion_peor_categoria_riesgo_deuda"),
        )
        .fillna(
            {
                "id_peor_categoria_riesgo_deuda": "S",
                "descripcion_peor_categoria_riesgo_deuda": "desconocido",
            }
        )
    )

    return df_sib_debt_history


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente"])
@task(name="transform_debt_history_task", tags=["data transformation", "processing"])
def transform_debt_history_task(
    p_cleaned_sib_risk_category: DataFrame,
    p_cleaned_sib_debt_detail: DataFrame,
    p_cleaned_sib_debt_history: DataFrame,
    p_cleaned_sib_entry_point: DataFrame,
    p_cleaned_customer: DataFrame,
) -> DataFrame:
    """
    Transforms and processes SIB debt historical data.

    This task processes the clean SIB debt historical data that was previously
    cleaned in the data cleaning layer.
    Args:
        p_cleaned_sib_risk_category (DataFrame): Cleaned SIB
        risk categories.
        p_cleaned_sib_debt_detail (DataFrame): Cleaned SIB
        debt detail data.
        p_cleaned_sib_debt_history (DataFrame): Cleaned SIB
        debt history data.
        p_cleaned_sib_entry_point (DataFrame): Cleaned SIB
        map between SIB id and BI customer id.
        p_cleaned_customer (DataFrame): Cleaned customer data.

    Returns:
        DataFrame: Transformed SIB debt history DataFrame.
    """

    p_cleaned_sib_debt_detail = p_cleaned_sib_debt_detail.filter(
        f.col("id_categoria_riesgo") != "S"
    )
    latest_date = (
        p_cleaned_sib_debt_history.groupBy("_observ_end_dt", "id_persona")
        .agg(f.max("fecha_transaccion").alias("fecha_transaccion"))
        .drop("_observ_end_dt")
    )
    p_cleaned_sib_debt_history = p_cleaned_sib_debt_history.join(
        latest_date, on=["fecha_transaccion", "id_persona"]
    )

    p_cleaned_sib_debt_history = (
        sib_entry_point_mapping(
            p_cleaned_customer, p_cleaned_sib_entry_point, p_cleaned_sib_debt_history
        )
        .drop("id_historial_deuda", "fecha_transaccion")
        .select(f.col("*"))
        .distinct()
    )

    p_cleaned_sib_debt_detail = sib_entry_point_mapping(
        p_cleaned_customer, p_cleaned_sib_entry_point, p_cleaned_sib_debt_detail
    )

    df_sib_debt_history_features = process_debt_history_task(
        p_cleaned_sib_debt_history,
        p_cleaned_sib_debt_detail,
        p_cleaned_sib_risk_category,
    )

    return df_sib_debt_history_features


@flow(name=" sib_debt_history_flow")
def sib_debt_history_flow():
    """
    Loads, transforms and saves SIB debt historical data features in the data lake.

    The flow performs the following operations:
    1. Loads SIB debt data data using the specified date range.
    2. Transforms and processes the SIB debt data features data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        SIB debt historical features data.
    """
    raw_data = load_raw_data_flow()

    df_sib_debt_history_features = transform_debt_history_task(
        raw_data["cleaned_sib_risk_category"],
        raw_data["cleaned_sib_debt_detail"],
        raw_data["cleaned_sib_debt_history"],
        raw_data["cleaned_sib_entry_point"],
        raw_data["cleaned_customer"],
    )

    save_data_flow(df_sib_debt_history_features)
