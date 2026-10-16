from typing import List

from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_transformation.src.utils.external_bureau_utils import (
    sib_entry_point_mapping,
)
from data_engineering.data_transformation.src.utils.utils import join_dataframes_task


@task(name="debt_type_task", tags=["data transformation", "processing"])
def debt_type_task(
    p_debt_type: str, p_df_worst_risk: DataFrame, p_risk_categories: DataFrame
) -> DataFrame:
    """
    Classify the debt type

    Args:
        p_debt_type (str): Debt type.
        p_df_worst_risk (DataFrame): Worst risk category for each debt type.
        p_risk_categories (DataFrame): Risk categories.

    Returns:
        DataFrame: Classified worst debt for each debt type.
    """
    if p_debt_type == "I":
        type = "deuda_indirecta_"
    else:
        type = "deuda_directa_"

    out = (
        p_df_worst_risk.alias("out")
        .join(
            p_risk_categories,
            on=[
                p_risk_categories["id_categoria_riesgo"] == p_df_worst_risk[p_debt_type]
            ],
            how="left",
        )
        .select(
            f.col("out.*"),
            f.col("id_categoria_riesgo").alias(type + "id_peor_categoria_deuda"),
            f.col("descripcion_categoria_riesgo").alias(
                type + "descripcion_peor_categoria_deuda"
            ),
        )
        .drop(p_debt_type)
    )
    return out


@task(
    name="process_worst_debt_category_task", tags=["data transformation", "processing"]
)
def process_worst_debt_category_task(
    p_cleaned_sib_debt_detail: DataFrame,
    p_cleaned_sib_risk_categories: DataFrame,
) -> DataFrame:
    """
    Transforms and processes worst debt category data.

    This task processes the clean SIB debt detail data by applying
    transformations and transforming steps. It standardizes column names, creates new
    columns based on transformations and aggregations.

    Args:
        p_cleaned_sib_debt_detail (DataFrame): Cleaned SIB
        debt detail data.
        p_cleaned_sib_risk_categories (DataFrame): SIB risk categories.

    Returns:
        DataFrame: Transformed worst debt category DataFrame.
    """
    df_risk_categories = p_cleaned_sib_risk_categories.select(
        "id_categoria_riesgo", "descripcion_categoria_riesgo"
    )
    df_worst_risk = (
        p_cleaned_sib_debt_detail.filter(f.col("id_categoria_riesgo") != "S")
        .groupBy(
            f.col("_observ_end_dt"),
            f.col("fecha_saldos"),
            f.col("id_cliente"),
            f.col("id_persona"),
        )
        .pivot("id_tipo_deuda")
        .agg(f.max(f.col("id_categoria_riesgo")))
    )

    df_worst_risk = debt_type_task("D", df_worst_risk, df_risk_categories)
    df_worst_risk = debt_type_task("I", df_worst_risk, df_risk_categories)

    return df_worst_risk


@task(name="process_debt_conditions_task", tags=["data transformation", "processing"])
def process_debt_conditions_task(
    p_cleaned_sib_debt_detail: DataFrame,
    p_credit_card_products: List,
) -> DataFrame:
    """
    Transforms and processes debt conditions data.

    This task processes the clean SIB debt detail data by applying
    transformations and transforming steps. It standardizes column names, creates new
    columns based on transformations and aggregations.

    Args:
        p_cleaned_sib_debt_detail (DataFrame): Cleaned SIB
        debt detail data.
        p_credit_card_products (List): Credit card products names.

    Returns:
        DataFrame: Transformed debt conditions DataFrame.
    """
    df_cc_total_debt = (
        p_cleaned_sib_debt_detail.filter(
            f.col("agrupacion_tipo_activo").isin(p_credit_card_products)
        )
        .groupBy(
            "_observ_end_dt",
            "fecha_saldos",
            "id_cliente",
            "descripcion_tipo_deuda",
            "agrupacion_tipo_activo",
            "nombre_entidad",
            "capital_original",
        )
        .count()
        .select(
            f.col("_observ_end_dt"),
            f.col("fecha_saldos"),
            f.col("id_cliente"),
            f.when(
                f.col("count") % 2 == 0,
                f.when(f.col("count") == 2, f.col("capital_original")).otherwise(
                    f.col("capital_original") * f.col("count") / 2
                ),
            )
            .otherwise(
                f.when(f.col("count") == 1, f.col("capital_original")).otherwise(
                    f.col("capital_original") * (f.col("count") + 1) / 2
                )
            )
            .alias("tc_limite_total"),
            f.concat(
                f.col("descripcion_tipo_deuda"),
                f.lit("_"),
                f.col("agrupacion_tipo_activo"),
            ).alias("tc_tipo_deuda"),
        )
        .groupBy(f.col("_observ_end_dt"), f.col("fecha_saldos"), f.col("id_cliente"))
        .pivot("tc_tipo_deuda")
        .agg(f.sum("tc_limite_total"))
        .withColumnRenamed("deuda_directa_tc", "deuda_directa_tc_limite_otorgado_gtq")
        .withColumnRenamed(
            "deuda_directa_tc_factorada",
            "deuda_directa_tc_factorada_limite_otorgado_gtq",
        )
        .withColumnRenamed(
            "deuda_indirecta_tc", "deuda_indirecta_tc_limite_otorgado_gtq"
        )
    )

    df_eb_debt_detail = p_cleaned_sib_debt_detail.filter(
        f.col("id_tipo_deuda") == "D"
    ).withColumn(
        "condiciones_deuda",
        f.concat(
            f.col("descripcion_tipo_deuda"),
            f.lit("_"),
            f.col("agrupacion_tipo_activo"),
        ),
    )

    df_indirect_total = (
        p_cleaned_sib_debt_detail.filter(f.col("id_tipo_deuda") == "I")
        .groupBy(f.col("_observ_end_dt"), f.col("fecha_saldos"), f.col("id_cliente"))
        .agg(
            f.sum("saldo_deuda").alias("deuda_indirecta_saldo_actual_deuda_gtq"),
            f.round(f.sum("conteo_customizado"))
            .cast("int")
            .alias("deudas_indirectas_cnt"),
            f.sum(
                f.when(
                    ~f.col("agrupacion_tipo_activo").isin(p_credit_card_products),
                    f.col("capital_original"),
                )
            ).alias("deuda_indirecta_monto_otorgado_gtq"),
        )
    )

    df_debt_conditions = (
        df_eb_debt_detail.groupBy(
            f.col("_observ_end_dt"), f.col("fecha_saldos"), f.col("id_cliente")
        )
        .pivot("condiciones_deuda")
        .agg(
            f.sum("capital_original").alias("monto_otorgado_gtq"),
            f.sum("saldo_deuda").alias("saldo_actual_deuda_gtq"),
            f.round(f.sum("conteo_customizado")).cast("int").alias("cnt"),
        )
    ).drop(
        "deuda_directa_tc_monto_otorgado_gtq",
        "deuda_directa_tc_factorada_monto_otorgado_gtq",
    )
    df_debt_conditions = join_dataframes_task(
        ["_observ_end_dt", "fecha_saldos", "id_cliente"],
        [df_debt_conditions, df_cc_total_debt, df_indirect_total],
    )

    return df_debt_conditions


@task(name="process_debt_status_task", tags=["data transformation", "processing"])
def process_debt_status_task(
    p_cleaned_sib_debt_detail: DataFrame,
) -> DataFrame:
    """
    Transforms and processes debt status data.

    This task processes the clean SIB debt detail data by applying
    transformations and transforming steps. It standardizes column names, creates new
    columns based on transformations and aggregations.

    Args:
        p_cleaned_sib_debt_detail (DataFrame): Cleaned SIB
        debt detail data.

    Returns:
        DataFrame: Transformed debt status features DataFrame.
    """
    df_eb_debt_detail = p_cleaned_sib_debt_detail.withColumn(
        "estatus_deuda",
        f.concat(
            f.col("descripcion_tipo_deuda"),
            f.lit("_"),
            f.col("descripcion_situacion_producto"),
        ),
    )

    df_direct_debt_status = (
        df_eb_debt_detail.groupBy(
            f.col("_observ_end_dt"), f.col("fecha_saldos"), f.col("id_cliente")
        )
        .pivot("estatus_deuda")
        .agg(
            f.round(f.sum("conteo_customizado")).cast("int"),
        )
        .fillna(0)
    )

    for col in df_direct_debt_status.schema.fields:
        if isinstance(col.dataType, (IntegerType)):
            df_direct_debt_status = df_direct_debt_status.withColumnRenamed(
                col.name, col.name + "_cnt"
            )

    return df_direct_debt_status


@task(
    name="pre_process_debt_datail_task", tags=["data transformation", "pre_processing"]
)
def pre_process_debt_datail_task(
    p_cleaned_sib_debt_detail: DataFrame,
    p_cleaned_sib_general_data: DataFrame,
) -> DataFrame:
    """
    Get latest debts data.

    Args:
        p_cleaned_sib_debt_detail (DataFrame): Cleaned SIB
        debt detail data.
        p_cleaned_sib_general_data (DataFrame): Cleaned debt general data.

    Returns:
        DataFrame: Debt detail unique debts DataFrame.
    """
    current_debts_window = Window.partitionBy(
        f.col("fecha_transaccion"), f.col("id_cliente")
    ).orderBy(f.col("id_deuda").desc())

    debt_detail = p_cleaned_sib_debt_detail.withColumn(
        "debt_number", f.row_number().over(current_debts_window)
    )

    transactions_qty_df = p_cleaned_sib_general_data.groupBy(
        "fecha_transaccion", "id_cliente"
    ).agg(f.count(f.lit("1")).alias("transactions_qty"))
    total_debt_records = (
        debt_detail.groupBy("fecha_transaccion", "id_cliente")
        .agg(f.count(f.lit("1")).alias("debts_qty"))
        .join(transactions_qty_df, on=["fecha_transaccion", "id_cliente"])
        .withColumn(
            "max_records", f.ceil(f.col("debts_qty") / f.col("transactions_qty"))
        )
    )

    debt_detail = (
        debt_detail.join(total_debt_records, on=["fecha_transaccion", "id_cliente"])
        .filter(f.col("debt_number") <= f.col("max_records"))
        .drop("debt_number", "max_records", "debts_qty", "transactions_qty")
    )

    return debt_detail


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente"])
@task(
    name="transform_debt_detail_task",
    tags=["data transformation", "processing"],
)
def transform_debt_detail_task(
    p_cleaned_sib_debt_detail: DataFrame,
    p_cleaned_sib_risk_categories: DataFrame,
    p_cleaned_customer: DataFrame,
    p_cleaned_sib_entry_point: DataFrame,
    p_cleaned_sib_general_data: DataFrame,
) -> DataFrame:
    """
    Transforms and processes SIB debt detail data.

    This task processes the clean SIB debt detail data that was previously
    cleaned in the data cleaning layer.
    Args:
        p_cleaned_sib_debt_detail (DataFrame): Cleaned debt detail.
        p_cleaned_sib_risk_categories (DataFrame): Cleaned SIB
        risk categories.
        p_cleaned_customer (DataFrame): Cleaned customer data.
        p_cleaned_sib_entry_point (DataFrame): Cleaned SIB
        map between SIB id and BI customer id.
        p_cleaned_sib_general_data (DataFrame): Cleaned debt general data.

    Returns:
        DataFrame: Transformed SIB debt detail DataFrame.
    """
    p_cleaned_sib_general_data = sib_entry_point_mapping(
        p_cleaned_customer, p_cleaned_sib_entry_point, p_cleaned_sib_general_data
    )
    debt_date = (
        p_cleaned_sib_general_data.groupBy(
            f.col("_observ_end_dt"), f.col("id_cliente"), f.col("fecha_saldos")
        )
        .agg(f.max("fecha_transaccion").alias("fecha_transaccion"))
        .drop("_observ_end_dt")
    )

    credit_card_products = ["tc", "tc_factorada"]
    p_cleaned_sib_debt_detail = sib_entry_point_mapping(
        p_cleaned_customer, p_cleaned_sib_entry_point, p_cleaned_sib_debt_detail
    )
    p_cleaned_sib_debt_detail = pre_process_debt_datail_task(
        p_cleaned_sib_debt_detail, p_cleaned_sib_general_data
    )

    p_cleaned_sib_debt_detail = p_cleaned_sib_debt_detail.filter(
        (
            f.col("agrupacion_tipo_activo").isin(credit_card_products)
            | ((f.col("saldo_deuda") > 0))
        )
        & ~(f.col("descripcion_situacion_producto").contains("cancelacion"))
    )

    p_cleaned_sib_debt_detail = p_cleaned_sib_debt_detail.join(
        debt_date, on=["fecha_transaccion", "id_cliente"]
    ).withColumn(
        "conteo_customizado",
        f.when(
            f.col("agrupacion_tipo_activo").isin(credit_card_products),
            0.5,
        ).otherwise(1),
    )

    df_worst_risk_category = process_worst_debt_category_task(
        p_cleaned_sib_debt_detail, p_cleaned_sib_risk_categories
    )
    df_debt_conditions = process_debt_conditions_task(
        p_cleaned_sib_debt_detail, credit_card_products
    )

    df_debt_status = process_debt_status_task(p_cleaned_sib_debt_detail)

    df_sib_debt_detail_features = join_dataframes_task(
        ["_observ_end_dt", "fecha_saldos", "id_cliente"],
        [
            df_worst_risk_category,
            df_debt_conditions,
            df_debt_status,
        ],
    )

    return df_sib_debt_detail_features


@flow(name="sib_debt_detail_flow")
def sib_debt_detail_flow():
    """
    Loads, transforms and saves SIB debt detail data features
    in the data lake.

    The flow performs the following operations:
    1. Loads SIB debt detail data using the specified date range.
    2. Transforms and processes the SIB debt detail features data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        SIB debt detail features data.
    """
    raw_data = load_raw_data_flow()

    df_sib_debt_detail_features = transform_debt_detail_task(
        raw_data["cleaned_sib_debt_detail"],
        raw_data["cleaned_sib_risk_category"],
        raw_data["cleaned_customer"],
        raw_data["cleaned_sib_entry_point"],
        raw_data["cleaned_sib_general_data"],
    )

    save_data_flow(df_sib_debt_detail_features)
