from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_transformation.src.utils.eqf_utils import (
    applicant_task,
    lookup_external_bureau_task,
)
from data_engineering.data_transformation.src.utils.utils import join_dataframes_task


@task(name="customer_income_task", tags=["data transformation", "processing"])
def customer_income_task(p_df_person_request: DataFrame) -> DataFrame:
    """
    Returns customer's income

    Args:
        p_df_person_request: person request

    Returns:
        DataFrame: customer's income
    """

    df_income = p_df_person_request.groupBy(
        f.col("id_cliente_persona"), f.col("id_numero_solicitud")
    ).agg(
        f.max("meses_trabajo").alias("max_meses_trabajo"),
        f.min("fecha_ingreso_trabajo").alias("min_fecha_ingreso_trabajo"),
        f.sum(f.lit(1)).alias("empleos_reportados_cnt"),
        f.sum("salario").alias("salario_reportado"),
    )

    return df_income


@task(name="liabilities_task", tags=["data transformation", "processing"])
def liabilities_task(
    p_df_sib_debt: DataFrame, p_type_debt: str, p_pivot_col: str
) -> DataFrame:
    """
    Transforms and processes direct liabilities by asset type.

    Args:
        p_df_sib_debt: sib debt
        p_type_debt: Type debt
        p_pivot_col: Pivot column

    Returns:
        DataFrame: principal and secondary applicant
    """
    df_sib_debt = p_df_sib_debt.filter(f.col("id_tipo_deuda") == p_type_debt)

    df_liabilities = df_sib_debt.select(
        f.col("_observ_end_dt"), f.col("id_buro_externo"), f.col("fecha_transaccion")
    ).distinct()

    df_cc_debt = (
        df_sib_debt.filter((f.col("tc_flag") == 1))
        .groupBy(
            f.col("_observ_end_dt"),
            f.col("id_buro_externo"),
            f.col("id_entidad"),
            f.col(p_pivot_col),
            f.col("fecha_transaccion"),
            f.col("monto_otorgado"),
        )
        .agg(
            f.ceil(f.sum(f.lit(0.5))).cast("int").alias("cantidad_tc"),
            f.sum("saldo_actual_deuda").cast("float").alias("saldo_actual_deuda_gtq"),
        )
        .withColumn("tc_limite", f.col("monto_otorgado") * f.col("cantidad_tc"))
        .groupBy(
            f.col("_observ_end_dt"),
            f.col("id_buro_externo"),
        )
        .pivot(p_pivot_col)
        .agg(
            f.sum("cantidad_tc").alias("cnt"),
            f.sum("tc_limite").alias("limite_otorgado_gtq"),
            f.sum("saldo_actual_deuda_gtq").alias("saldo_actual_deuda_gtq"),
        )
    )

    df_others_debt = (
        df_sib_debt.filter((f.col("tc_flag") == 0))
        .groupBy(f.col("_observ_end_dt"), f.col("id_buro_externo"))
        .pivot(p_pivot_col)
        .agg(
            f.sum(f.lit(1)).cast("int").alias("cnt"),
            f.sum("monto_otorgado").alias("monto_otorgado_gtq"),
            f.sum("saldo_actual_deuda").alias("saldo_actual_deuda_gtq"),
        )
    )

    if p_type_debt == "I":
        df_cc_debt = df_cc_debt.withColumnRenamed(
            "deuda_indirecta_limite_otorgado_gtq", "deuda_indirecta_monto_otorgado_gtq"
        )

        df_indirect_debt = (
            df_cc_debt.union(df_others_debt)
            .groupBy(
                f.col("_observ_end_dt"),
                f.col("id_buro_externo"),
            )
            .agg(
                f.sum("deuda_indirecta_cnt").alias("deuda_indirecta_cnt"),
                f.sum("deuda_indirecta_monto_otorgado_gtq").alias(
                    "deuda_indirecta_monto_otorgado_gtq"
                ),
                f.sum("deuda_indirecta_saldo_actual_deuda_gtq").alias(
                    "deuda_indirecta_saldo_actual_deuda_gtq"
                ),
            )
        )

        df_liabilities_final = join_dataframes_task(
            ["_observ_end_dt", "id_buro_externo"],
            [df_liabilities, df_indirect_debt],
            "inner",
        )

    else:
        df_liabilities_final = join_dataframes_task(
            ["_observ_end_dt", "id_buro_externo"],
            [df_liabilities, df_cc_debt, df_others_debt],
            "left",
        )

    return df_liabilities_final


@task(name="type_debt_task", tags=["data transformation", "processing"])
def type_debt_task(p_type_debt: str) -> DataFrame:
    """
    Classify the type of debt

    Args:
        p_type_debt: Type debt

    Returns:
        DataFrame: Type of debt
    """
    if p_type_debt == "I":
        type = "deuda_indirecta"
    else:
        type = "deuda_directa"

    return type


@task(name="worst_debt_category_task", tags=["data transformation", "processing"])
def worst_debt_category_task(p_df_sib_debt: DataFrame, p_type_debt: str) -> DataFrame:
    """
    Returns the worst debt category

    Args:
        p_df_sib_debt: sib debt
        p_type_debt: Type debt

    Returns:
        DataFrame: worst debt category
    """
    type = type_debt_task(p_type_debt)

    df_sib_debt = p_df_sib_debt.filter(
        (f.col("id_tipo_deuda") == p_type_debt)
        & (f.col("id_categoria_riesgo_deuda") != "S")
    )

    window_spec = Window.partitionBy("_observ_end_dt", "id_buro_externo").orderBy(
        f.col("id_categoria_riesgo_deuda").desc()
    )

    df_worst__debtcategory = (
        df_sib_debt.withColumn("rn", f.row_number().over(window_spec))
        .filter(f.col("rn") == 1)
        .select(
            f.col("_observ_end_dt"),
            f.col("fecha_transaccion"),
            f.col("id_buro_externo"),
            f.col("id_categoria_riesgo_deuda").alias(f"{type}_id_peor_categoria_deuda"),
            f.col("descripcion_categoria_riesgo_deuda").alias(
                f"{type}_descripcion_peor_categoria_deuda"
            ),
        )
    )

    return df_worst__debtcategory


@arrange_columns(
    p_start_cols=[
        "_observ_end_dt",
        "fecha_transaccion",
        "id_buro_externo",
        "id_numero_solicitud",
        "id_cliente_bi",
        "buro_referencia",
    ]
)
@task(name="transform_debt_history_task", tags=["data transformation", "processing"])
def transform_debt_history_task(
    p_cleaned_external_bureau: DataFrame,
    p_cleaned_eql_person_request: DataFrame,
    p_cleaned_eql_person: DataFrame,
    p_cleaned_debt_detail: DataFrame,
    p_cleaned_eql_sib_debt_history: DataFrame,
    p_cleaned_customer: DataFrame,
) -> DataFrame:
    """
    Transforms and processes information to create features.

    Args:
        p_cleaned_external_bureau (DataFrame): Cleaned external bureau.
        p_cleaned_eql_person_request (DataFrame): Cleaned person request.
        p_cleaned_eql_person (DataFrame): Cleaned person.
        p_cleaned_debt_detail (DataFrame): Cleaned debt detail.
        p_cleaned_eql_sib_debt_history (DataFrame): Cleaned debt history.
        p_cleaned_customer (DataFrame): Cleaned customer.

    Returns:
        DataFrame: debt detail features DataFrame.
    """

    df_applicant = applicant_task(
        p_cleaned_eql_person_request, p_cleaned_eql_person, p_cleaned_customer
    )

    df_external_bureau = join_dataframes_task(
        ["id_numero_solicitud", "id_cliente_persona"],
        [df_applicant, p_cleaned_external_bureau],
        "inner",
    )

    df_main_sib = lookup_external_bureau_task(df_external_bureau, "sib")

    df_sib_applicant = join_dataframes_task(
        ["id_numero_solicitud", "id_cliente_bi"], [df_applicant, df_main_sib], "inner"
    )

    window_spec = Window.partitionBy(
        "_observ_end_dt",
        "fecha_transaccion",
        "id_buro_externo",
        "id_cliente_persona",
        "id_numero_solicitud",
        "tipo_deuda",
    ).orderBy(f.col("fecha_historial_deuda").desc())

    df_sib_debt_history = (
        p_cleaned_eql_sib_debt_history.alias("d")
        .join(
            df_sib_applicant.alias("a"),
            on=[
                "id_buro_externo",
                "_observ_end_dt",
                "fecha_transaccion",
                "id_cliente_persona",
                "id_numero_solicitud",
            ],
            how="inner",
        )
        .select(
            f.col("d.*"),
            f.col("a.id_cliente_bi"),
            f.row_number().over(window_spec).alias("rn"),
        )
        .filter(f.col("rn") <= 18)
    )

    df_worst_category_direct_debt = worst_debt_category_task(p_cleaned_debt_detail, "D")
    df_worst_category_indirect_debt = worst_debt_category_task(
        p_cleaned_debt_detail, "I"
    )

    df_sib_debt_history_final = join_dataframes_task(
        ["_observ_end_dt", "fecha_transaccion", "id_buro_externo"],
        [
            df_sib_debt_history,
            df_worst_category_direct_debt,
            df_worst_category_indirect_debt,
        ],
        "left",
    ).select(
        df_sib_debt_history["_observ_end_dt"],
        df_sib_debt_history["fecha_transaccion"],
        df_sib_debt_history["id_buro_externo"],
        df_sib_debt_history["id_numero_solicitud"],
        df_sib_debt_history["id_cliente_persona"],
        df_sib_debt_history["id_cliente_bi"],
        df_sib_debt_history["buro_referencia"],
        df_sib_debt_history["fecha_historial_deuda"],
        df_sib_debt_history["tipo_deuda"],
        df_sib_debt_history["descripcion_tipo_deuda"],
        f.when(
            (f.col("rn") == 1) & (f.col("tipo_deuda") == "D"),
            f.col("deuda_directa_id_peor_categoria_deuda"),
        )
        .when(
            (f.col("rn") == 1) & (f.col("tipo_deuda") == "I"),
            f.col("deuda_indirecta_id_peor_categoria_deuda"),
        )
        .otherwise(f.col("categoria_riesgo_deuda"))
        .alias("id_categoria_deuda"),
        f.when(
            (f.col("rn") == 1) & (f.col("tipo_deuda") == "D"),
            f.col("deuda_directa_descripcion_peor_categoria_deuda"),
        )
        .when(
            (f.col("rn") == 1) & (f.col("tipo_deuda") == "I"),
            f.col("deuda_indirecta_descripcion_peor_categoria_deuda"),
        )
        .otherwise(f.col("descripcion_categoria_riesgo_deuda"))
        .alias("descripcion_categoria_deuda"),
        df_sib_debt_history["total_deuda"],
        df_sib_debt_history["total_mora_intereses"],
        df_sib_debt_history["total_mora_capital"],
        df_sib_debt_history["max_dias_mora_capital"],
        df_sib_debt_history["max_dias_mora_intereses"],
    )

    return df_sib_debt_history_final


@flow(name="eql_sib_debt_history_flow")
def eql_sib_debt_history_flow():
    """
    Loads, transforms and saves debt history features in the data lake.

    The flow performs the following operations:
    1. Loads cleaned data using the specified date range.
    2. Transforms and processes the data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        debt history features data.
    """
    cleaned_data = load_raw_data_flow()

    df_debt_history = transform_debt_history_task(
        cleaned_data["cleaned_eql_external_bureau"],
        cleaned_data["cleaned_eql_person_request"],
        cleaned_data["cleaned_eql_person"],
        cleaned_data["cleaned_eql_sib_debt_detail"],
        cleaned_data["cleaned_eql_sib_debt_history"],
        cleaned_data["cleaned_customer"],
    )

    save_data_flow(df_debt_history)
