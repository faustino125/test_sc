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


@task(name="sib_debt_history_task", tags=["data transformation", "processing"])
def sib_debt_history_task(
    p_df_sib_debt_his: DataFrame, p_df_liabilities: DataFrame, p_type_debt: str
) -> DataFrame:
    """
    Returns the worst debt category

    Args:
        p_df_sib_debt_hib: sib debt history
        p_type_debt: type debt

    Returns:
        DataFrame: date of the debt
    """

    type = type_debt_task(p_type_debt)

    window_spec = Window.partitionBy("_observ_end_dt", "id_buro_externo").orderBy(
        f.col("fecha_historial_deuda").desc()
    )

    if type == "deuda_directa":
        cnt_cols = [c for c in p_df_liabilities.columns if c.endswith("_cnt")]
        p_df_liabilities = p_df_liabilities.fillna(0, subset=cnt_cols).withColumn(
            "deuda_directa_cnt", sum(f.col(c) for c in cnt_cols)
        )

    df_sib_debt_hist = p_df_sib_debt_his.join(
        p_df_liabilities, on=["id_buro_externo", "_observ_end_dt"], how="left"
    ).select(
        p_df_sib_debt_his["*"],
        f.col(f"{type}_cnt"),
    )

    df_debt_history = (
        df_sib_debt_hist.filter(f.col("tipo_deuda") == p_type_debt)
        .withColumn("rn", f.row_number().over(window_spec))
        .filter(f.col("rn") == 1)
        .select(
            f.col("id_buro_externo"),
            f.col("_observ_end_dt"),
            f.col("fecha_transaccion"),
            f.when(f.col(f"{type}_cnt") > 0, f.col("fecha_historial_deuda")).alias(
                f"{type}_fecha"
            ),
            f.when(f.col(f"{type}_cnt") > 0, f.col("total_mora_capital")).alias(
                f"{type}_total_mora_capital_gtq"
            ),
            f.when(f.col(f"{type}_cnt") > 0, f.col("total_mora_intereses")).alias(
                f"{type}_total_mora_intereses_gtq"
            ),
            f.when(f.col(f"{type}_cnt") > 0, f.col("max_dias_mora_capital")).alias(
                f"{type}_max_dias_mora_capital"
            ),
            f.when(f.col(f"{type}_cnt") > 0, f.col("max_dias_mora_intereses")).alias(
                f"{type}_max_dias_mora_intereses"
            ),
        )
    )

    return df_debt_history


@arrange_columns(
    p_start_cols=[
        "_observ_end_dt",
        "fecha_transaccion",
        "id_buro_externo",
        "id_numero_solicitud",
        "id_cliente_bi",
    ]
)
@task(name="transform_debt_detail_task", tags=["data transformation", "processing"])
def transform_debt_detail_task(
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

    df_sib_debt = (
        p_cleaned_debt_detail.alias("d")
        .join(df_sib_applicant.alias("a"), on=["id_buro_externo"], how="inner")
        .select(
            f.col("d.*"),
            f.concat_ws(
                "_", f.col("descripcion_tipo_deuda"), f.col("agrupacion_tipo_activo")
            ).alias("pivot_direct_debts"),
            f.when(f.col("agrupacion_tipo_activo").isin("tc", "tc_factorada"), 1)
            .otherwise(0)
            .alias("tc_flag"),
            f.col("a.id_entidad"),
            f.col("a.descripcion_entidad"),
            f.col("a.agrupacion_entidad"),
            f.col("a.id_cliente_bi"),
        )
    )

    df_direct_liabilities = liabilities_task(df_sib_debt, "D", "pivot_direct_debts")
    df_indirect_liabilities = liabilities_task(
        df_sib_debt, "I", "descripcion_tipo_deuda"
    )
    df_worst_category_direct_debt = worst_debt_category_task(df_sib_debt, "D")
    df_worst_category_indirect_debt = worst_debt_category_task(df_sib_debt, "I")

    df_debt_date = join_dataframes_task(
        [
            "id_numero_solicitud",
            "id_cliente_persona",
            "_observ_end_dt",
            "fecha_transaccion",
            "id_buro_externo",
        ],
        [df_sib_applicant, p_cleaned_eql_sib_debt_history],
        "left",
    )

    df_direct_debt_date_final = sib_debt_history_task(
        df_debt_date, df_direct_liabilities, "D"
    )
    df_indirect_debt_date_final = sib_debt_history_task(
        df_debt_date, df_indirect_liabilities, "I"
    )

    df_debt_keys = df_sib_debt.select("id_buro_externo", "_observ_end_dt").distinct()

    df_sib_applicant = (
        df_sib_applicant.alias("a")
        .join(
            df_debt_keys.alias("d"),
            on=[
                "id_buro_externo",
                "_observ_end_dt",
            ],
            how="inner",
        )
        .select(f.col("a.*"))
    )

    df_sib_debt_final = join_dataframes_task(
        ["_observ_end_dt", "fecha_transaccion", "id_buro_externo"],
        [
            df_sib_applicant,
            df_direct_debt_date_final,
            df_worst_category_direct_debt,
            df_direct_liabilities,
            df_indirect_debt_date_final,
            df_worst_category_indirect_debt,
            df_indirect_liabilities,
        ],
        "left",
    )

    return df_sib_debt_final


@flow(name="eql_sib_debt_detail_flow")
def eql_sib_debt_detail_flow():
    """
    Loads, transforms and saves debt detail features in the data lake.

    The flow performs the following operations:
    1. Loads cleaned data using the specified date range.
    2. Transforms and processes the data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        debt detail features data.
    """
    cleaned_data = load_raw_data_flow()

    df_debt_detail = transform_debt_detail_task(
        cleaned_data["cleaned_eql_external_bureau"],
        cleaned_data["cleaned_eql_person_request"],
        cleaned_data["cleaned_eql_person"],
        cleaned_data["cleaned_eql_sib_debt_detail"],
        cleaned_data["cleaned_eql_sib_debt_history"],
        cleaned_data["cleaned_customer"],
    )

    save_data_flow(df_debt_detail)
