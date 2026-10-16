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


@task(name="unify_currency_task", tags=["data transformation", "processing"])
def unify_currency_task(
    p_df_tu_debt: DataFrame, p_cleaned_exchange_rate: DataFrame
) -> DataFrame:
    """
    Unifies various currencies in a field

    Args:
        p_df_sib_debt: sib debt
        p_cleaned_exchange_rate: Exchange rate

    Returns:
        DataFrame: Unified currency
    """
    df_unify_currency = (
        p_df_tu_debt.filter(
            ~(f.col("agrupacion_estado_deuda").isin("cancelado", "extraviada"))
        )
        .join(
            p_cleaned_exchange_rate,
            ["fecha_transaccion"],
            "left",
        )
        .select(
            p_df_tu_debt["*"],
            f.coalesce(
                f.when(
                    p_df_tu_debt["moneda"] == "usd",
                    p_df_tu_debt["monto_otorgado"]
                    * p_cleaned_exchange_rate["tasa_cambio"],
                ).otherwise(p_df_tu_debt["monto_otorgado"]),
                f.lit(0),
            ).alias("monto_otorgado_gtq"),
            f.coalesce(
                f.when(
                    p_df_tu_debt["moneda"] == "usd",
                    p_df_tu_debt["saldo"] * p_cleaned_exchange_rate["tasa_cambio"],
                ).otherwise(p_df_tu_debt["saldo"]),
                f.lit(0),
            ).alias("saldo_gtq"),
            f.coalesce(
                f.when(
                    p_df_tu_debt["moneda"] == "usd",
                    p_df_tu_debt["saldo_pendiente"]
                    * p_cleaned_exchange_rate["tasa_cambio"],
                ).otherwise(p_df_tu_debt["saldo_pendiente"]),
                f.lit(0),
            ).alias("saldo_pendiente_gtq"),
            f.coalesce(
                f.when(
                    p_df_tu_debt["moneda"] == "usd",
                    p_df_tu_debt["saldo_vencido_deuda"]
                    * p_cleaned_exchange_rate["tasa_cambio"],
                ).otherwise(p_df_tu_debt["saldo_vencido_deuda"]),
                f.lit(0),
            ).alias("saldo_vencido_deuda_gtq"),
            f.coalesce(
                f.when(
                    p_df_tu_debt["moneda"] == "usd",
                    p_df_tu_debt["cuota"] * p_cleaned_exchange_rate["tasa_cambio"],
                ).otherwise(p_df_tu_debt["cuota"]),
                f.lit(0),
            ).alias("cuota_gtq"),
        )
    )

    df_unify_currency_final = df_unify_currency.groupBy(
        f.col("_observ_end_dt"),
        f.col("id_buro_externo"),
        f.col("id_numero_solicitud"),
        f.col("id_cliente_persona"),
        f.col("id_cliente_bi"),
        f.col("id_entidad"),
        f.col("descripcion_entidad"),
        f.col("agrupacion_entidad"),
        f.col("fecha_transaccion"),
        f.col("buro_referencia"),
        f.col("id_tipo_deuda"),
        f.col("descripcion_tipo_deuda"),
        f.col("fecha_referencia").alias("fecha_referencia_deuda"),
        f.col("fecha_concesion"),
        f.col("entidad"),
        f.col("agrupacion_tipo_activo"),
    ).agg(
        f.count(
            f.when(
                f.col("agrupacion_tipo_activo") != "tc", f.col("agrupacion_tipo_activo")
            )
        ).alias("cnt_otros_productos"),
        f.first(
            f.when(f.col("moneda") == "gtq", f.col("agrupacion_estado_deuda")),
            ignorenulls=True,
        ).alias("agrupacion_estado_deuda_gtq"),
        f.first(
            f.when(f.col("moneda") == "usd", f.col("agrupacion_estado_deuda")),
            ignorenulls=True,
        ).alias("agrupacion_estado_deuda_usd"),
        f.first(
            f.when(
                (f.col("moneda") == "gtq") & (f.col("agrupacion_tipo_activo") == "tc"),
                f.col("monto_otorgado_gtq"),
            ),
            ignorenulls=True,
        ).alias("tc_monto_otorgado_gtq"),
        f.first(
            f.when(
                (f.col("moneda") == "usd") & (f.col("agrupacion_tipo_activo") == "tc"),
                f.col("monto_otorgado_gtq"),
            ),
            ignorenulls=True,
        ).alias("tc_monto_otorgado_usd_gtq"),
        f.sum(f.col("monto_otorgado_gtq")).alias("otros_monto_otorgado"),
        f.sum("saldo_gtq").alias("saldo_gtq"),
        f.sum("saldo_pendiente_gtq").alias("saldo_pendiente_gtq"),
        f.sum("saldo_vencido_deuda_gtq").alias("saldo_vencido_deuda_gtq"),
        f.sum("cuota_gtq").alias("cuota_gtq"),
        f.max("flag_mora_1_30_dias").alias("max_flag_mora_1_30_dias"),
        f.max("flag_mora_31_60_dias").alias("max_flag_mora_31_60_dias"),
        f.max("flag_mora_61_90_dias").alias("max_flag_mora_61_90_dias"),
        f.max("flag_mora_91_120_dias").alias("max_flag_mora_91_120_dias"),
        f.max("flag_mora_121_o_mas_dias").alias("max_flag_mora_121_o_mas_dias"),
        f.max("id_estado_deuda").alias("id_estado_deuda"),
    )

    return df_unify_currency_final


@arrange_columns(
    p_start_cols=[
        "_observ_end_dt",
        "fecha_transaccion",
        "buro_referencia",
        "id_buro_externo",
        "id_numero_solicitud" "id_cliente_persona",
        "id_cliente_bi",
    ]
)
@task(name="transform_debt_detail_task", tags=["data transformation", "processing"])
def transform_debt_detail_task(
    p_cleaned_external_bureau: DataFrame,
    p_cleaned_eql_person_request: DataFrame,
    p_cleaned_eql_person: DataFrame,
    p_cleaned_debt_detail: DataFrame,
    p_cleaned_exchange_rate: DataFrame,
    p_cleaned_customer: DataFrame,
) -> DataFrame:
    """
    Transforms and processes information to create features.

    Args:
        p_cleaned_external_bureau (DataFrame): Cleaned external bureau.
        p_cleaned_eql_person_request (DataFrame): Cleaned person request.
        p_cleaned_eql_person (DataFrame): Cleaned person.
        p_cleaned_debt_detail (DataFrame): Cleaned debt detail.
        p_cleaned_exchange_rate (DataFrame) : Cleaned exchange rate.
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

    df_main_tu = lookup_external_bureau_task(df_external_bureau, "tu")

    df_tu_applicant = join_dataframes_task(
        ["id_numero_solicitud", "id_cliente_bi"], [df_applicant, df_main_tu], "inner"
    )

    df_tu_debt = (
        p_cleaned_debt_detail.alias("d")
        .join(df_tu_applicant.alias("a"), on=["id_buro_externo"], how="inner")
        .select(
            f.col("d.*"),
            f.concat_ws(
                "_", f.col("descripcion_tipo_deuda"), f.col("agrupacion_tipo_activo")
            ).alias("pivot_debts"),
            f.col("a.id_entidad"),
            f.col("a.descripcion_entidad"),
            f.col("a.agrupacion_entidad"),
            f.col("a.id_cliente_bi"),
        )
    )

    window_spec = Window.partitionBy(
        "_observ_end_dt",
        "id_buro_externo",
        "id_numero_solicitud",
        "id_cliente_persona",
        "id_cliente_bi",
        "id_entidad",
        "fecha_transaccion",
        "id_tipo_deuda",
        "agrupacion_tipo_activo",
        "fecha_referencia_deuda",
        "entidad",
        "tc_monto_otorgado_gtq",
        "tc_monto_otorgado_usd_gtq",
        "agrupacion_estado_deuda_gtq",
        "agrupacion_estado_deuda_usd",
    ).orderBy(f.col("id_buro_externo").desc())

    df_tu_unify_currency = unify_currency_task(
        df_tu_debt, p_cleaned_exchange_rate
    ).withColumn("rn", f.row_number().over(window_spec))

    df_tu_debt_final = df_tu_unify_currency.groupBy(
        f.col("_observ_end_dt"),
        f.col("id_buro_externo"),
        f.col("id_numero_solicitud"),
        f.col("id_cliente_persona"),
        f.col("id_cliente_bi"),
        f.col("id_entidad"),
        f.col("descripcion_entidad"),
        f.col("agrupacion_entidad"),
        f.col("fecha_transaccion"),
        f.col("buro_referencia"),
        f.col("id_tipo_deuda"),
        f.col("descripcion_tipo_deuda"),
        f.when(
            f.col("agrupacion_estado_deuda_gtq").isNull(),
            f.col("agrupacion_estado_deuda_usd"),
        )
        .when(
            f.col("agrupacion_estado_deuda_usd").isNull(),
            f.col("agrupacion_estado_deuda_gtq"),
        )
        .when(
            f.col("agrupacion_estado_deuda_gtq")
            == f.col("agrupacion_estado_deuda_usd"),
            f.col("agrupacion_estado_deuda_gtq"),
        )
        .when(
            f.col("agrupacion_estado_deuda_gtq") == f.lit("no_registra"),
            f.col("agrupacion_estado_deuda_usd"),
        )
        .when(
            f.col("agrupacion_estado_deuda_usd") == f.lit("no_registra"),
            f.col("agrupacion_estado_deuda_gtq"),
        )
        .when(
            f.col("agrupacion_estado_deuda_gtq") == f.lit("vigente"),
            f.col("agrupacion_estado_deuda_usd"),
        )
        .when(
            f.col("agrupacion_estado_deuda_usd") == f.lit("vigente"),
            f.col("agrupacion_estado_deuda_gtq"),
        )
        .when(
            f.col("agrupacion_estado_deuda_gtq") == f.lit("irrecuperable"),
            f.col("agrupacion_estado_deuda_gtq"),
        )
        .when(
            f.col("agrupacion_estado_deuda_usd") == f.lit("irrecuperable"),
            f.col("agrupacion_estado_deuda_usd"),
        )
        .when(
            f.col("agrupacion_estado_deuda_gtq") == f.lit("juridico"),
            f.col("agrupacion_estado_deuda_gtq"),
        )
        .when(
            f.col("agrupacion_estado_deuda_usd") == f.lit("juridico"),
            f.col("agrupacion_estado_deuda_usd"),
        )
        .when(
            f.col("agrupacion_estado_deuda_gtq") == f.lit("cobro_administrativo"),
            f.col("agrupacion_estado_deuda_gtq"),
        )
        .when(
            f.col("agrupacion_estado_deuda_usd") == f.lit("cobro_administrativo"),
            f.col("agrupacion_estado_deuda_usd"),
        )
        .when(
            f.col("agrupacion_estado_deuda_gtq")
            == f.lit("refinanciada_reestructurada_o_convenio"),
            f.col("agrupacion_estado_deuda_gtq"),
        )
        .when(
            f.col("agrupacion_estado_deuda_usd")
            == f.lit("refinanciada_reestructurada_o_convenio"),
            f.col("agrupacion_estado_deuda_usd"),
        )
        .when(
            f.col("agrupacion_estado_deuda_gtq") == f.lit("mora"),
            f.col("agrupacion_estado_deuda_gtq"),
        )
        .when(
            f.col("agrupacion_estado_deuda_usd") == f.lit("mora"),
            f.col("agrupacion_estado_deuda_usd"),
        )
        .alias("peor_estado_deuda_descripcion"),
        f.col("fecha_referencia_deuda"),
        f.col("entidad"),
        f.col("agrupacion_tipo_activo"),
    ).agg(
        f.sum(
            (
                f.when(
                    (f.col("rn") == 1)
                    & (~f.col("tc_monto_otorgado_gtq").isNull())
                    & (f.col("agrupacion_tipo_activo") == "tc"),
                    f.col("tc_monto_otorgado_gtq"),
                )
            )
            .when(
                (f.col("rn") == 1)
                & (f.col("tc_monto_otorgado_gtq").isNull())
                & (f.col("agrupacion_tipo_activo") == "tc"),
                f.col("tc_monto_otorgado_usd_gtq"),
            )
            .when(
                (f.col("agrupacion_tipo_activo") != "tc"), f.col("otros_monto_otorgado")
            )
        ).alias("monto_otorgado_gtq"),
        f.sum(
            f.when(f.col("agrupacion_tipo_activo") == "tc", f.lit(1)).otherwise(
                f.col("cnt_otros_productos")
            )
        ).alias("conteo_productos"),
        f.sum("saldo_gtq").alias("saldo_gtq"),
        f.sum("saldo_pendiente_gtq").alias("saldo_pendiente_gtq"),
        f.sum("saldo_vencido_deuda_gtq").alias("saldo_vencido_deuda_gtq"),
        f.sum("cuota_gtq").alias("cuota_gtq"),
        f.max("max_flag_mora_1_30_dias").alias("max_flag_mora_1_30_dias"),
        f.max("max_flag_mora_31_60_dias").alias("max_flag_mora_31_60_dias"),
        f.max("max_flag_mora_61_90_dias").alias("max_flag_mora_61_90_dias"),
        f.max("max_flag_mora_91_120_dias").alias("max_flag_mora_91_120_dias"),
        f.max("max_flag_mora_121_o_mas_dias").alias("max_flag_mora_121_o_mas_dias"),
    )

    return df_tu_debt_final


@flow(name="eql_sib_debt_detail_flow")
def eql_tu_debt_detail_flow():
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
        cleaned_data["cleaned_eql_tu_debt_detail"],
        cleaned_data["cleaned_exchange_rate"],
        cleaned_data["cleaned_customer"],
    )

    save_data_flow(df_debt_detail)
