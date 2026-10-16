import pyspark.sql.functions as f  # type: ignore
from prefect import flow, task  # type: ignore
from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql.window import Window

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_integration.src.utils.utils import (
    add_suffix_to_columns_task,
    join_dataframes_task,
)


@task(name="visa_cc_category_feature_task", tags=["data integration", "processing"])
def visa_cc_category_feature_task(p_transformed_credit_card: DataFrame) -> DataFrame:
    """visa cc category for customer

    This task processes the credit card features, and based on a
    weighted criteria, it returns a single category per customer.

    Args:
        p_transformed_credit_card (DataFrame): Transformed credit card data.

    Returns:
        DataFrame: Unified and processed DataFrame.
    """
    visa_category_window = Window.partitionBy(
        f.col("id_cliente"), f.col("_observ_end_dt")
    ).orderBy(f.col("tc_id_categoria").desc(), f.col("tc_flag_no_renovacion").desc())

    df_upgrades = (
        p_transformed_credit_card.filter(f.col("tc_emisor") == "visa")
        .withColumn("max_category", f.row_number().over(visa_category_window))
        .filter(f.col("max_category") == 1)
    )

    df_visa_category = df_upgrades.groupBy(
        f.col("id_cliente"), f.col("_observ_end_dt")
    ).agg(f.last(f.col("tc_categoria")).alias("categoria_actual_visa_tipo"))

    return df_visa_category


@task(
    name="past_due_currencies_cc_features_task", tags=["data integration", "processing"]
)
def past_due_currencies_cc_features_task(
    p_transformed_credit_card: DataFrame,
) -> DataFrame:
    """Past Due cards features data

    This task processes the past due cc, and creates usd and gtq
    customer based features.

    Args:
        p_transformed_credit_card (DataFrame): Transformed credit card data.

    Returns:
        Tuple: Unified and processed tuple of DataFrames.
    """
    df_past_due = {}
    currency_list = ["gtq", "usd"]
    for currency in currency_list:
        v_pivot_col = f"tc_dias_mora_{currency}"

        if currency == "gtq":
            sum_mapping = {
                "tc_dias_mora_gtq": "dias_mora_gtq_cnt",
                "tc_saldo_capital_gtq_val": "dias_mora_saldo_capital_gtq",
                "tc_limite_gtq_val": "dias_mora_limite_gtq",
            }

        else:
            sum_mapping = {
                "tc_dias_mora_usd": "dias_mora_usd_cnt",
                "tc_saldo_capital_usd_val": "dias_mora_saldo_capital_usd",
            }

        df_past_due[currency] = (
            p_transformed_credit_card.filter(f.col(v_pivot_col) <= 180)
            .groupBy(f.col("id_cliente"), f.col("_observ_end_dt"))
            .pivot(v_pivot_col)
            .agg(*[f.sum(col).alias(alias) for col, alias in sum_mapping.items()])
        )
    return (df_past_due["gtq"], df_past_due["usd"])


@task(name="cc_default_cycles_task", tags=["data integration", "processing"])
def cc_default_cycles_task(
    p_transformed_credit_card: DataFrame,
) -> DataFrame:
    """Default cycles data

    This task processes the default cycles cc features.

    Args:
        p_transformed_credit_card (DataFrame): Transformed credit card data.

    Returns:
        DataFrame: Unified and processed DataFrame.
    """
    default_cycles = p_transformed_credit_card.filter(
        f.col("tc_max_ciclo_mora") > 0
    ).withColumn(
        "default_cycles",
        f.when(
            f.col("tc_max_ciclo_mora") <= 7,
            f.concat(f.lit("mora_"), (f.col("tc_max_ciclo_mora") * 30).cast("string")),
        ).otherwise("mora_superior_210"),
    )
    default_cycles = (
        default_cycles.groupBy(f.col("_observ_end_dt"), f.col("id_cliente"))
        .pivot("default_cycles")
        .agg(
            f.sum(f.col("tc_mayor_saldo_mora_quetzalizado_val")).alias(
                "mayor_saldo_mora_quetzalizado"
            ),
            f.sum(f.col("tc_saldo_mora_quetzalizado_val")).alias(
                "saldo_mora_quetzalizado"
            ),
        )
        .fillna(0)
    )

    return default_cycles


@task(name="additional_cc_features_task", tags=["data integration", "processing"])
def additional_cc_features_task(
    p_transformed_credit_card: DataFrame,
    p_transformed_additional_credit_card: DataFrame,
) -> DataFrame:
    """Additional cards features data

    This task processes the additional cards, creating customer
    based features.

    Args:
        p_transformed_credit_card (DataFrame): Transformed credit card data.
        p_transformed_additional_credit_card (DataFrame): Transformed additional credit
        card data.

    Returns:
        Tuple: Unified and processed tuple of DataFrames.
    """

    valid_ac_list = ["vigente", "bloqueada", "juridica", "sobregirada"]
    df_valid_ac = p_transformed_additional_credit_card.filter(
        f.col("tc_situacion_adicional").isin(valid_ac_list)
    )

    df_additonal_cards_base = df_valid_ac.select(
        f.col("_observ_end_dt"),
        f.col("tc_cuenta_corporativa_principal").alias("cuenta_corporativa"),
        f.col("tc_situacion_adicional").alias("situacion_adicional"),
    )

    no_additional_window = Window.partitionBy(
        f.col("id_cliente"), f.col("_observ_end_dt")
    ).orderBy(f.col("tc_id_categoria").desc())

    df_no_additional = (
        p_transformed_credit_card.join(
            df_additonal_cards_base.dropDuplicates(
                ["_observ_end_dt", "cuenta_corporativa"]
            ),
            ["_observ_end_dt", "cuenta_corporativa"],
            "left_anti",
        )
        .filter(f.col("tc_emisor") == "visa")
        .withColumn("max_category", f.row_number().over(no_additional_window))
        .filter(f.col("max_category") == 1)
    )

    df_visa_category_no_additional = df_no_additional.groupBy(
        f.col("id_cliente"), f.col("_observ_end_dt")
    ).agg(f.last(f.col("tc_categoria_estandard")).alias("categoria_visa_sin_ac_tipo"))

    df_additional_active = (
        df_additonal_cards_base.join(
            p_transformed_credit_card.dropDuplicates(
                ["_observ_end_dt", "cuenta_corporativa"]
            ),
            ["_observ_end_dt", "cuenta_corporativa"],
            "inner",
        )
        .groupBy(f.col("id_cliente"), f.col("_observ_end_dt"))
        .agg(f.count(f.col("*")).alias("tarjetas_adicionales_validas_cnt"))
        .select(
            f.col("_observ_end_dt"),
            f.col("id_cliente"),
            f.col("tarjetas_adicionales_validas_cnt"),
        )
    )
    return (df_visa_category_no_additional, df_additional_active)


@task(
    name="calculates_active_financing_metrics_task",
    tags=["data transformation", "processing"],
)
def calculates_active_financing_metrics_task(p_credit_card: DataFrame) -> DataFrame:
    """
    Aggregates active financing metrics
    Args:
        p_credit_card (DataFrame): Credit Card

    Returns:
        DataFrame: A Dataframe containing active financing metrics.
    """

    df_active_financing_metrics = (
        p_credit_card.fillna(0)
        .groupBy(f.col("id_cliente"), f.col("_observ_end_dt"))
        .agg(
            f.sum(f.col("tc_financiamientos_activos_cnt")).alias(
                "financiamientos_activos_cnt"
            ),
            f.sum(
                f.col("tc_total_monto_otorgado_financiamiento_activo_quetzalizado_val")
            ).alias("total_monto_otorgado_financiamiento_activo_quetzalizado"),
            f.sum(
                f.col("tc_total_saldo_pendiente_financiamiento_activo_quetzalizado_val")
            ).alias("total_saldo_pendiente_financiamiento_activo_quetzalizado"),
            f.sum(
                f.col("tc_total_cuota_mensual_financiamiento_activo_quetzalizado_val")
            ).alias("total_cuota_mensual_financiamiento_activo_quetzalizado"),
            f.sum(
                f.col("tc_convenio_de_pago_monto_vigente_otorgado_quetzalizado_val")
            ).alias("convenio_de_pago_monto_vigente_otorgado_quetzalizado"),
            f.sum(
                f.col("tc_convenio_de_pago_monto_otorgado_mes_actual_quetzalizado_val")
            ).alias("convenio_de_pago_monto_otorgado_mes_actual_quetzalizado"),
            f.sum(f.col("tc_convenio_de_pago_saldo_pendiente_quetzalizado_val")).alias(
                "convenio_de_pago_saldo_pendiente_quetzalizado"
            ),
            f.sum(
                f.col("tc_convenio_de_pago_cuota_mensual_vigente_quetzalizado_val")
            ).alias("convenio_de_pago_cuota_mensual_vigente_quetzalizado"),
            f.sum(
                f.col("tc_extrafinanciamiento_monto_vigente_otorgado_quetzalizado_val")
            ).alias("extrafinanciamiento_monto_vigente_otorgado_quetzalizado"),
            f.sum(
                f.col(
                    "tc_extrafinanciamiento_monto_otorgado_mes_actual_quetzalizado_val"
                )
            ).alias("extrafinanciamiento_monto_otorgado_mes_actual_quetzalizado"),
            f.sum(
                f.col("tc_extrafinanciamiento_saldo_pendiente_quetzalizado_val")
            ).alias("extrafinanciamiento_saldo_pendiente_quetzalizado"),
            f.sum(
                f.col("tc_extrafinanciamiento_cuota_mensual_vigente_quetzalizado_val")
            ).alias("extrafinanciamiento_cuota_mensual_vigente_quetzalizado"),
            f.sum(f.col("tc_neocuotas_monto_vigente_otorgado_quetzalizado_val")).alias(
                "neocuotas_monto_vigente_otorgado_quetzalizado"
            ),
            f.sum(
                f.col("tc_neocuotas_monto_otorgado_mes_actual_quetzalizado_val")
            ).alias("neocuotas_monto_otorgado_mes_actual_quetzalizado"),
            f.sum(f.col("tc_neocuotas_saldo_pendiente_quetzalizado_val")).alias(
                "neocuotas_saldo_pendiente_quetzalizado"
            ),
            f.sum(f.col("tc_neocuotas_cuota_mensual_vigente_quetzalizado_val")).alias(
                "neocuotas_cuota_mensual_vigente_quetzalizado"
            ),
            f.sum(
                f.col("tc_traslado_a_cuotas_monto_vigente_otorgado_quetzalizado_val")
            ).alias("traslado_a_cuotas_monto_vigente_otorgado_quetzalizado"),
            f.sum(
                f.col("tc_traslado_a_cuotas_monto_otorgado_mes_actual_quetzalizado_val")
            ).alias("traslado_a_cuotas_monto_otorgado_mes_actual_quetzalizado"),
            f.sum(f.col("tc_traslado_a_cuotas_saldo_pendiente_quetzalizado_val")).alias(
                "traslado_a_cuotas_saldo_pendiente_quetzalizado"
            ),
            f.sum(
                f.col("tc_traslado_a_cuotas_cuota_mensual_vigente_quetzalizado_val")
            ).alias("traslado_a_cuotas_cuota_mensual_vigente_quetzalizado"),
            f.sum(
                f.col("tc_unificacion_saldos_monto_vigente_otorgado_quetzalizado_val")
            ).alias("unificacion_saldos_monto_vigente_otorgado_quetzalizado"),
            f.sum(
                f.col(
                    "tc_unificacion_saldos_monto_otorgado_mes_actual_quetzalizado_val"
                )
            ).alias("unificacion_saldos_monto_otorgado_mes_actual_quetzalizado"),
            f.sum(
                f.col("tc_unificacion_saldos_saldo_pendiente_quetzalizado_val")
            ).alias("unificacion_saldos_saldo_pendiente_quetzalizado"),
            f.sum(
                f.col("tc_unificacion_saldos_cuota_mensual_vigente_quetzalizado_val")
            ).alias("unificacion_saldos_cuota_mensual_vigente_quetzalizado"),
        )
    )

    return df_active_financing_metrics


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente"])
@task(name="mc_credit_card_task", tags=["data integration", "processing"])
def mc_credit_card_task(
    p_transformed_credit_card: DataFrame,
    p_transformed_additional_credit_card: DataFrame,
    p_transformed_autoriza: DataFrame,
) -> DataFrame:
    """Processes and integrates customer data.

    This task integrates the resulting data from the Data Transformation Layer,
    using features from credit card, additional credit card, and autoriza.

    Args:
        p_transformed_credit_card (DataFrame): Transformed credit card data.
        p_transformed_additional_credit_card (DataFrame): Transformed additional credit
        card data.
        p_transformed_autoriza (DataFrame): Transformed POS transactions data.

    Returns:
        DataFrame: Unified and processed Dataframe.
    """

    df_credit_card = p_transformed_credit_card.filter(f.col("tc_bi_online_flag") != 1)

    df_active_financing_metrics = calculates_active_financing_metrics_task(
        df_credit_card
    )

    df_transformed_autoriza = p_transformed_autoriza.filter(
        f.col("au_id_aplicacion") == 4
    ).join(df_credit_card, ["_observ_end_dt", "id_cliente", "cuenta_corporativa"])

    valid_cc_list = [
        "bloqueada",
        "con_problema",
        "juridico",
        "en_mora",
        "sobregirada",
        "vigente",
    ]
    df_valid_credit_card = df_credit_card.filter(
        f.col("tc_situacion_cuenta_homologado").isin(valid_cc_list)
    )

    df_au_processed = df_transformed_autoriza.groupBy(
        f.col("id_cliente"), f.col("_observ_end_dt")
    ).agg(
        f.sum(f.col("au_atm_retiros_total_transacciones_cnt")).alias(
            "retiros_atm_bi_cnt"
        ),
        f.sum(f.col("au_restaurantes_cnt")).alias("restaurantes_transacciones_cnt"),
        f.sum(f.col("au_comida_rapida_cnt")).alias("comida_rapida_transacciones_cnt"),
        f.sum(f.col("au_supermercados_y_alimentos_cnt")).alias(
            "supermercados_y_alimentos_transacciones_cnt"
        ),
        f.sum(f.col("au_monto_total_transacciones_val")).alias(
            "monto_transacciones_sum"
        ),
        f.sum(f.col("au_total_transacciones_cnt")).alias("transacciones_cnt"),
        f.sum(f.col("au_atm_retiros_monto_total_transacciones_val")).alias(
            "monto_transacciones_retiros_atm_sum"
        ),
        f.sum(f.col("au_atm_retiros_total_transacciones_cnt")).alias(
            "transacciones_retiros_atm_cnt"
        ),
        (
            f.sum(f.col("au_consumos_monto_total_transacciones_val"))
            / f.sum(f.col("au_consumos_total_transacciones_cnt"))
        ).alias("monto_promedio_transaccion"),
        f.sum(f.col("au_total_transacciones_gtq_cnt")).alias("transacciones_gtq_cnt"),
        f.sum(f.col("au_total_transacciones_usd_cnt")).alias("transacciones_usd_cnt"),
        f.sum(f.col("au_consumos_monto_total_transacciones_val")).alias(
            "monto_compras_sin_cuotas_mensuales"
        ),
        f.sum(f.col("au_consumos_total_transacciones_cnt")).alias(
            "compras_sin_cuotas_mensuales_cnt"
        ),
    )

    df_cc_mc_full = (
        df_credit_card.groupBy(
            f.col("id_cliente"),
            f.col("_observ_end_dt"),
        )
        .agg(
            f.min(f.col("tc_fecha_apertura")).alias("fecha_min_apertura"),
            f.max(f.col("tc_empresarial_flag")).alias("tarjetas_empresariales_flag"),
            f.sum(f.col("tc_empresarial_flag")).alias("tarjetas_empresariales_cnt"),
        )
        .withColumn(
            "fecha_apertura_2018_flag",
            f.when(f.col("fecha_min_apertura") >= "2018-06-01", 0).otherwise(1),
        )
    )

    df_valid_cc = (
        df_valid_credit_card.groupBy(f.col("id_cliente"), f.col("_observ_end_dt"))
        .agg(
            f.sum(f.col("tc_saldo_capital_quetzalizado_val")).alias(
                "monto_pago_transacciones_sum"
            ),
            f.sum(f.col("tc_saldo_capital_quetzalizado_val")).alias(
                "saldo_capital_sum"
            ),
            f.sum(f.col("tc_limite_gtq_val")).alias("limite_total_gtq_sum"),
            f.max(f.col("tc_mora30D_flag")).alias("flag_mora_tarjeta_credito_30d"),
            f.max(f.col("tc_mora60D_flag")).alias("flag_mora_tarjeta_credito_60d"),
            f.max(f.col("tc_max_dias_mora_capital_e83")).alias(
                "max_dias_mora_capital_e83"
            ),
            f.max(f.col("tc_max_dias_mora_intereses_e83")).alias(
                "max_dias_mora_intereses_e83"
            ),
            f.max(f.col("tc_max_ciclo_mora")).alias("max_ciclo_mora"),
            f.max(f.col("tc_ciclo_mora_gtq")).alias("max_ciclo_mora_gtq"),
            f.max(f.col("tc_ciclo_mora_usd")).alias("max_ciclo_mora_usd"),
        )
        .withColumn(
            "ratio_utilizacion",
            f.col("saldo_capital_sum") / f.col("limite_total_gtq_sum"),
        )
    )

    past_due_cc_window = Window.partitionBy(
        f.col("id_cliente"), f.col("_observ_end_dt")
    ).orderBy(
        f.col("tc_mora60D_flag").desc(),
        f.col("tc_fecha_apertura").cast("date").desc(),
        f.col("tc_flag_no_renovacion").desc(),
    )

    df_past_due_credit_card = (
        df_credit_card.withColumn("_rn", f.row_number().over(past_due_cc_window))
        .filter(f.col("_rn") == 1)
        .drop("_rn")
        .groupBy(f.col("id_cliente"), f.col("_observ_end_dt"))
        .agg(f.last(f.col("tc_fecha_apertura")).alias("fecha_mora_tarjeta_credito"))
    )

    df_visa_category = visa_cc_category_feature_task(df_credit_card)

    df_situations = (
        df_credit_card.groupBy(f.col("id_cliente"), f.col("_observ_end_dt"))
        .pivot("tc_descripcion_situacion_cuenta")
        .agg(f.count(f.col("tc_descripcion_situacion_cuenta")))
    ).withColumn(
        "total_tarjetas",
        (
            f.coalesce(f.col("vigente"), f.lit(0))
            + f.coalesce(f.col("sobregirada"), f.lit(0))
            + f.coalesce(f.col("moroso"), f.lit(0))
            + f.coalesce(f.col("juridica"), f.lit(0))
            + f.coalesce(f.col("con_problema"), f.lit(0))
            + f.coalesce(f.col("bloqueada"), f.lit(0))
        ),
    )

    df_situations = add_suffix_to_columns_task(
        df_situations, "_cnt", ["_observ_end_dt", "id_cliente"]
    )

    df_past_due_currencies_cc = past_due_currencies_cc_features_task(
        df_valid_credit_card
    )

    df_additional_cards = additional_cc_features_task(
        df_credit_card, p_transformed_additional_credit_card
    )

    df_default_cycles = cc_default_cycles_task(df_valid_credit_card)

    return join_dataframes_task(
        ["id_cliente", "_observ_end_dt"],
        [
            df_cc_mc_full,
            df_valid_cc,
            df_au_processed,
            df_past_due_credit_card,
            df_visa_category,
            df_situations,
            *df_past_due_currencies_cc,
            *df_additional_cards,
            df_default_cycles,
            df_active_financing_metrics,
        ],
    )


@flow(name="mc_credit_card_flow")
def mc_credit_card_flow():
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

    df_master_credit_card = mc_credit_card_task(
        data_transformation["transformed_credit_card"],
        data_transformation["transformed_additional_credit_card"],
        data_transformation["transformed_autoriza"],
    )

    save_data_flow(df_master_credit_card)
