from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_transformation.src.utils.utils import (
    add_exchange_rate_task,
    convert_currency_to_gtq_task,
    create_currency_col_task,
    join_dataframes_task,
    split_currency_task,
    unified_currency_col_task,
)


@task(
    name="process_credit_card_account_task",
    tags=["data transformation", "processing"],
)
def process_credit_card_account_task(
    p_cleaned_cc_account: DataFrame,
    p_cleaned_cc_updates: DataFrame,
    p_cleaned_daily_rate: DataFrame,
) -> DataFrame:
    """
    Transforms and processes credit cards account data.

    Args:
        p_cleaned_cc_account (DataFrame): Cleaned main credit card account data.
        p_cleaned_cc_updates (DataFrame): Cleaned credit card updates data.
        p_cleaned_daily_rate (DataFrame): Cleaned daily exchange data.

    Returns:
        DataFrame: Transformed main credit card account DataFrame.
    """
    daily_rate = p_cleaned_daily_rate.filter(
        f.col("fecha_transaccion") == f.col("_observ_end_dt")
    )

    account_df = (
        add_exchange_rate_task(p_cleaned_cc_account, daily_rate, True)
        .select(
            f.col("id_cliente"),
            f.col("cuenta_corporativa"),
            f.col("_observ_end_dt"),
            f.col("bi_online_flag"),
            f.col("segmento"),
            f.col("emisor"),
            f.col("id_categoria"),
            f.col("categoria"),
            f.col("categoria_estandard"),
            f.col("id_situacion_cuenta"),
            f.col("descripcion_situacion_cuenta"),
            f.col("situacion_cuenta_homologado"),
            f.col("limite_gtq"),
            f.col("fecha_corte"),
            f.col("saldo_al_corte_gtq"),
            f.col("saldo_al_corte_usd"),
            (
                f.col("saldo_al_corte_gtq")
                + (f.col("saldo_al_corte_usd") * f.col("tasa_cambio"))
            )
            .cast("float")
            .alias("saldo_al_corte_quetzalizado"),
            f.col("empresarial_flag"),
            f.col("institucional_flag"),
            f.col("flag_no_renovacion"),
            f.when(
                (
                    f.col("descripcion_producto").contains("especial")
                    & (f.col("categoria") == "classic")
                    & (f.col("grupo_situacion") == "activa")
                ),
                1,
            )
            .otherwise(0)
            .alias("activa_especial_flag"),
            f.col("dias_mora_gtq"),
            f.col("dias_mora_usd"),
            f.when(f.col("dias_mora") < 90, 0).otherwise(1).alias("mora90D_flag"),
            f.when(f.col("dias_mora") < 60, 0).otherwise(1).alias("mora60D_flag"),
            f.when(f.col("dias_mora") == 0, 0).otherwise(1).alias("mora30D_flag"),
            f.col("dias_mora").alias("max_dias_mora"),
            f.col("grupo_situacion"),
            f.col("fecha_apertura"),
            f.col("fecha_vencimiento_pago"),
            f.col("pagos_realizados_al_corte_gtq").alias("pago_gtq"),
            f.col("pagos_realizados_al_corte_usd").alias("pago_usd"),
            (
                (f.col("pagos_realizados_al_corte_usd") * f.col("tasa_cambio"))
                + f.col("pagos_realizados_al_corte_gtq")
            ).alias("pago_quetzalizado"),
            f.col("creditos_por_traslado_de_saldo_al_corte_gtq"),
            f.col("creditos_por_traslado_de_saldo_al_corte_usd"),
            (
                f.col("creditos_por_traslado_de_saldo_al_corte_gtq")
                + (
                    f.col("creditos_por_traslado_de_saldo_al_corte_usd")
                    * f.col("tasa_cambio")
                )
            )
            .cast("decimal(18,2)")
            .alias("creditos_por_traslado_de_saldo_al_corte_quetzalizado"),
            f.col("creditos_por_regularizacion_al_corte_gtq"),
            f.col("creditos_por_regularizacion_al_corte_usd"),
            (
                f.col("creditos_por_regularizacion_al_corte_gtq")
                + (
                    f.col("creditos_por_regularizacion_al_corte_usd")
                    * f.col("tasa_cambio")
                )
            )
            .cast("decimal(18,2)")
            .alias("creditos_por_regularizacion_al_corte_quetzalizado"),
            f.col("producto_mala_situacion_flag"),
            f.col("ciclo_mora_gtq"),
            f.col("ciclo_mora_usd"),
            f.col("ciclo_mora").alias("max_ciclo_mora"),
            f.when(f.col("ciclo_mora_gtq") > 0, f.col("saldo_al_dia_gtq"))
            .otherwise(0)
            .alias("saldo_mora_gtq"),
            f.when(f.col("ciclo_mora_usd") > 0, f.col("saldo_al_dia_usd"))
            .otherwise(0)
            .alias("saldo_mora_usd"),
            f.when(
                (f.col("ciclo_mora") > 0) & (f.col("saldo_al_dia_gtq") > 0),
                f.col("saldo_al_dia_gtq"),
            )
            .otherwise(0)
            .alias("mayor_saldo_mora_gtq"),
            f.when(
                (f.col("ciclo_mora") > 0) & (f.col("saldo_al_dia_usd") > 0),
                f.col("saldo_al_dia_usd"),
            )
            .otherwise(0)
            .alias("mayor_saldo_mora_usd"),
            f.col("tasa_cambio"),
        )
        .select(
            f.col("*"),
            f.when(
                f.col("max_ciclo_mora") > 0,
                f.col("mayor_saldo_mora_gtq")
                + (f.col("mayor_saldo_mora_usd") * f.col("tasa_cambio")),
            )
            .otherwise(0)
            .alias("mayor_saldo_mora_quetzalizado"),
            f.when(
                (f.col("ciclo_mora_gtq") > 0) & (f.col("ciclo_mora_usd") > 0),
                f.col("saldo_mora_gtq")
                + f.col("saldo_mora_usd") * f.col("tasa_cambio"),
            )
            .when(f.col("ciclo_mora_gtq") > 0, f.col("saldo_mora_gtq"))
            .when(
                f.col("ciclo_mora_usd") > 0,
                f.col("saldo_mora_usd") * f.col("tasa_cambio"),
            )
            .otherwise(0)
            .alias("saldo_mora_quetzalizado"),
        )
        .drop(f.col("tasa_cambio"))
    )

    cc_limit_changes = (
        p_cleaned_cc_updates.filter(
            (f.col("id_proceso") == 5)
            & (f.col("estatus_anterior") != f.col("estatus_actual"))
        )
        .groupBy(
            f.col("id_cliente"), f.col("cuenta_corporativa"), f.col("_observ_end_dt")
        )
        .agg(
            f.lit(1).alias("cambio_limite_flag"),
        )
    )

    account_df = account_df.join(
        cc_limit_changes,
        on=["id_cliente", "cuenta_corporativa", "_observ_end_dt"],
        how="left",
    )

    return account_df


@task(
    name="process_credit_card_balance_task",
    tags=["data transformation", "processing"],
)
def process_credit_card_balance_task(
    p_cleaned_cc_balance: DataFrame,
    p_cleaned_daily_rate: DataFrame,
) -> DataFrame:
    """
    Transforms and processes credit cards balance data.

    Args:
        p_cleaned_cc_balance (DataFrame): Cleaned credit card balance data.
        p_cleaned_daily_rate (DataFrame): Cleaned daily exchange data.

    Returns:
        DataFrame: Transformed credit card balance DataFrame.
    """
    daily_rate = p_cleaned_daily_rate.filter(
        f.col("fecha_transaccion") == f.col("_observ_end_dt")
    )

    df_balance = add_exchange_rate_task(p_cleaned_cc_balance, daily_rate, True).select(
        f.col("cuenta_corporativa"),
        f.col("_observ_end_dt"),
        f.col("saldo_total_usd"),
        f.col("saldo_total_gtq"),
        (
            convert_currency_to_gtq_task("saldo_total_usd", "tasa_cambio")
            + f.col("saldo_total_gtq")
        ).alias("saldo_total_quetzalizado"),
        f.col("saldo_capital_usd"),
        f.col("saldo_capital_gtq"),
        (
            convert_currency_to_gtq_task("saldo_capital_usd", "tasa_cambio")
            + f.col("saldo_capital_gtq")
        ).alias("saldo_capital_quetzalizado"),
        f.col("saldo_intereses_usd"),
        f.col("saldo_intereses_gtq"),
        (
            convert_currency_to_gtq_task("saldo_intereses_usd", "tasa_cambio")
            + f.col("saldo_intereses_gtq")
        ).alias("saldo_intereses_quetzalizado"),
        f.col("cuota_minima_pago_gtq"),
        f.col("cuota_minima_pago_usd"),
        (
            convert_currency_to_gtq_task("cuota_minima_pago_usd", "tasa_cambio")
            + f.col("cuota_minima_pago_gtq")
        ).alias("cuota_minima_pago_quetzalizado"),
    )

    return df_balance


@task(
    name="process_credit_card_monthly_payments_task",
    tags=["data transformation", "processing"],
)
def process_credit_card_monthly_payments_task(
    p_cleaned_cc_transaction_universe: DataFrame,
    p_cleaned_daily_rate: DataFrame,
) -> DataFrame:
    """
    Transforms and processes credit cards monthly payments data.

    Args:
        p_cleaned_cc_transaction_universe (DataFrame): Cleaned credit card payments.
        p_cleaned_daily_rate (DataFrame): Cleaned daily exchange data.

    Returns:
        DataFrame: Transformed credit card payments DataFrame.
    """
    cc_transaction_universe = (
        add_exchange_rate_task(p_cleaned_cc_transaction_universe, p_cleaned_daily_rate)
        .filter(f.col("id_tipo_transaccion").isin(2, 7))
        .select(
            f.col("*"),
            f.when(f.col("id_tipo_transaccion") == 2, f.lit("pagos"))
            .otherwise(f.lit("otros_creditos"))
            .alias("descripcion_transaccion"),
            *create_currency_col_task("id_moneda", "monto_transaccion2", "tasa_cambio"),
        )
        .groupBy(f.col("cuenta_corporativa"), f.col("_observ_end_dt"))
        .pivot("descripcion_transaccion")
        .agg(
            f.sum(f.col("monto_transaccion2_gtq")).alias("monto_pago_gtq"),
            f.sum(f.col("monto_transaccion2_usd")).alias("monto_pago_usd"),
            f.sum(f.col("monto_transaccion2_quetzalizado")).alias(
                "monto_pago_quetzalizado"
            ),
        )
    )

    return cc_transaction_universe


@task(
    name="process_credit_card_tenure_tsae83_task",
    tags=["data transformation", "processing"],
)
def process_credit_card_tenure_tsae83_task(
    p_cleaned_cc_tenure_tsae83: DataFrame,
) -> DataFrame:
    """
    Transforms and processes credit cards tenure data from tsae83.

    Args:
        p_cleaned_cc_tenure_tsae83 (DataFrame): Cleaned tenure from tsae83 data.

    Returns:
        DataFrame: Transformed credit card tsae83 tenure DataFrame.
    """
    df_tsae83 = (
        p_cleaned_cc_tenure_tsae83.filter(~f.col("id_situacion_mora").isin(5, 11))
        .groupBy(f.col("cuenta_corporativa"), f.col("_observ_end_dt"))
        .agg(
            f.max(
                f.when(
                    f.col("dias_mora") > f.col("dias_mora_intereses"),
                    f.col("dias_mora"),
                ).otherwise(f.col("dias_mora_intereses"))
            ).alias("max_dias_mora_e83"),
            f.max(
                f.when(
                    f.col("ciclo_mora") > f.col("ciclo_mora_intereses"),
                    f.col("ciclo_mora"),
                ).otherwise(f.col("ciclo_mora_intereses"))
            ).alias("max_ciclo_mora_e83"),
            f.max(f.col("dias_mora_capital")).alias("max_dias_mora_capital_e83"),
            f.max(f.col("dias_mora_intereses")).alias("max_dias_mora_intereses_e83"),
        )
    )

    return df_tsae83


@task(
    name="calculates_active_financing_metrics_task",
    tags=["data transformation", "processing"],
)
def calculates_active_financing_metrics_task(
    p_cleaned_cc_extra_financing: DataFrame, p_cleaned_cc_account: DataFrame
) -> DataFrame:
    """
    Aggregates active financing metrics, including the financing count,
    earliest and latest financing dates, total pending installments,
    and total financing term in months.

    Args:
        p_cleaned_cc_extra_financing (DataFrame): Cleaned cc financing data.
        p_cleaned_cc_account (DataFrame): Cleaned credit card data

    Returns:
        DataFrame: A Dataframe containing active financing metrics.
    """
    df_active_extra_financing = join_dataframes_task(
        ["cuenta_corporativa", "_observ_end_dt"],
        [p_cleaned_cc_extra_financing, p_cleaned_cc_account],
    ).select(
        p_cleaned_cc_extra_financing["*"],
        f.col("fecha_financiamiento").alias("fecha_inicio_financiamiento"),
        f.when(
            ((f.dayofmonth("fecha_financiamiento") <= f.dayofmonth("fecha_corte")))
            & (f.month("fecha_financiamiento") == f.month("fecha_corte"))
            & (f.year("fecha_financiamiento") == f.year("fecha_corte")),
            f.make_date(
                f.year(
                    f.add_months(
                        f.col("fecha_financiamiento"), (f.col("plazo_meses") - 1)
                    )
                ),
                f.month(
                    f.add_months(
                        f.col("fecha_financiamiento"), (f.col("plazo_meses") - 1)
                    )
                ),
                f.dayofmonth("fecha_corte"),
            ),
        )
        .otherwise(
            f.make_date(
                f.year(
                    f.add_months(f.col("fecha_financiamiento"), (f.col("plazo_meses")))
                ),
                f.month(
                    f.add_months(f.col("fecha_financiamiento"), (f.col("plazo_meses")))
                ),
                f.dayofmonth("fecha_corte"),
            )
        )
        .alias("fecha_fin_programada_finanaciamiento"),
        f.round(f.col("saldo_pendiente") / f.col("cuota_mensual"), 0)
        .cast("int")
        .alias("cuotas_pendientes_financiamientos"),
        unified_currency_col_task(
            "descripcion_moneda", "monto_total", "tasa_cambio"
        ).alias("monto_total_quetzalizado"),
        unified_currency_col_task(
            "descripcion_moneda", "cuota_mensual", "tasa_cambio"
        ).alias("cuota_mensual_quetzalizado"),
        unified_currency_col_task(
            "descripcion_moneda", "saldo_pendiente", "tasa_cambio"
        ).alias("saldo_pendiente_quetzalizado"),
    )

    df_active_financing_metrics = df_active_extra_financing.groupBy(
        f.col("cuenta_corporativa"), f.col("_observ_end_dt")
    ).agg(
        f.count("*").alias("financiamientos_activos_cnt"),
        f.min(f.col("fecha_inicio_financiamiento")).alias(
            "min_fecha_inicio_financiamiento_activo"
        ),
        f.max("fecha_fin_programada_finanaciamiento").alias(
            "max_fecha_programada_fin_finanaciamiento_activo"
        ),
        f.sum("cuotas_pendientes_financiamientos").alias(
            "sum_cuotas_pendientes_financiamiento"
        ),
        f.sum("plazo_meses").alias("sum_total_plazo_meses_financiamiento_activo"),
        f.sum("monto_total_quetzalizado").alias(
            "total_monto_otorgado_financiamiento_activo_quetzalizado"
        ),
        f.sum("cuota_mensual_quetzalizado").alias(
            "total_cuota_mensual_financiamiento_activo_quetzalizado"
        ),
        f.sum("saldo_pendiente_quetzalizado").alias(
            "total_saldo_pendiente_financiamiento_activo_quetzalizado"
        ),
    )

    return df_active_financing_metrics


@task(
    name="calculate_cc_current_month_financing_amounts_task",
    tags=["data transformation", "processing"],
)
def calculate_cc_current_month_financing_amounts_task(
    p_cleaned_cc_extra_financing: DataFrame,
) -> DataFrame:
    """
    Calculates the financing amounts granted during the current month for each
    currency, broken down by financing type

    Args:
        p_cleaned_cc_extra_financing (DataFrame): Cleaned cc financing data.

    Returns:
        DataFrame: A Dataframe containing the financing amounts granted during
        the current month.
    """

    df_active_financing = (
        p_cleaned_cc_extra_financing.filter(
            f.col("fecha_financiamiento").between(
                f.col("_observ_start_dt"), f.col("_observ_end_dt")
            )
        )
        .select(
            p_cleaned_cc_extra_financing["*"],
            *split_currency_task("monto_total", "descripcion_moneda", True),
            unified_currency_col_task(
                "descripcion_moneda", "monto_total", "tasa_cambio"
            ).alias("monto_total_quetzalizado"),
        )
        .fillna(0)
    )

    df_financing_amounts = (
        df_active_financing.groupBy(
            f.col("cuenta_corporativa"), f.col("_observ_end_dt")
        )
        .pivot("agrupacion_promocion")
        .agg(
            f.sum(f.col("monto_total_gtq")).alias("monto_otorgado_mes_actual_gtq"),
            f.sum(f.col("monto_total_quetzalizado")).alias(
                "monto_otorgado_mes_actual_quetzalizado"
            ),
        )
    )

    return df_financing_amounts


@task(
    name="calculates_active_financing_amount_metrics_task",
    tags=["data transformation", "processing"],
)
def calculates_active_financing_amount_metrics_task(
    p_cleaned_cc_extra_financing: DataFrame,
) -> DataFrame:
    """
    Calculates active financing metrics by financing type and currency.

    Args:
        p_cleaned_cc_extra_financing (DataFrame): Cleaned cc financing data.

    Returns:
        DataFrame: A Dataframe containing active financing metrics by financing type
        and currency.
    """

    df_active_financing = p_cleaned_cc_extra_financing.select(
        p_cleaned_cc_extra_financing["*"],
        *split_currency_task("monto_total", "descripcion_moneda", True),
        *split_currency_task("cuota_mensual", "descripcion_moneda", True),
        *split_currency_task("saldo_pendiente", "descripcion_moneda", True),
        unified_currency_col_task(
            "descripcion_moneda", "monto_total", "tasa_cambio"
        ).alias("monto_total_quetzalizado"),
        unified_currency_col_task(
            "descripcion_moneda", "cuota_mensual", "tasa_cambio"
        ).alias("cuota_mensual_quetzalizado"),
        unified_currency_col_task(
            "descripcion_moneda", "saldo_pendiente", "tasa_cambio"
        ).alias("saldo_pendiente_quetzalizado"),
    ).fillna(0)

    df_active_financing_metrics = (
        df_active_financing.groupBy(
            f.col("cuenta_corporativa"), f.col("_observ_end_dt")
        )
        .pivot("agrupacion_promocion")
        .agg(
            f.sum(f.col("monto_total_gtq")).alias("monto_vigente_otorgado_gtq"),
            f.sum(f.col("cuota_mensual_gtq")).alias("cuota_mensual_vigente_gtq"),
            f.sum(f.col("saldo_pendiente_gtq")).alias("saldo_pendiente_gtq"),
            f.sum(f.col("monto_total_quetzalizado")).alias(
                "monto_vigente_otorgado_quetzalizado"
            ),
            f.sum(f.col("saldo_pendiente_quetzalizado")).alias(
                "saldo_pendiente_quetzalizado"
            ),
            f.sum(f.col("cuota_mensual_quetzalizado")).alias(
                "cuota_mensual_vigente_quetzalizado"
            ),
            f.round(
                (
                    (f.sum(f.col("saldo_pendiente_quetzalizado")))
                    / (f.sum(f.col("monto_total_quetzalizado")))
                )
                * 100,
                2,
            ).alias("porcentaje_saldo_pendiente"),
        )
    )

    return df_active_financing_metrics


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente", "cuenta_corporativa"])
@task(name="transform_credit_card_task", tags=["data transformation", "processing"])
def transform_credit_card_task(
    p_cleaned_cc_account: DataFrame,
    p_cleaned_cc_categories: DataFrame,
    p_cleaned_cc_updates: DataFrame,
    p_cleaned_cc_balance: DataFrame,
    p_cleaned_cc_tenure_tsae83: DataFrame,
    p_cleaned_cc_transaction_universe: DataFrame,
    p_cleaned_daily_rate: DataFrame,
    p_cleaned_cc_extra_financing: DataFrame,
) -> DataFrame:
    """
    Transforms and processes credit card information to create features.

    Args:
        p_cleaned_cc_account (DataFrame): Cleaned main credit card account data.
        p_cleaned_cc_categories (DataFrame): Cleaned credit card categories data.
        p_cleaned_cc_updates (DataFrame): Cleaned credit card updates data.
        p_cleaned_cc_balance (DataFrame): Cleaned credit card balance data.
        p_cleaned_cc_tenure_tsae83 (DataFrame): Cleaned tenure from tsae83 data.
        p_cleaned_cc_transaction_universe (DataFrame): Cleaned credit card payments.
        p_cleaned_daily_rate (DataFrame): Cleaned daily exchange data.
        p_cleaned_cc_extra_financing (DataFrame): Cleaned cc financing data.

    Returns:
        DataFrame: Main credit card features DataFrame.
    """
    p_cleaned_cc_account = (
        p_cleaned_cc_account.alias("acc")
        .join(
            p_cleaned_cc_categories.alias("cat"),
            on=["id_categoria_tc"],
            how="left",
        )
        .select(
            p_cleaned_cc_account["*"],
            f.when(f.col("cat.emisor").isNotNull(), f.col("cat.emisor"))
            .otherwise(f.col("acc.emisor"))
            .alias("emisor"),
            f.col("cat.segmento"),
            f.col("cat.id_categoria"),
            f.col("cat.descripcion_categoria").alias("categoria"),
            f.col("cat.categoria_estandard"),
            f.col("cat.descripcion_producto"),
            f.col("cat.empresarial_flag"),
            f.col("cat.institucional_flag"),
            f.coalesce(f.col("bi_online_flag"), f.lit(0)).alias("bi_online_flag"),
        )
        .drop(p_cleaned_cc_account.emisor)
    )

    cc_account_df = process_credit_card_account_task(
        p_cleaned_cc_account,
        p_cleaned_cc_updates,
        p_cleaned_daily_rate,
    )

    cc_balance_df = process_credit_card_balance_task(
        p_cleaned_cc_balance, p_cleaned_daily_rate
    )

    cc_tenure_tsae83_df = process_credit_card_tenure_tsae83_task(
        p_cleaned_cc_tenure_tsae83
    )

    cc_account_df = cc_account_df.join(
        cc_tenure_tsae83_df, on=["_observ_end_dt", "cuenta_corporativa"], how="left"
    ).select(
        cc_account_df["*"],
        f.col("max_dias_mora_e83"),
        f.col("max_ciclo_mora_e83"),
        f.when(f.col("max_ciclo_mora") == 0, 0)
        .otherwise(f.col("max_dias_mora_capital_e83"))
        .alias("max_dias_mora_capital_e83"),
        f.when(f.col("max_ciclo_mora") == 0, 0)
        .otherwise(f.col("max_dias_mora_intereses_e83"))
        .alias("max_dias_mora_intereses_e83"),
    )

    cc_monthly_payments = process_credit_card_monthly_payments_task(
        p_cleaned_cc_transaction_universe, p_cleaned_daily_rate
    )

    daily_rate_df = p_cleaned_daily_rate.filter(
        f.col("fecha_transaccion") == f.col("_observ_end_dt")
    ).drop("_observ_start_dt")

    df_active_extra_financing = p_cleaned_cc_extra_financing.filter(
        (f.col("estado_financiamiento") == "activo")
        & (f.col("fecha_financiamiento") <= f.col("_observ_end_dt"))
    )

    df_active_extra_financing = add_exchange_rate_task(
        df_active_extra_financing, daily_rate_df, True
    )

    df_cc_financing_date = calculates_active_financing_metrics_task(
        df_active_extra_financing, p_cleaned_cc_account
    )

    cc_financing_monthly_amount_df = calculate_cc_current_month_financing_amounts_task(
        df_active_extra_financing
    )

    df_cc_financing = calculates_active_financing_amount_metrics_task(
        df_active_extra_financing
    )

    df_cc_features = join_dataframes_task(
        ["cuenta_corporativa", "_observ_end_dt"],
        [
            cc_account_df,
            cc_balance_df,
            cc_monthly_payments,
            df_cc_financing_date,
            cc_financing_monthly_amount_df,
            df_cc_financing,
        ],
    )

    return df_cc_features


@flow(name="credit_card_flow")
def credit_card_flow():
    """
    Loads, transforms and saves main credit card features in the data lake.

    The flow performs the following operations:
    1. Loads cleaned credit card data using the specified date range.
    2. Transforms and processes the credit card features data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        credit card features data.
    """
    raw_data = load_raw_data_flow()

    df_credit_card_features = transform_credit_card_task(
        raw_data["cleaned_cc_account"],
        raw_data["cleaned_cc_categories"],
        raw_data["cleaned_cc_updates"],
        raw_data["cleaned_cc_balance"],
        raw_data["cleaned_cc_tenure_tsae83"],
        raw_data["cleaned_cc_transaction_universe"],
        raw_data["cleaned_exchange_rate"],
        raw_data["cleaned_cc_extra_financing_master"],
    )

    save_data_flow(df_credit_card_features)
