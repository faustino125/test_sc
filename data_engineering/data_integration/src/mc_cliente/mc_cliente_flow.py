import pyspark.sql.functions as f  # type: ignore
from prefect import flow, task  # type: ignore
from pyspark.sql import DataFrame  # type: ignore

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_integration.src.utils.customer_utils import (
    legal_segment_mapping_task,
)
from data_engineering.data_integration.src.utils.utils import (
    join_dataframes_task,
    sum_features_across_dataframes_task,
)


@task(name="mc_cliente_aggregations_task", tags=["data integration", "processing"])
def mc_cliente_aggregations_task(
    p_transformed_current: DataFrame,
    p_transformed_saving: DataFrame,
    p_transformed_customer: DataFrame,
    p_transformed_credit_card: DataFrame,
    p_transformed_loan: DataFrame,
    p_transformed_investment: DataFrame,
) -> dict[str, DataFrame]:
    """Builds customer universe and product-level aggregations.

    Aggregates raw transformed data from each product into features grouped
    by client and observation date.

    Args:
        p_transformed_current (DataFrame): Transformed current account data.
        p_transformed_saving (DataFrame): Transformed saving data.
        p_transformed_customer (DataFrame): Transformed customer data.
        p_transformed_credit_card (DataFrame): Transformed credit card data.
        p_transformed_loan (DataFrame): Transformed loan data.
        p_transformed_investment(DataFrame): Transformed investment data.

    Returns:
        dict[str, DataFrame]: Dictionary of DataFrames with customer
            aggregations by key.
    """
    df_customer = p_transformed_customer.select(
        "id_cliente", "_observ_end_dt"
    ).distinct()
    df_customer_balance = p_transformed_customer.select(
        f.col("id_cliente"),
        f.col("_observ_end_dt"),
        f.col("cli_ingreso_mensual_val"),
        f.col("cli_id_tipo_cliente"),
        f.col("cli_grupo_corporativo"),
        f.greatest(
            "cli_ingresos_anuales_moodys_gtq_val",
            "cli_ingresos_anuales_sib_gtq_val",
            "cli_ingresos_anuales_e83_gtq_val",
            "cli_ingresos_anuales_ire_gtq_val",
        ).alias("cli_ingresos_anuales_gtq"),
        f.col("cli_ingresos_anuales_monetarios_gtq"),
    ).distinct()

    df_savings = (
        p_transformed_saving.filter(
            f.col("aho_situacion_cuenta_homologado") == "vigente"
        )
        .groupBy("id_cliente", "_observ_end_dt")
        .agg(
            f.sum("aho_saldo_total_quetzalizado_val").alias(
                "saldo_total_quetzalizado_sum"
            ),
            f.sum("aho_saldo_promedio_quetzalizado_val")
            .cast("float")
            .alias("aho_saldo_promedio_quetzalizado"),
            f.max("aho_flag_institucional").alias("aho_flag_institucional"),
        )
    )

    df_current = (
        p_transformed_current.filter(
            f.col("mon_situacion_cuenta_homologado") == "vigente"
        )
        .groupBy("id_cliente", "_observ_end_dt")
        .agg(
            f.sum("mon_saldo_total_quetzalizado_val").alias(
                "vigente_saldo_total_quetzalizado"
            ),
            f.sum("mon_saldo_promedio_quetzalizado_val")
            .cast("float")
            .alias("mon_saldo_promedio_quetzalizado"),
            f.max("mon_flag_institucional").alias("mon_flag_institucional"),
        )
    )

    df_loan = p_transformed_loan.groupBy("id_cliente", "_observ_end_dt").agg(
        f.sum("pre_cuota_prestamo_quetzalizado_val").alias(
            "cuota_prestamo_quetzalizado_sum"
        )
    )

    valid_cc_list = [
        "bloqueada",
        "con_problema",
        "juridico",
        "en_mora",
        "sobregirada",
        "vigente",
    ]

    df_credit_card = (
        p_transformed_credit_card.filter(f.col("tc_bi_online_flag") != 1)
        .filter(f.col("tc_situacion_cuenta_homologado").isin(valid_cc_list))
        .groupBy("id_cliente", "_observ_end_dt")
        .agg(
            f.sum("tc_saldo_capital_quetzalizado_val").alias(
                "saldo_capital_quetzalizado_sum"
            ),
            f.sum("tc_saldo_al_corte_quetzalizado_val").alias(
                "saldo_al_corte_quetzalizado_sum"
            ),
        )
    )

    df_investment_product = (
        p_transformed_investment.groupBy(
            f.col("id_cliente"),
            f.col("_observ_end_dt"),
        )
        .pivot("inv_descripcion_producto")
        .agg(
            f.sum(f.coalesce(f.col("inv_saldo_promedio_6_meses_gtq_val"), f.lit(0)))
            .cast("float")
            .alias("saldo_promedio_6m_gtq"),
            f.sum(f.coalesce(f.col("inv_saldo_promedio_6_meses_usd_val"), f.lit(0)))
            .cast("float")
            .alias("saldo_promedio_6m_usd"),
            f.sum(
                f.coalesce(
                    f.col("inv_saldo_promedio_6_meses_quetzalizado_val"), f.lit(0)
                )
            )
            .cast("float")
            .alias("saldo_promedio_6m_quetzalizado"),
            f.sum(f.coalesce(f.col("inv_saldo_gtq_val"), f.lit(0)))
            .cast("float")
            .alias("saldo_gtq"),
            f.sum(f.coalesce(f.col("inv_saldo_usd_val"), f.lit(0)))
            .cast("float")
            .alias("saldo_usd"),
            f.sum(f.coalesce(f.col("inv_saldo_quetzalizado_val"), f.lit(0)))
            .cast("float")
            .alias("saldo_quetzalizado"),
        )
    )

    df_investment = p_transformed_investment.groupBy(
        "id_cliente", "_observ_end_dt"
    ).agg(
        f.sum("inv_saldo_promedio_6_meses_quetzalizado_val")
        .cast("float")
        .alias("inv_saldo_promedio_quetzalizado"),
        f.max("inv_flag_institucional").alias("inv_flag_institucional"),
    )

    return {
        "df_customer": df_customer,
        "df_customer_balance": df_customer_balance,
        "df_savings": df_savings,
        "df_current": df_current,
        "df_loan": df_loan,
        "df_credit_card": df_credit_card,
        "df_investment_product": df_investment_product,
        "df_investment": df_investment,
    }


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente"])
@task(name="mc_cliente_task", tags=["data integration", "processing"])
def mc_cliente_task(
    p_aggregations: dict[str, DataFrame],
) -> DataFrame:
    """Processes and integrates customer data.

    Using product-level aggregations, computes current assets, current
    liabilities, total balance and derived financial ratios at the
    (id_cliente, _observ_end_dt) grain.

    Args:
        p_aggregations (dict[str, DataFrame]): Dictionary returned by
            mc_cliente_aggregations_task containing the customer universe
            and product-level aggregations.

    Returns:
        DataFrame: Unified and processed Dataframe.
    """
    df_customer = p_aggregations["df_customer"]
    df_customer_balance = p_aggregations["df_customer_balance"]
    df_savings = p_aggregations["df_savings"]
    df_current = p_aggregations["df_current"]
    df_loan = p_aggregations["df_loan"]
    df_credit_card = p_aggregations["df_credit_card"]
    df_investment_product = p_aggregations["df_investment_product"]
    df_investment = p_aggregations["df_investment"]

    df_current_assets = sum_features_across_dataframes_task(
        p_features_to_sum=[
            (df_savings, "saldo_total_quetzalizado_sum"),
            (df_current, "vigente_saldo_total_quetzalizado"),
            (df_credit_card, "saldo_capital_quetzalizado_sum"),
        ],
        p_join_keys=["id_cliente", "_observ_end_dt"],
        p_result_col="activos_corrientes_sum",
        p_replace_negative=True,
    )

    df_current_liabilities = sum_features_across_dataframes_task(
        p_features_to_sum=[
            (df_loan, "cuota_prestamo_quetzalizado_sum"),
            (df_credit_card, "saldo_al_corte_quetzalizado_sum"),
        ],
        p_join_keys=["id_cliente", "_observ_end_dt"],
        p_result_col="pasivos_corrientes_sum",
        p_replace_negative=True,
    )

    total_balance = sum_features_across_dataframes_task(
        p_features_to_sum=[
            (df_savings, "saldo_total_quetzalizado_sum"),
            (df_current, "vigente_saldo_total_quetzalizado"),
        ],
        p_join_keys=["id_cliente", "_observ_end_dt"],
        p_result_col="balance_total_sum",
    )

    df_balance = (
        df_customer_balance.join(
            total_balance,
            on=["id_cliente", "_observ_end_dt"],
            how="left",
        )
        .withColumn(
            "razon_balance_ingresos",
            f.when(
                (f.col("balance_total_sum") != 0)
                & (f.col("cli_ingreso_mensual_val") / f.col("balance_total_sum") >= 0),
                f.col("cli_ingreso_mensual_val") / f.col("balance_total_sum"),
            ).otherwise(None),
        )
        .select("id_cliente", "_observ_end_dt", "razon_balance_ingresos")
    )

    df_current_ratio = (
        df_current_assets.join(
            df_current_liabilities,
            on=["id_cliente", "_observ_end_dt"],
            how="full",
        )
        .withColumn(
            "razon_corriente",
            f.when(
                f.col("pasivos_corrientes_sum") > 0,
                f.col("activos_corrientes_sum") / f.col("pasivos_corrientes_sum"),
            ).otherwise(None),
        )
        .select("id_cliente", "_observ_end_dt", "razon_corriente")
    )

    df_legal = (
        join_dataframes_task(
            ["id_cliente", "_observ_end_dt"],
            [df_customer_balance, df_investment, df_current, df_savings],
        )
        .filter(f.col("cli_id_tipo_cliente") == "2")
        .select(
            f.col("id_cliente"),
            f.col("_observ_end_dt"),
            f.col("cli_grupo_corporativo"),
            f.col("mon_flag_institucional"),
            f.col("aho_flag_institucional"),
            f.col("inv_flag_institucional"),
            f.col("cli_ingresos_anuales_gtq"),
            f.col("cli_ingresos_anuales_monetarios_gtq"),
            (
                f.coalesce(f.col("aho_saldo_promedio_quetzalizado"), f.lit(0))
                + f.coalesce(f.col("mon_saldo_promedio_quetzalizado"), f.lit(0))
                + f.coalesce(f.col("inv_saldo_promedio_quetzalizado"), f.lit(0))
            ).alias("total_captacion_quetzalizado"),
        )
    )

    df_legal_segment = legal_segment_mapping_task(df_legal)

    df_final = join_dataframes_task(
        ["id_cliente", "_observ_end_dt"],
        [
            df_customer,
            df_current_assets,
            df_current_liabilities,
            df_current_ratio,
            total_balance,
            df_balance,
            df_investment_product,
            df_legal_segment,
        ],
    ).fillna(0)

    return df_final


@flow(name="mc_cliente_flow")
def mc_cliente_flow():
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

    aggregations = mc_cliente_aggregations_task(
        data_transformation["transformed_current"],
        data_transformation["transformed_saving"],
        data_transformation["transformed_customer"],
        data_transformation["transformed_credit_card"],
        data_transformation["transformed_loan"],
        data_transformation["transformed_investment"],
    )

    df_master_cliente = mc_cliente_task(aggregations)

    save_data_flow(df_master_cliente)
