import pyspark.sql.functions as f  # type: ignore
from prefect import flow, task  # type: ignore
from pyspark.sql import DataFrame  # type: ignore

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente"])
@task(name="mc_current_account_task", tags=["data integration", "processing"])
def mc_current_account_task(
    p_transformed_transformed_current: DataFrame,
) -> DataFrame:
    """Processes and integrates customer data.

    This task integrates the resulting data from the Data Transformation Layer,
    using features from bi current account.

    Args:
        p_transformed_transformed_current (DataFrame): Transformed current account data.

    Returns:
        DataFrame: Unified and processed Dataframe.
    """
    current_account_df = p_transformed_transformed_current
    df_ca_mc = current_account_df.groupBy(
        f.col("id_cliente"), f.col("_observ_end_dt")
    ).agg(
        f.sum(f.col("mon_pago_cheque_monto_quetzalizado_val"))
        .cast("float")
        .alias("pago_cheque_monto_quetzalizado"),
        f.sum(f.col("mon_pago_cheque_gtq_cnt") + f.col("mon_pago_cheque_usd_cnt"))
        .cast("int")
        .alias("pago_cheque_monto_quetzalizado_cnt"),
        f.sum(f.col("mon_creditos_monto_gtq_val"))
        .cast("float")
        .alias("creditos_monto_gtq"),
        f.sum(f.col("mon_creditos_gtq_cnt")).cast("int").alias("creditos_gtq_cnt"),
        f.sum(f.col("mon_creditos_usd_cnt")).cast("int").alias("creditos_usd_cnt"),
        f.sum(f.col("mon_nota_credito_monto_quetzalizado_val"))
        .cast("float")
        .alias("nota_credito_monto_quetzalizado"),
        f.sum(f.col("mon_nota_credito_gtq_cnt") + f.col("mon_nota_credito_usd_cnt"))
        .cast("int")
        .alias("nota_credito_monto_quetzalizado_cnt"),
        f.sum(f.col("mon_debitos_gtq_val")).cast("float").alias("debitos_gtq"),
        f.sum(f.col("mon_debitos_gtq_cnt")).cast("int").alias("debitos_gtq_cnt"),
        f.sum(f.col("mon_nota_debito_monto_quetzalizado_val"))
        .cast("float")
        .alias("nota_debito_monto_quetzalizado"),
        f.sum(f.col("mon_nota_debito_gtq_cnt") + f.col("mon_nota_debito_usd_cnt"))
        .cast("int")
        .alias("nota_debito_monto_quetzalizado_cnt"),
        f.sum(f.col("mon_deposito_monto_quetzalizado_val"))
        .cast("float")
        .alias("deposito_monto_quetzalizado"),
        f.sum(f.col("mon_deposito_gtq_cnt") + f.col("mon_deposito_usd_cnt"))
        .cast("int")
        .alias("deposito_monto_quetzalizado_cnt"),
        f.sum(f.col("mon_creditos_quetzalizado_val"))
        .cast("float")
        .alias("creditos_quetzalizado"),
        f.sum(f.col("mon_creditos_gtq_cnt") + f.col("mon_creditos_usd_cnt"))
        .cast("int")
        .alias("creditos_quetzalizado_cnt"),
        f.sum(f.col("mon_debitos_quetzalizado_val"))
        .cast("float")
        .alias("debitos_quetzalizado"),
        f.sum(f.col("mon_debitos_gtq_cnt") + f.col("mon_debitos_usd_cnt"))
        .cast("int")
        .alias("debitos_quetzalizado_cnt"),
        f.sum(f.col("mon_saldo_total_gtq_val")).cast("float").alias("saldo_total_gtq"),
        f.sum(f.col("mon_saldo_total_usd_val")).cast("float").alias("saldo_total_usd"),
        f.sum(f.coalesce(f.col("mon_saldo_promedio_gtq_val"), f.lit(0))).alias(
            "saldo_promedio_gtq"
        ),
        f.sum(f.coalesce(f.col("mon_saldo_promedio_usd_val"), f.lit(0))).alias(
            "saldo_promedio_usd"
        ),
        f.sum(f.coalesce(f.col("mon_saldo_promedio_quetzalizado_val"), f.lit(0))).alias(
            "saldo_promedio_quetzalizado"
        ),
        f.sum(f.col("mon_debito_usd_val")).cast("float").alias("debito_usd"),
        f.sum(f.col("mon_debitos_usd_cnt")).cast("int").alias("debitos_usd_cnt"),
        f.min(f.col("mon_fecha_apertura")).alias("fecha_primera_cuenta_dt"),
        f.min(
            f.when(
                f.col("mon_situacion_cuenta_homologado") == "vigente",
                f.col("mon_fecha_apertura"),
            )
        ).alias("fecha_primera_cuenta_valida_dt"),
        f.max(f.col("mon_dias_apertura_cuenta_monetaria")).alias(
            "dias_apertura_cuenta_monetaria_cnt"
        ),
        f.max(f.col("mon_meses_cuenta_monetaria_valida")).alias(
            "meses_cuenta_monetaria_valida_cnt"
        ),
        f.max(f.col("mon_dias_cuenta_monetaria_valida")).alias(
            "dias_cuenta_monetaria_valida_cnt"
        ),
    )

    account_valid_df = (
        current_account_df.filter(f.col("mon_situacion_cuenta_homologado") == "vigente")
        .groupBy(f.col("id_cliente"), f.col("_observ_end_dt"))
        .agg(
            f.sum(f.col("mon_saldo_total_gtq_val")).alias("vigente_saldo_total_gtq"),
            f.count(f.when(f.col("mon_id_moneda") == 1, 1).otherwise(0)).alias(
                "vigente_cuentas_gtq_cnt"
            ),
            f.sum(f.col("mon_saldo_total_usd_val")).alias("vigente_saldo_total_usd"),
            f.count(f.when(f.col("mon_id_moneda") == 2, 1).otherwise(0)).alias(
                "vigente_cuentas_usd_cnt"
            ),
            f.sum(f.col("mon_saldo_total_quetzalizado_val")).alias(
                "vigente_saldo_total_quetzalizado"
            ),
            f.count("*").alias("vigente_cuentas_cnt"),
        )
    )

    df_ca_mc = df_ca_mc.join(
        account_valid_df, ["id_cliente", "_observ_end_dt"], "left"
    ).fillna(0)

    return df_ca_mc


@flow(name="mc_current_account_flow")
def mc_current_account_flow():
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

    df_master_current = mc_current_account_task(
        data_transformation["transformed_current"]
    )

    save_data_flow(df_master_current)
