import pyspark.sql.functions as f  # type: ignore
from prefect import flow, task  # type: ignore
from pyspark.sql import DataFrame  # type: ignore

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente"])
@task(name="mc_saving_account_task", tags=["data integration", "processing"])
def mc_saving_account_task(
    p_transformed_saving: DataFrame,
) -> DataFrame:
    """Processes and integrates customer's saving account data.

    This task integrates the resulting data from the Data Transformation Layer,
      using features from bi saving account.

    Args:
        p_transformed_saving (DataFrame): Transformed saving account data.

    Returns:
        DataFrame: Unified and processed Dataframe.
    """
    df_master_savings = p_transformed_saving.groupBy(
        f.col("id_cliente"), f.col("_observ_end_dt")
    ).agg(
        f.sum(
            f.when(
                f.col("aho_situacion_cuenta_homologado") == "vigente",
                f.col("aho_saldo_total_gtq_val"),
            ).otherwise(0)
        ).alias("saldo_total_gtq_sum"),
        f.sum(
            f.when(
                f.col("aho_situacion_cuenta_homologado") == "vigente",
                f.col("aho_saldo_total_usd_val"),
            ).otherwise(0)
        ).alias("saldo_total_usd_sum"),
        f.sum(
            f.when(
                f.col("aho_situacion_cuenta_homologado") == "vigente",
                f.col("aho_saldo_promedio_gtq_val"),
            ).otherwise(0)
        ).alias("saldo_promedio_gtq_sum"),
        f.sum(
            f.when(
                f.col("aho_situacion_cuenta_homologado") == "vigente",
                f.col("aho_saldo_promedio_usd_val"),
            ).otherwise(0)
        ).alias("saldo_promedio_usd_sum"),
        f.sum(
            f.when(
                f.col("aho_situacion_cuenta_homologado") == "vigente",
                f.col("aho_saldo_promedio_quetzalizado_val"),
            ).otherwise(0)
        ).alias("saldo_promedio_quetzalizado_sum"),
        f.sum(f.col("aho_creditos_monto_gtq_val")).alias("monto_creditos_gtq_sum"),
        f.sum(f.col("aho_nota_credito_monto_quetzalizado_val")).alias(
            "monto_notas_credito_quetzalizado_sum"
        ),
        f.sum(
            f.col("aho_nota_credito_gtq_cnt") + f.col("aho_nota_credito_usd_cnt")
        ).alias("notas_credito_quetzalizado_cnt"),
        f.sum(f.col("aho_debitos_gtq_val")).alias("monto_debitos_gtq_sum"),
        f.sum(f.col("aho_debitos_gtq_cnt")).alias("debitos_gtq_cnt"),
        f.min(f.col("aho_fecha_apertura")).alias("fecha_apertura_primera_cuenta"),
        f.sum(f.col("aho_creditos_quetzalizado_val")).alias(
            "monto_creditos_quetzalizado_sum"
        ),
        f.sum(f.col("aho_creditos_usd_cnt") + f.col("aho_creditos_gtq_cnt")).alias(
            "creditos_quetzalizado_cnt"
        ),
        f.sum(f.col("aho_debitos_quetzalizado_val")).alias(
            "monto_debitos_quetzalizado_sum"
        ),
        f.sum(f.col("aho_debitos_gtq_cnt") + f.col("aho_debitos_usd_cnt")).alias(
            "debitos_quetzalizado_cnt"
        ),
        f.round(
            f.months_between(
                f.col("_observ_end_dt"), f.min(f.col("aho_fecha_apertura"))
            ),
            0,
        ).alias("meses_primera_apertura_cnt"),
        f.sum(f.col("aho_retiro_monto_quetzalizado_val")).alias(
            "monto_retiros_quetzalizado_sum"
        ),
        f.sum(f.col("aho_retiro_gtq_cnt") + f.col("aho_retiro_usd_cnt")).alias(
            "retiros_quetzalizado_cnt"
        ),
        f.max(f.col("aho_meses_cuentas_vigentes")).alias("meses_cuentas_vigentes_cnt"),
    )
    return df_master_savings


@flow(name="mc_saving_account_flow")
def mc_saving_account_flow():
    """
    Load, integrate and saves result data in data lake.

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

    df_master_saving = mc_saving_account_task(
        data_transformation["transformed_saving"],
    )

    save_data_flow(df_master_saving)
