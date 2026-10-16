from functools import reduce
from operator import add

import pyspark.sql.functions as f  # type: ignore
from prefect import flow, task  # type: ignore
from pyspark.sql import DataFrame  # type: ignore

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns


@task(name="mc_debit_card_account_task", tags=["data integration", "processing"])
def mc_debit_card_account_task(
    p_transformed_debit_card_account: DataFrame,
) -> DataFrame:
    """Processes and integrates Debit Card Account

    Args:
        p_transformed_debit_card_account (DataFrame): Transformed debit card account.

    Returns:
        DataFrame: Processed debit card account DataFrame.
    """
    df_transformed_debit_card_account = p_transformed_debit_card_account.withColumn(
        "situacion_cuenta_homologado",
        f.concat(f.col("td_situacion_cuenta_homologado"), f.lit("_cnt")),
    )

    df_dc_mc_processed = (
        df_transformed_debit_card_account.filter(f.col("td_id_producto") == 6)
        .groupBy(
            f.col("id_cliente"),
            f.col("_observ_end_dt"),
        )
        .pivot("situacion_cuenta_homologado")
        .agg(f.count("cuenta_corporativa"))
    ).fillna(0)

    df_processes_td_account = df_dc_mc_processed.select(
        f.col("id_cliente"),
        f.col("_observ_end_dt"),
        f.col("bloqueada_cnt").cast("int"),
        f.col("cancelada_cnt").cast("int"),
        f.col("desconocida_cnt").cast("int"),
        f.col("extraviada_cnt").cast("int"),
        f.col("juridico_cnt").cast("int"),
        f.col("robada_cnt").cast("int"),
        f.col("vigente_cnt").cast("int"),
    ).withColumn(
        "cuentas_cnt",
        reduce(
            add,
            [f.col(x) for x in df_dc_mc_processed.columns if ("_cnt" in x)],
        ),
    )

    return df_processes_td_account


@task(name="mc_debit_card_transactions_task", tags=["data_integration", "processing"])
def mc_debit_card_transactions_task(p_transformed_autoriza: DataFrame) -> DataFrame:
    """Groups a Dataframe by id_cliente and _observ_end_dt.
    Then generates aggregated features for debit card transactions.

    Args:
        p_transformed_autoriza (DataFrame): Transformed POS transactions data.

    Returns:
        DataFrame: grouped Dataframe from POS transactions.
    """
    df_au_processed_dc = p_transformed_autoriza.groupBy(
        f.col("id_cliente"), f.col("_observ_end_dt")
    ).agg(
        f.sum(f.col("au_atm_consultas_cnt")).cast("int").alias("atm_consultas_cnt"),
        f.sum(f.col("au_monto_total_transacciones_gtq_val")).alias(
            "monto_total_transacciones_gtq"
        ),
        f.sum(f.col("au_total_transacciones_gtq_cnt"))
        .cast("int")
        .alias("total_transacciones_gtq_cnt"),
        f.sum(f.col("au_monto_total_transacciones_usd_val")).alias(
            "monto_total_transacciones_usd"
        ),
        f.sum(f.col("au_total_transacciones_usd_cnt")).alias(
            "total_transacciones_usd_cnt"
        ),
        f.sum(f.col("au_monto_total_transacciones_val")).alias(
            "monto_total_transacciones"
        ),
        f.sum(f.col("au_total_transacciones_cnt"))
        .cast("int")
        .alias("total_transacciones_cnt"),
        f.sum(f.col("au_atm_retiros_monto_total_transacciones_val")).alias(
            "atm_retiros_monto_total_transacciones"
        ),
        f.sum(f.col("au_atm_retiros_total_transacciones_cnt"))
        .cast("int")
        .alias("atm_retiros_total_transacciones_cnt"),
        f.sum(f.col("au_otros_retiros_monto_total_transacciones_val")).alias(
            "otros_retiros_monto_total_transacciones"
        ),
        f.sum(f.col("au_otros_retiros_total_transacciones_cnt"))
        .cast("int")
        .alias("otros_retiros_total_transacciones_cnt"),
        f.sum(f.col("au_estaciones_combustible_cnt"))
        .cast("int")
        .alias("combustibles_trx_cnt"),
        f.sum(f.col("au_tecnologia_cnt")).cast("int").alias("tecnologia_trx_cnt"),
        f.sum(f.col("au_transporte_cnt")).cast("int").alias("transporte_trx_cnt"),
        f.sum(f.col("au_transporte_mcc_monto_transacciones_val")).alias(
            "transporte_mcc_monto_transacciones"
        ),
    )

    return df_au_processed_dc


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente"])
@task(name="mc_debit_card_task", tags=["data integration", "processing"])
def mc_debit_card_task(
    p_transformed_debit_card_account: DataFrame,
    p_transformed_autoriza: DataFrame,
) -> DataFrame:
    """Processes and integrates debit cards and POS transactions data.

    This task integrates the resulting data from the Data Transformation Layer,
    using features from bi debit card.


    Args:
        p_transformed_debit_card_account (DataFrame): Transformed debit card data.
        p_transformed_autoriza (DataFrame): Transformed POS transactions data.

    Returns:
        DataFrame: Unified and processed Dataframe.
    """
    df_processed_debit_card_acc = mc_debit_card_account_task(
        p_transformed_debit_card_account
    )

    df_processed_autoriza = mc_debit_card_transactions_task(
        p_transformed_autoriza.filter((f.col("au_id_aplicacion") == 6))
    )

    df_integrated_dc = df_processed_debit_card_acc.join(
        df_processed_autoriza,
        on=["id_cliente", "_observ_end_dt"],
        how="left",
    ).fillna(0)

    return df_integrated_dc


@flow(name="mc_debit_card_flow")
def mc_debit_card_flow():
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

    df_master_debit_card = mc_debit_card_task(
        data_transformation["transformed_debit_card"],
        data_transformation["transformed_autoriza"],
    )

    save_data_flow(df_master_debit_card)
