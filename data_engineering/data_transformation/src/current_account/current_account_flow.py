from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_transformation.src.utils.utils import (
    add_exchange_rate_task,
    join_dataframes_task,
    split_currency_task,
    sum_cond_task,
    unified_currency_col_task,
)


@task(name="process_account_task", tags=["data transformation", "processing"])
def process_account_task(
    p_cleaned_current_account: DataFrame, p_cleaned_daily_rate: DataFrame
) -> DataFrame:
    """
    Transforms and processes current account monthly data.

    This task processes the clean current account and daily exchange rate
    data by applying transformationsand calculations to add a balance
    column separated by currency abreviation. It also returns the current
    account columns as they were in the data cleaning layer except for
    `fecha_informacion`.

    Args:
        p_cleaned_current_account (DataFrame): Cleaned current account data.
        p_cleaned_daily_rate (DataFrame): Cleaned daily exchange rate.

    Returns:
        DataFrame: Transformed base current account DataFrame.
    """
    df_cleaned_daily_rate = p_cleaned_daily_rate.select(
        f.col("tasa_cambio"), f.col("_observ_end_dt"), f.col("fecha_transaccion")
    )
    df_status = add_exchange_rate_task(
        p_cleaned_current_account, df_cleaned_daily_rate, True
    )

    df_status = df_status.select(
        "*",
        *split_currency_task("saldo_total", "id_moneda"),
        unified_currency_col_task("id_moneda", "saldo_total", "tasa_cambio").alias(
            "saldo_total_quetzalizado"
        ),
        *split_currency_task("saldo_promedio", "id_moneda"),
        unified_currency_col_task("id_moneda", "saldo_promedio", "tasa_cambio").alias(
            "saldo_promedio_quetzalizado"
        ),
    ).drop("fecha_informacion", "saldo_promedio")
    return df_status


@task(
    name="process_currency_transactions_task",
    tags=["data transformation", "processing"],
)
def process_currency_transactions_task(
    p_cleaned_transactions: DataFrame, p_cleaned_daily_rate: DataFrame
) -> DataFrame:
    """
    Transforms and processes current account transactional data.

    This task processes transactional current account and daily exchange rate
    data by applying transformations and calculations. It standardizes
    column names, creates new columns based on transformations, and joins
    them into a unnified dataset.

    Args:
        p_cleaned_transactions (DataFrame): Cleaned transactional current account data.
        p_cleaned_daily_rate (DataFrame): Cleaned daily exchange data.

    Returns:
        DataFrame: Transformed current account transactions DataFrame.
    """

    df_transactions = add_exchange_rate_task(
        p_cleaned_transactions, p_cleaned_daily_rate
    ).select(
        f.col("id_cliente"),
        f.col("cuenta_corporativa"),
        f.col("fecha_transaccion"),
        f.col("_observ_end_dt"),
        f.col("descripcion_transaccion"),
        f.col("descripcion_tipo_transaccion"),
        unified_currency_col_task(
            "id_moneda", "monto_transaccion", "tasa_cambio"
        ).alias("monto_transacciones_quetzalizado"),
        f.col("descripcion_legajo"),
        f.col("legajo"),
        *split_currency_task("monto_transaccion", "id_moneda"),
    )

    df_segmented_transactions = (
        df_transactions.groupBy(["id_cliente", "cuenta_corporativa", "_observ_end_dt"])
        .pivot("descripcion_transaccion")
        .agg(
            f.sum(f.col("monto_transaccion_gtq")).alias("monto_gtq"),
            sum_cond_task(f.col("monto_transaccion_gtq") > 0, 1).alias("gtq_cnt"),
            f.sum(f.col("monto_transaccion_usd")).alias("monto_usd"),
            sum_cond_task(f.col("monto_transaccion_usd") > 0, 1).alias("usd_cnt"),
            f.sum(f.col("monto_transacciones_quetzalizado")).alias(
                "monto_quetzalizado"
            ),
        )
    )

    df_segmented_transactions = df_segmented_transactions.fillna(0).select(
        "*",
        (f.col("deposito_monto_gtq") + f.col("nota_credito_monto_gtq")).alias(
            "creditos_monto_gtq"
        ),
        (f.col("deposito_gtq_cnt") + f.col("nota_credito_gtq_cnt")).alias(
            "creditos_gtq_cnt"
        ),
        (f.col("deposito_monto_usd") + f.col("nota_credito_monto_usd")).alias(
            "creditos_usd"
        ),
        (f.col("deposito_usd_cnt") + f.col("nota_credito_usd_cnt")).alias(
            "creditos_usd_cnt"
        ),
        (
            f.col("deposito_monto_quetzalizado")
            + f.col("nota_credito_monto_quetzalizado")
        ).alias("creditos_quetzalizado"),
        (
            f.col("debito_compensacion_monto_gtq")
            + f.col("nota_debito_monto_gtq")
            + f.col("pago_cheque_monto_gtq")
            + f.col("debito_sobregiro_monto_gtq")
        ).alias("debitos_gtq"),
        (
            f.col("debito_compensacion_gtq_cnt")
            + f.col("nota_debito_gtq_cnt")
            + f.col("pago_cheque_gtq_cnt")
            + f.col("debito_sobregiro_gtq_cnt")
        ).alias("debitos_gtq_cnt"),
        (
            f.col("debito_compensacion_monto_usd")
            + f.col("nota_debito_monto_usd")
            + f.col("pago_cheque_monto_usd")
            + f.col("debito_sobregiro_monto_usd")
        ).alias("debito_usd"),
        (
            f.col("debito_compensacion_usd_cnt")
            + f.col("nota_debito_usd_cnt")
            + f.col("pago_cheque_usd_cnt")
            + f.col("debito_sobregiro_usd_cnt")
        ).alias("debitos_usd_cnt"),
        (
            f.col("debito_compensacion_monto_quetzalizado")
            + f.col("nota_debito_monto_quetzalizado")
            + f.col("pago_cheque_monto_quetzalizado")
            + f.col("debito_sobregiro_monto_quetzalizado")
        ).alias("debitos_quetzalizado"),
    )

    COMMON_JURIDIC_CLIENT_CHANNELS = [592, 900, 437, 561, 718, 564, 333]

    df_credit_operations = (
        df_transactions.filter(
            (f.col("descripcion_tipo_transaccion") == "CREDITO")
            & (f.col("legajo").isin(COMMON_JURIDIC_CLIENT_CHANNELS))
        )
        .groupBy(
            "id_cliente",
            "cuenta_corporativa",
            "_observ_end_dt",
        )
        .pivot("descripcion_legajo")
        .agg(
            f.sum(f.col("monto_transaccion_gtq")).alias("monto_creditos_gtq"),
            sum_cond_task(f.col("monto_transaccion_gtq") > 0, 1).alias(
                "creditos_gtq_cnt"
            ),
            f.sum(f.col("monto_transaccion_usd")).alias("monto_creditos_usd"),
            sum_cond_task(f.col("monto_transaccion_usd") > 0, 1).alias(
                "creditos_usd_cnt"
            ),
            f.sum(f.col("monto_transacciones_quetzalizado")).alias(
                "monto_creditos_quetzalizado"
            ),
        )
    )

    return join_dataframes_task(
        ["id_cliente", "cuenta_corporativa", "_observ_end_dt"],
        [df_segmented_transactions, df_credit_operations],
    )


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente", "cuenta_corporativa"])
@task(name="transform_current_account_task", tags=["data transformation", "processing"])
def transform_current_account_task(
    p_cleaned_current_account: DataFrame,
    p_cleaned_exchange_rate: DataFrame,
    p_cleaned_current_account_transaction: DataFrame,
) -> DataFrame:
    """
    Transforms and processes current account information to create features.

    This task processes the clean daily exchange data, cleaned current account base,
    and transactions data, by applying transformation steps. It creates new columns
    based on joins, creates new data based on transformations, and joins them into a
    unnified dataset.

    Args:
        p_cleaned_current_account (DataFrame): Cleaned current account base data.
        p_cleaned_exchange_rate (DataFrame): Cleaned daily exchange rate data.
        p_cleaned_current_account_transaction (DataFrame): Cleaned current account
        transactions data.

    Returns:
        DataFrame: Current Account features DataFrame.
    """
    df_base_current_account = process_account_task(
        p_cleaned_current_account, p_cleaned_exchange_rate
    )
    df_transactional_current_account = process_currency_transactions_task(
        p_cleaned_current_account_transaction, p_cleaned_exchange_rate
    )

    df_current_account_features = join_dataframes_task(
        ["id_cliente", "cuenta_corporativa", "_observ_end_dt"],
        [df_base_current_account, df_transactional_current_account],
    )
    return df_current_account_features


@flow(name="current_account_flow")
def current_account_flow():
    """
    Loads, transforms and saves current account features in the data lake.

    The flow performs the following operations:
    1. Loads raw customer data using the specified date range.
    2. Transforms and processes the customer features data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        current account features data.
    """
    raw_data = load_raw_data_flow()

    df_current_account_features = transform_current_account_task(
        raw_data["cleaned_current_account"],
        raw_data["cleaned_exchange_rate"],
        raw_data["cleaned_current_account_transaction"],
    )

    save_data_flow(df_current_account_features)
