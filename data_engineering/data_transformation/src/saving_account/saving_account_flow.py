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


@task(name="process_saving_account_task", tags=["data transformation", "processing"])
def process_saving_account_task(
    p_cleaned_daily_rate: DataFrame,
    p_cleaned_saving_account: DataFrame,
) -> DataFrame:
    """
    Transforms and processes saving account data.

    This task processes the clean saving account data by applying
    transformations and transforming steps. It standardizes column names, creates new
    columns based on transformations, and joins them into a unnified dataset.

    Args:
        p_cleaned_daily_rate (DataFrame): Cleaned daily rate data.
        p_cleaned_saving_account (DataFrame): Cleaned saving account data.

    Returns:
        DataFrame: Transformed saving account DataFrame.
    """

    df_cleaned_daily_rate = p_cleaned_daily_rate.select(
        f.col("tasa_cambio"), f.col("_observ_end_dt"), f.col("fecha_transaccion")
    )

    df_saving_account = add_exchange_rate_task(
        p_cleaned_saving_account, df_cleaned_daily_rate, True
    )

    df_saving_account = df_saving_account.select(
        "*",
        *split_currency_task("saldo_total", "id_moneda"),
        unified_currency_col_task("id_moneda", "saldo_total", "tasa_cambio").alias(
            "saldo_total_quetzalizado"
        ),
        *split_currency_task("saldo_promedio", "id_moneda"),
        unified_currency_col_task("id_moneda", "saldo_promedio", "tasa_cambio").alias(
            "saldo_promedio_quetzalizado"
        )
    ).drop("fecha_informacion", "saldo_promedio", "saldo_total")

    return df_saving_account


@task(
    name="process_saving_account_transactions_task",
    tags=["data transformation", "processing"],
)
def process_saving_account_transactions_task(
    p_cleaned_transactions: DataFrame, p_cleaned_daily_rate: DataFrame
) -> DataFrame:
    """
    Transforms and processes saving account transactional data.

    This task processes transactional saving account and daily exchange rate
    data by applying transformations and calculations. It standardizes
    column names, creates new columns based on transformations, and joins
    them into a unnified dataset.

    Args:
        p_cleaned_transactions (DataFrame): Cleaned saving account data.
        p_cleaned_daily_rate (DataFrame): Cleaned daily exchange data.

    Returns:
        DataFrame: Transformed saving account transactions DataFrame.
    """

    df_transactions = (
        add_exchange_rate_task(p_cleaned_transactions, p_cleaned_daily_rate)
        .filter(f.col("descripcion_transaccion") != "anotacion")
        .select(
            f.col("id_cliente"),
            f.col("cuenta_corporativa"),
            f.col("fecha_transaccion"),
            f.col("_observ_end_dt"),
            f.col("descripcion_transaccion"),
            f.col("descripcion_tipo_transaccion"),
            unified_currency_col_task(
                "id_moneda", "monto_transaccion", "tasa_cambio"
            ).alias("monto_transacciones_quetzalizado"),
            *split_currency_task("monto_transaccion", "id_moneda")
        )
    )

    df_agregations = df_transactions.groupBy(
        ["id_cliente", "cuenta_corporativa", "_observ_end_dt"]
    ).agg(
        f.sum(
            f.when(
                f.col("descripcion_tipo_transaccion") == "debito",
                f.col("monto_transaccion_gtq"),
            ).otherwise(0)
        ).alias("debitos_gtq"),
        f.count(
            f.when(
                (f.col("descripcion_tipo_transaccion") == "debito")
                & (f.col("monto_transaccion_gtq") > 0),
                f.lit(1),
            )
        ).alias("debitos_gtq_cnt"),
        f.sum(
            f.when(
                f.col("descripcion_tipo_transaccion") == "debito",
                f.col("monto_transaccion_usd"),
            ).otherwise(0)
        ).alias("debitos_usd"),
        f.count(
            f.when(
                (f.col("descripcion_tipo_transaccion") == "debito")
                & (f.col("monto_transaccion_usd") > 0),
                f.lit(1),
            )
        ).alias("debitos_usd_cnt"),
        f.sum(
            f.when(
                f.col("descripcion_tipo_transaccion") == "debito",
                f.col("monto_transacciones_quetzalizado"),
            ).otherwise(0)
        ).alias("debitos_quetzalizado"),
        f.sum(
            f.when(
                f.col("descripcion_tipo_transaccion") == "credito",
                f.col("monto_transaccion_gtq"),
            ).otherwise(0)
        ).alias("creditos_monto_gtq"),
        f.count(
            f.when(
                (f.col("descripcion_tipo_transaccion") == "credito")
                & (f.col("monto_transaccion_gtq") > 0),
                f.lit(1),
            )
        ).alias("creditos_gtq_cnt"),
        f.sum(
            f.when(
                f.col("descripcion_tipo_transaccion") == "credito",
                f.col("monto_transaccion_usd"),
            ).otherwise(0)
        ).alias("creditos_usd"),
        f.count(
            f.when(
                (f.col("descripcion_tipo_transaccion") == "credito")
                & (f.col("monto_transaccion_usd") > 0),
                f.lit(1),
            )
        ).alias("creditos_usd_cnt"),
        f.sum(
            f.when(
                f.col("descripcion_tipo_transaccion") == "credito",
                f.col("monto_transacciones_quetzalizado"),
            ).otherwise(0)
        ).alias("creditos_quetzalizado"),
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

    return join_dataframes_task(
        ["id_cliente", "cuenta_corporativa", "_observ_end_dt"],
        [df_segmented_transactions, df_agregations],
    )


@arrange_columns(p_start_cols=["_observ_end_dt", "_particion", "id_cliente"])
@task(name="transform_saving_account_task", tags=["data transformation", "processing"])
def transform_saving_account_task(
    p_cleaned_exchange_rate: DataFrame,
    p_cleaned_saving_account: DataFrame,
    p_cleaned_saving_account_transaction: DataFrame,
) -> DataFrame:
    """
    Transforms and processes customer information to create customer features.

    This task processes the clean customer, employee, products, income,
    main address, ire, and bbk supplier data by
    applying transformation steps. It creates new columns based on joins,
    creates new data based on transformations, and joins them into a
    unnified dataset.

    Args:
       p_cleaned_exchange_rate (DataFrame): Cleaned customer data.
       p_cleaned_saving_account (DataFrame): Cleaned saving account data.
       p_cleaned_saving_account_transaction (DataFrame): Cleaned saving account
       transactional data.

    Returns:
        DataFrame: Customer features DataFrame.
    """

    df_base_saving_account = process_saving_account_task(
        p_cleaned_exchange_rate, p_cleaned_saving_account
    )
    df_saving_account_transaction = process_saving_account_transactions_task(
        p_cleaned_exchange_rate, p_cleaned_saving_account_transaction
    )

    df_saving_account_features = join_dataframes_task(
        ["id_cliente", "cuenta_corporativa", "_observ_end_dt"],
        [
            df_base_saving_account,
            df_saving_account_transaction,
        ],
    )
    return df_saving_account_features


@flow(name="saving_account_flow")
def saving_account_flow():
    """
    Loads, transforms and saves saving account features in the data lake.

    The flow performs the following operations:
    1. Loads raw saving account data using the specified date range.
    2. Transforms and processes the saving account features data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        saving account features data.
    """
    raw_data = load_raw_data_flow()

    df_saving_account_features = transform_saving_account_task(
        raw_data["cleaned_exchange_rate"],
        raw_data["cleaned_saving_account"],
        raw_data["cleaned_saving_account_transaction"],
    )

    save_data_flow(df_saving_account_features)
