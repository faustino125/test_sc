from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_transformation.src.utils.utils import (
    add_exchange_rate_task,
    create_currency_col_task,
    join_dataframes_task,
    sum_cond_task,
)


def drop_columns_task(df: DataFrame) -> DataFrame:
    columns_to_exclude = {
        "id_cliente",
        "cuenta_corporativa",
        "_observ_end_dt",
        "flag_cuenta_distinta",
    }

    col_validate = (
        df.select(
            [
                f.sum(col).alias(col)
                for col in df.columns
                if col not in columns_to_exclude
            ]
        )
        .head()
        .asDict()
    )

    columns_to_drop = [
        col
        for col in df.columns
        if col not in columns_to_exclude
        and (col_validate.get(col, 0) == 0 or col_validate.get(col, 0) is None)
    ]

    return df.drop(*columns_to_drop)


@task(
    name="process_bi_banking_transactions_task",
    tags=["data transformation", "processing"],
)
def process_bi_banking_transactions_task(
    p_cleaned_bbk_transactions: DataFrame, p_cleaned_daily_rate: DataFrame
) -> DataFrame:
    """
    Transforms and processes BI Banking transactional data.

    This task processes transactional BI Banking and daily exchange rate
    data by applying transformations and calculations. It standardizes
    column names, creates new columns based on transformations, and joins
    them into a unnified dataset.

    Args:
        p_cleaned_bbk_transactions (DataFrame): Cleaned transactional BI Banking data.
        p_cleaned_daily_rate (DataFrame): Cleaned daily exchange data.

    Returns:
        DataFrame: Transformed BI Banking transactions DataFrame.
    """

    df_transaction = add_exchange_rate_task(
        p_cleaned_bbk_transactions, p_cleaned_daily_rate
    ).select(
        f.col("id_cliente"),
        f.col("cuenta_corporativa"),
        f.col("_observ_end_dt"),
        f.col("modulo_operacion"),
        f.col("id_moneda"),
        *create_currency_col_task("id_moneda", "monto", "tasa_cambio"),
        f.col("flag_cuenta_distinta")
    )

    df_account_flg_trx = df_transaction.groupBy(
        "id_cliente", "cuenta_corporativa", "_observ_end_dt"
    ).agg(
        f.max(f.col("flag_cuenta_distinta")).alias("flag_cuenta_distinta"),
        f.count("*").cast("int").alias("transacciones_cnt"),
    )

    df_bbk_transaction = (
        df_transaction.groupBy("id_cliente", "cuenta_corporativa", "_observ_end_dt")
        .pivot("modulo_operacion")
        .agg(
            sum_cond_task(f.col("id_moneda") == 1, 1).alias("gtq_cnt"),
            f.sum(f.col("monto_gtq")).cast("float").alias("gtq"),
            sum_cond_task(f.col("id_moneda") == 2, 1).alias("usd_cnt"),
            f.sum(f.col("monto_usd")).cast("float").alias("usd"),
            f.count("cuenta_corporativa").cast("int").alias("quetzalizado_cnt"),
            f.sum(f.col("monto_quetzalizado")).cast("float").alias("quetzalizado"),
        )
        .fillna(0)
    )
    df_selected_transactions = drop_columns_task(df_bbk_transaction)

    return join_dataframes_task(
        ["id_cliente", "cuenta_corporativa", "_observ_end_dt"],
        [df_selected_transactions, df_account_flg_trx],
    )


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente", "cuenta_corporativa"])
@task(name="transform_bi_banking_task", tags=["data transformation", "process"])
def transform_bi_banking_task(
    p_cleaned_customer_products: DataFrame,
    p_cleaned_bbk_transaction: DataFrame,
    p_cleaned_exchange_rate: DataFrame,
) -> DataFrame:
    """
    Transforms and processes transactional BI Banking information to create features.

    This task processes the clean daily exchange data and cleaned transactional BI
    Banking data, applying transformation steps. It creates new columns
    based on joins, creates new data based on transformations, and joins them into a
    unnified dataset.

    Args:
        p_cleaned_customer_products (DataFrame): Cleaned customer-product catalog data.
        p_cleaned_bbk_transaction (DataFrame): Cleaned transactional BI Banking data.
        p_cleaned_exchange_rate (DataFrame): Cleaned daily exchange rate data.
        transactions data.

    Returns:
        DataFrame: BI Banking features DataFrame.
    """

    df_bi_banking_transaction = process_bi_banking_transactions_task(
        p_cleaned_bbk_transaction, p_cleaned_exchange_rate
    )

    df_selected_customer_products = p_cleaned_customer_products.select(
        "id_cliente", "cuenta_corporativa", "_observ_end_dt"
    )

    df_bi_banking_features = join_dataframes_task(
        ["id_cliente", "cuenta_corporativa", "_observ_end_dt"],
        [df_bi_banking_transaction, df_selected_customer_products],
    )
    return df_bi_banking_features


@flow(name="bi_banking_flow")
def bi_banking_flow():
    """
    Loads, transforms and saves transactional BI Banking features in the data lake.

    The flow performs the following operations:
    1. Loads raw customer data using the specified date range.
    2. Transforms and processes the customer features data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        transactional BI Banking features data.
    """
    raw_data = load_raw_data_flow()

    df_bi_banking_features = transform_bi_banking_task(
        raw_data["cleaned_customer_products"],
        raw_data["cleaned_bbk_transaction"],
        raw_data["cleaned_exchange_rate"],
    )

    save_data_flow(df_bi_banking_features)
