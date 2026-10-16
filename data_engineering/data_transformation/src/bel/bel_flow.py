import pyspark.sql.functions as f
from prefect import flow, task
from pyspark.sql import Column, DataFrame

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_transformation.src.utils.utils import (
    add_exchange_rate_task,
    convert_currency_to_gtq_task,
    split_currency_task,
    sum_cond_task,
)


@task(name="sum_columns_task", tags=["data transformation", "task", "helper"])
def sum_columns_task(p_channel: str, p_add: str) -> Column:
    """
    sum all the transfers sent from a given channel. will sum amounts or quantities.

    Args:
        p_channel (str): Source channel for transaction.
        p_add (str): Column specification to sum.

    Returns:
        Column: Sum of columns.
    """
    return f.col(p_channel + "_transferencias_ach_enviadas_" + p_add) + f.col(
        p_channel + "_transferencias_terceros_enviadas_" + p_add
    )


@task(name="process_bel_transfers_task", tags=["data transformation", "processing"])
def process_bel_transfers_task(
    p_cleaned_customer_products: DataFrame,
    p_cleaned_bel_app_transfers: DataFrame,
    p_cleaned_bel_web_transfers: DataFrame,
    p_cleaned_exchange_rate: DataFrame,
) -> DataFrame:
    """
    transforms and processes both bel app and web transfers data.

    It performs a join between the app transactions and customer
    products, then a union with the web transactions. Finally, after grouping
    by customer id, account type and id, and a given time frame, it
    creates aggregation columns based on the transaction types available.

    Args:
        p_cleaned_customer_products (DataFrame): Cleaned customer products data.
        p_cleaned_bel_app_transfers (DataFrame): Cleaned bel app transactions data.
        p_cleaned_bel_web_transfers (DataFrame): Cleaned bel web transactions data.
        p_cleaned_daily_rate (DataFrame): Cleaned daily exchange data.

    Returns:
        DataFrame: Transformed bel transfers DataFrame.
    """
    customer_products = p_cleaned_customer_products.filter(
        f.col("id_aplicacion").isin([1, 2, 4])  # 2 = AHORROS, 1 = MONETARIOS, 4 = TC
    )

    ls_transfers_type = [
        "transferencias_terceros",
        "transferencias_ach",
        "transferencias_propias",
    ]

    ls_products_list = ["monetarios", "ahorros", "tarjeta_de_credito"]

    app_transfers = p_cleaned_bel_app_transfers.filter(
        (f.col("descripcion_operacion").isin(ls_transfers_type))
        & (f.col("descripcion_cuenta_origen").isin(ls_products_list))
    ).select(
        f.col("cuenta_origen").alias("cuenta_corporativa"),
        f.col("descripcion_cuenta_origen").alias("descripcion_aplicacion"),
        f.col("_observ_end_dt"),
        f.col("monto_debito"),
        f.col("descripcion_moneda_origen"),
        f.col("canal"),
        f.col("descripcion_operacion"),
        f.col("fecha_transaccion"),
    )

    app_transfers = app_transfers.join(
        customer_products,
        ["descripcion_aplicacion", "cuenta_corporativa", "_observ_end_dt"],
    ).select(
        f.col("id_cliente"),
        f.col("cuenta_corporativa"),
        f.col("descripcion_aplicacion").alias("descripcion_tipo_cuenta"),
        *split_currency_task("monto_debito", "descripcion_moneda_origen", True),
        f.col("canal"),
        f.col("descripcion_operacion"),
        app_transfers._observ_end_dt,
        f.col("fecha_transaccion"),
    )

    web_transfers = p_cleaned_bel_web_transfers.filter(
        (f.col("descripcion_operacion").isin(ls_transfers_type))
        & (f.col("descripcion_cuenta_origen").isin(ls_products_list))
    ).select(
        f.col("id_cliente_origen").alias("id_cliente"),
        f.col("cuenta_origen").alias("cuenta_corporativa"),
        f.col("descripcion_cuenta_origen").alias("descripcion_tipo_cuenta"),
        f.col("monto_gtq").alias("monto_debito_gtq"),
        f.col("monto_usd").alias("monto_debito_usd"),
        f.col("canal"),
        f.col("descripcion_operacion"),
        f.col("_observ_end_dt"),
        f.col("fecha_transaccion"),
    )

    df_transfers = add_exchange_rate_task(
        app_transfers.union(web_transfers), p_cleaned_exchange_rate
    ).select(
        f.col("*"),
        f.concat(
            f.col("canal"),
            f.lit("_"),
            f.col("descripcion_operacion"),
            f.lit("_enviadas"),
        ).alias("tipo_operacion"),
        (
            f.col("monto_debito_gtq")
            + convert_currency_to_gtq_task("monto_debito_usd", "tasa_cambio", "float")
        )
        .cast("float")
        .alias("monto_debito_quetzalizado"),
    )

    df_transfers = (
        df_transfers.groupBy(
            f.col("id_cliente"),
            f.col("descripcion_tipo_cuenta"),
            f.col("cuenta_corporativa"),
            f.col("_observ_end_dt"),
        )
        .pivot("tipo_operacion")
        .agg(
            (f.round(f.sum(f.col("monto_debito_gtq")), 2))
            .cast("float")
            .alias("monto_gtq"),
            (f.round(f.sum(f.col("monto_debito_usd")), 2))
            .cast("float")
            .alias("monto_usd"),
            (f.round(f.sum(f.col("monto_debito_quetzalizado")), 2))
            .cast("float")
            .alias("monto_quetzalizado"),
            (sum_cond_task(f.col("monto_debito_gtq") > 0, 1))
            .cast("int")
            .alias("gtq_cnt"),
            (sum_cond_task(f.col("monto_debito_usd") > 0, 1))
            .cast("int")
            .alias("usd_cnt"),
        )
        .fillna(0)
    )

    df_transfers = df_transfers.select(
        f.col("*"),
        sum_columns_task("app", "monto_gtq").alias("app_monto_transacciones_gtq"),
        sum_columns_task("app", "gtq_cnt").alias("app_transacciones_gtq_cnt"),
        sum_columns_task("app", "monto_usd").alias("app_monto_transacciones_usd"),
        sum_columns_task("app", "usd_cnt").alias("app_transacciones_usd_cnt"),
        sum_columns_task("app", "monto_quetzalizado").alias(
            "app_monto_transacciones_quetzalizado"
        ),
        sum_columns_task("web", "monto_gtq").alias("web_monto_transacciones_gtq"),
        sum_columns_task("web", "gtq_cnt").alias("web_transacciones_gtq_cnt"),
        sum_columns_task("web", "monto_usd").alias("web_monto_transacciones_usd"),
        sum_columns_task("web", "usd_cnt").alias("web_transacciones_usd_cnt"),
        sum_columns_task("web", "monto_quetzalizado").alias(
            "web_monto_transacciones_quetzalizado"
        ),
    )

    return df_transfers


@arrange_columns(p_start_cols=["fecha_informacion", "_particion", "id_cliente"])
@task(name="transform_digital_channel_task", tags=["data transformation", "processing"])
def transform_digital_channel_task(
    p_cleaned_customer_products: DataFrame,
    p_cleaned_bel_app_transfers: DataFrame,
    p_cleaned_bel_web_transfers: DataFrame,
    p_cleaned_exchange_rate: DataFrame,
) -> DataFrame:
    """
    Transforms and processes digital channel information to create
    digital channel features.

    This task processes the clean digital channel, employee, products, income,
    favorite channel, main address, ire, and bbk supplier data by
    applying transformation steps. It creates new columns based on joins,
    creates new data based on transformations, and joins them into a
    unified dataset.

    Args:
        p_cleaned_customer_products (DataFrame): Cleaned customer products.
        p_cleaned_bel_app_transfers (DataFrame): Cleaned bel app transfers.
        p_cleaned_bel_web_transfers: DataFrame: Cleaned bel web transfers.

    Returns:
        DataFrame: Digital Channel features DataFrame.
    """

    df_digital_channel_features = process_bel_transfers_task(
        p_cleaned_customer_products,
        p_cleaned_bel_app_transfers,
        p_cleaned_bel_web_transfers,
        p_cleaned_exchange_rate,
    )

    return df_digital_channel_features


@flow(name="bel_flow")
def bel_flow():
    """
    Loads, transforms and saves digital channel features in the data lake.

    The flow performs the following operations:
    1. Loads raw digital channel data using the specified date range.
    2. Transforms and processes the digital channel features data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        digital channel features data.
    """
    raw_data = load_raw_data_flow()

    df_digital_channel_features = transform_digital_channel_task(
        raw_data["cleaned_customer_products"],
        raw_data["cleaned_bel_app_transfers"],
        raw_data["cleaned_bel_web_transfers"],
        raw_data["cleaned_exchange_rate"],
    )

    save_data_flow(df_digital_channel_features)
