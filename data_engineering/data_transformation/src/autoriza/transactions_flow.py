from functools import reduce
from operator import add

from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_transformation.src.utils.utils import (
    add_exchange_rate_task,
    join_dataframes_task,
)


def add_currency_colums(
    p_currency_col: str,
    p_col_trx_name: str,
    p_alias: str,
    p_exchange_rate: str,
) -> list:
    """Generates a list of columns by splitting the currency column.

    Args:
        p_currency_col (str): The name of the column containing currency information.
        p_col_trx_name (str): The name of the column containing amounts..
        p_alias (str): The alias for the generated columns.
        p_exchange_rate (str): The name of the column containing exchange rates.

    Returns:
        list: A list of columns generated from the input parameters.
    """
    currency_map = {"gtq": "_gtq", "usd": "_usd"}

    split_columns = [
        (
            f.when(
                f.col(p_currency_col) == key,
                f.col(p_col_trx_name),
            ).otherwise(0)
            if key == "gtq"
            else f.when(
                (f.col(p_currency_col) == key)
                | f.col(p_currency_col).isNull()
                | ~f.col(p_currency_col).isin("gtq", "usd"),
                f.col(p_col_trx_name).cast("float") / f.col(p_exchange_rate),
            ).otherwise(0)
        )
        .cast("float")
        .alias(f"{p_alias}{suffix}")
        for key, suffix in currency_map.items()
    ]
    ls_columns = f.col(p_col_trx_name).alias(f"{p_alias}_quetzalizado")

    return split_columns + [ls_columns]


@task(name="pivot_df_task", tags=["data transformation", "task", "helper"])
def pivot_df_task(
    p_df: DataFrame, p_pivot_col: str, p_sum_alias: str, p_count_alias: str = ""
) -> DataFrame:
    """Groups a DataFrame by cuenta_corporativa and _observ_end_dt.
    Then generates pivoted aggregation columns based on that groupBy statement.

    Args:
        df: Dataframe that will be grouped.
        p_pivot_col: column from dataframe used for pivot statement
        p_sum_alias: name for sum aggregation column.
        p_count_alias: name for count aggregation column.

    Returns:
        DataFrame: Pivoted DataFrame.
    """
    grouped_df = (
        p_df.groupBy(f.col("cuenta_corporativa"), f.col("_observ_end_dt"))
        .pivot(p_pivot_col)
        .agg(
            f.count("monto_transaccion_quetzalizado")
            .cast("int")
            .alias(p_count_alias + "cnt"),
            f.sum("monto_transaccion_quetzalizado").cast("float").alias(p_sum_alias),
        )
    ).fillna(0)

    return grouped_df


@task(
    name="process_atm_consultation_task",
    tags=["data transformation", "processing"],
)
def process_atm_consultation_task(
    p_cleaned_cards_transactions: DataFrame,
) -> DataFrame:
    """
    Transforms and processes autoriza ATM consultations.

    Args:
        p_cleaned_cards_transactions (DataFrame): Cleaned autoriza transactions.

    Returns:
        DataFrame: Transformed autoriza ATM consultations DataFrame.
    """

    atm_consultation_df = p_cleaned_cards_transactions.filter(
        (f.col("descripcion_categoria_transaccion").isin("atm_consultas"))
    )

    atm_consultations_df_pv = pivot_df_task(
        atm_consultation_df, "descripcion_categoria_transaccion", "monto"
    )

    return atm_consultations_df_pv.drop("atm_consultas_monto").fillna(0)


@task(
    name="process_transaction_type_task",
    tags=["data transformation", "processing"],
)
def process_transaction_type_task(
    p_cleaned_cards_transactions: DataFrame,
) -> DataFrame:
    """
    Transforms and processes autoriza transactions based on the transaction type.

    Args:
        p_cleaned_cards_transactions (DataFrame): Cleaned autoriza transactions.

    Returns:
        DataFrame: Transformed autoriza transaction types DataFrame.
    """
    pv_type_transactions_df = (
        p_cleaned_cards_transactions.filter(
            (
                (
                    ~f.col("descripcion_categoria_transaccion").isin(
                        "administrativas", "sin_uso", "desconocido", "atm_consultas"
                    )
                )
                & (f.col("monto_transaccion_quetzalizado") > 0)
            )
        )
        .groupBy(f.col("cuenta_corporativa"), f.col("_observ_end_dt"))
        .pivot("descripcion_categoria_transaccion")
        .agg(
            f.sum("monto_transaccion_gtq").alias("monto_total_trx_gtq"),
            f.count(f.when(f.col("id_moneda") == "gtq", 1)).alias("total_trx_gtq_cnt"),
            f.sum("monto_transaccion_usd").alias("monto_total_trx_usd"),
            f.count(f.when(f.col("id_moneda") == "usd", 1)).alias("total_trx_usd_cnt"),
            f.sum("monto_transaccion_quetzalizado").alias("monto_total_transacciones"),
            f.count("no_autorizacion").alias("total_transacciones_cnt"),
        )
        .fillna(0)
    )

    df_type_transactions = (
        pv_type_transactions_df.select(
            f.col("*"),
            reduce(
                add,
                [
                    f.col(x)
                    for x in pv_type_transactions_df.columns
                    if (("monto_total_trx_gtq" in x))
                ],
            ).alias("monto_total_transacciones_gtq"),
            reduce(
                add,
                [
                    f.col(x)
                    for x in pv_type_transactions_df.columns
                    if (("total_trx_gtq_cnt" in x))
                ],
            )
            .cast("int")
            .alias("total_transacciones_gtq_cnt"),
            reduce(
                add,
                [
                    f.col(x)
                    for x in pv_type_transactions_df.columns
                    if (("monto_total_trx_usd" in x))
                ],
            ).alias("monto_total_transacciones_usd"),
            reduce(
                add,
                [
                    f.col(x)
                    for x in pv_type_transactions_df.columns
                    if (("total_trx_usd_cnt" in x))
                ],
            )
            .cast("int")
            .alias("total_transacciones_usd_cnt"),
            reduce(
                add,
                [
                    f.col(x)
                    for x in pv_type_transactions_df.columns
                    if (("monto_total_transacciones" in x))
                ],
            ).alias("monto_total_transacciones"),
            reduce(
                add,
                [
                    f.col(x)
                    for x in pv_type_transactions_df.columns
                    if (("total_transacciones_cnt" in x))
                ],
            )
            .cast("int")
            .alias("total_transacciones_cnt"),
        )
        .withColumn(
            "monto_promedio_transacciones",
            f.col("monto_total_transacciones")
            / f.col("total_transacciones_cnt").cast("float"),
        )
        .fillna(0)
    )

    return df_type_transactions


@task(
    name="process_mcc_transactions_task",
    tags=["data transformation", "processing"],
)
def process_mcc_transactions_task(
    p_cleaned_cards_transactions: DataFrame,
) -> DataFrame:
    """
    Transforms and processes autoriza transactions based on the mcc group.

    Args:
        p_cleaned_cards_transactions (DataFrame): Cleaned autoriza transactions.

    Returns:
        DataFrame: Transformed autoriza mcc DataFrame
    """

    mcc_groups_df = p_cleaned_cards_transactions.filter(
        f.col("descripcion_categoria_transaccion").isin(
            "consumos", "transferencias", "depositos"
        )
    )

    mcc_groups_df = pivot_df_task(
        mcc_groups_df, "agrupacion_mcc", "mcc_monto_transacciones"
    )

    return mcc_groups_df


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente", "cuenta_corporativa"])
@task(
    name="transform_pos_transactions_task",
    tags=["data transformation", "processing"],
)
def transform_pos_transactions_task(
    p_cleaned_cards_transactions: DataFrame,
    p_cleaned_mcc_catalog: DataFrame,
    p_cleaned_daily_rate: DataFrame,
    p_cleaned_customer_products: DataFrame,
) -> DataFrame:
    """
    Transforms and processes transactions from POS to create features.

    Args:
        p_cleaned_cards_transactions (DataFrame): Cleaned autoriza transactions.
        p_cleaned_mcc_catalog (DataFrame): Cleaned mcc catalog.
        p_cleaned_customer_products (DataFrame): Cleaned customer products.

    Returns:
        DataFrame: Autoriza features DataFrame.
    """

    df_cleaned_daily_rate = p_cleaned_daily_rate.select(
        f.col("tasa_cambio"), f.col("_observ_end_dt"), f.col("fecha_transaccion")
    )
    df_customer_products = p_cleaned_customer_products.filter(
        f.col("id_aplicacion").isin(6, 4, 9)
    ).select(
        f.col("_observ_end_dt"),
        f.col("cuenta_corporativa"),
        f.col("id_cliente"),
        f.col("id_aplicacion"),
        f.col("descripcion_aplicacion"),
    )

    df_transactions_w_daily_rate = add_exchange_rate_task(
        p_cleaned_cards_transactions.select(
            p_cleaned_cards_transactions["*"],
            f.col("monto_transaccion_gtq"),
            f.when(f.col("id_moneda_origen_transaccion") == "320", f.lit("gtq"))
            .when(f.col("id_moneda_origen_transaccion") == "840", f.lit("usd"))
            .alias("id_moneda"),
        ),
        df_cleaned_daily_rate,
    )

    df_transactions = (
        df_transactions_w_daily_rate.alias("trx")
        .join(p_cleaned_mcc_catalog, "mcc")
        .select(
            f.col("no_autorizacion"),
            f.col("cuenta_corporativa"),
            f.when(
                (f.col("descripcion_mcc").contains("moneysend "))
                | (f.lower(f.col("agrupacion_mcc")).contains("transferencia"))
                | (f.lower(f.col("agrupacion_mcc")).contains("fondeo"))
                | (f.lower(f.col("descripcion_mcc")).contains("transferencia")),
                f.lit("transferencias"),
            )
            .when(
                f.col("descripcion_categoria_transaccion") == "consultas",
                f.lit("atm_consultas"),
            )
            .otherwise(
                f.when(
                    (
                        (f.col("descripcion_categoria_transaccion") == "retiros")
                        & (f.col("trx.mcc") == "6011")
                    ),
                    f.lit("atm_retiros"),
                )
                .when(
                    (
                        (f.col("descripcion_categoria_transaccion") == "retiros")
                        & (f.col("trx.mcc") != "6011")
                    ),
                    f.lit("otros_retiros"),
                )
                .otherwise(f.col("descripcion_categoria_transaccion"))
            )
            .alias("descripcion_categoria_transaccion"),
            f.col("id_moneda"),
            f.col("tipo_compra"),
            f.col("agrupacion_mcc"),
            f.col("fecha_transaccion"),
            f.col("red_atm"),
            f.col("trx._observ_end_dt"),
            f.col("trx._observ_start_dt"),
            *add_currency_colums(
                "id_moneda", "monto_transaccion_gtq", "monto_transaccion", "tasa_cambio"
            ),
        )
    )

    df_mcc_group = process_mcc_transactions_task(df_transactions)

    df_mcc_trx_type = process_transaction_type_task(df_transactions)

    df_atm_consultations = process_atm_consultation_task(df_transactions)

    df_trx_features = join_dataframes_task(
        ["cuenta_corporativa", "_observ_end_dt"],
        [
            df_customer_products,
            df_mcc_group,
            df_mcc_trx_type,
            df_atm_consultations,
        ],
    ).fillna(0)

    return df_trx_features


@flow(name="transactions_flow")
def transactions_flow():
    """
    Loads, transforms and saves autoriza features in the data lake.

    The flow performs the following operations:
    1. Loads cleaned autoriza data using the specified date range.
    2. Transforms and processes the autoriza features data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        autoriza features data.
    """
    raw_data = load_raw_data_flow()

    df_trx_features = transform_pos_transactions_task(
        raw_data["cleaned_cards_transactions"],
        raw_data["cleaned_mcc_catalog"],
        raw_data["cleaned_exchange_rate"],
        raw_data["cleaned_customer_products"],
    )

    save_data_flow(df_trx_features)
