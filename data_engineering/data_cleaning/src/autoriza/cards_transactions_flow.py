from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    convert_to_hex_task,
    replace_null_or_empty_values,
)


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_transaccion", "id_cliente", "cuenta_corporativa"])
@task(
    name="clean_cards_transactions_task",
)
def clean_cards_transactions_task(
    p_raw_cards_transactions: DataFrame,
) -> DataFrame:
    """
    Cleans and processes card transactions authorized
    Args:
        p_raw_cards_transactions (DataFrame): raw data with card transactions.
    Returns:
        DataFrame: Processed card transactions DataFrame.
    """
    df_autoriza = (
        p_raw_cards_transactions.select(
            f.col("No_Autorizacion").alias("no_autorizacion"),
            convert_to_hex_task(f.col("cuenta_corporativa"), "cuenta"),
            f.col("mcc").alias("mcc"),
            f.when(
                f.col("categoria_descripcion").isNotNull(),
                f.lower(f.regexp_replace(f.col("categoria_descripcion"), " ", "_")),
            )
            .when(f.col("tipo_transaccion") == "26", "consumos")
            .when(
                f.lower(f.col("descripcion_tipo_transaccion")).contains("deposito"),
                "depositos",
            )
            .when(
                f.col("tipo_transaccion") == "30-CONSULTA",
                f.when(
                    f.lower(f.col("descripcion_tipo_transaccion")).contains(
                        "transferencia"
                    ),
                    "transferencias",
                ).otherwise("consultas"),
            )
            .when(
                (
                    (f.col("tipo_transaccion") == "11-QUASICASH")
                    | (f.col("tipo_transaccion") == "10")
                ),
                "consumos",
            )
            .when(f.col("tipo_transaccion") == "VA", "administrativas")
            .when(
                f.col("tipo_transaccion") == "00-COMPRA",
                f.when(
                    f.col("descripcion_tipo_transaccion").contains("CONSULTA"),
                    "consultas",
                ).otherwise("consumos"),
            )
            .when(f.col("tipo_transaccion") == "21", "administrativas")
            .when(f.trim(f.col("tipo_transaccion")) == "", "administrativas")
            .alias("descripcion_categoria_transaccion"),
            f.col("tipo_cuota"),
            f.col("meses_cuotas"),
            f.col("moneda").alias("id_moneda_origen_transaccion"),
            f.col("valor_gtq").cast("float").alias("monto_transaccion_gtq"),
            f.lower("Tipo_tarjeta").alias("tipo_tarjeta"),
            f.trim(f.lower(f.col("tipo_producto"))).alias("tipo_producto"),
            f.col("forma_trx_new"),
            f.when(f.lower(f.trim("red_descripcion")).contains("cajeros"), "atm_bi")
            .when(f.lower(f.trim("red_descripcion")).contains("5b"), "atm_5b")
            .when(
                f.lower(f.trim("red_descripcion")).contains("credomatic"),
                "atm_credomatic",
            )
            .otherwise("otros")
            .alias("red_atm"),
            f.col("fecha_transaccion"),
        )
        .withColumn(
            "tipo_compra",
            f.when(
                f.col("descripcion_categoria_transaccion") != "retiros",
                f.when(
                    (
                        (f.col("forma_trx_new").contains("INTERNET"))
                        | (f.col("forma_trx_new").contains("E-COMERCE"))
                    ),
                    "ecommerce",
                )
                .when((f.col("forma_trx_new").contains("CONTACTLESS")), "contactless")
                .when((f.col("forma_trx_new").contains("CHIP")), "chip")
                .when((f.col("forma_trx_new").contains("BANDA MAGNETICA")), "banda_mag")
                .when((f.col("forma_trx_new").contains("MANUAL")), "manual")
                .when((f.col("forma_trx_new").contains("RECURRENTE")), "recurrente")
                .otherwise("otros"),
            ).otherwise("retiros"),
        )
        .drop_duplicates()
    )

    return df_autoriza


@flow(name="cards_transactions_flow")
def cards_transactions_flow():
    """
    Loads, processes, and saves data in datalake.
    The flow performs the following operations:
    1. Loads raw data using the specified date range.
    2. Cleans and processes the all data.
    3. Saves the processed data to the appropriate environment using the specified
    overwrite strategy.
    Returns:
        None: This flow does not return a value, but it saves the processed data.
    """
    raw_data = load_raw_data_flow()

    df_card_transactions_final = clean_cards_transactions_task(
        raw_data["raw_cards_transactions"]
    )

    save_data_flow(df_card_transactions_final)
