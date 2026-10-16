from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    clean_currency_task,
    convert_to_hex_task,
    replace_null_or_empty_values,
)


@replace_null_or_empty_values()
@arrange_columns(
    p_start_cols=["fecha_transaccion", "id_cliente_origen", "cuenta_origen"]
)
@task(name="clean_bel_web_transaction_task")
def clean_bel_web_transaction_task(
    p_raw_bel_transfers: DataFrame, df_raw_customer_products: DataFrame
) -> DataFrame:
    """Clean data about bel web transactions

    Args:
        p_raw_bel_transfers (DataFrame): Raw data bel transfers
        df_raw_customer_products (DataFrame): Raw customer products data.

    Returns:
        DataFrame: Processed bel transactions
    """
    df_customer_products = df_raw_customer_products.select(
        convert_to_hex_task("Codigo_Cliente", "cliente"),
        convert_to_hex_task("Cuenta_Corporativa", "cuenta"),
        f.regexp_replace(f.lower(f.col("Dw_Aplicacion_Descripcion")), " ", "_").alias(
            "descripcion_aplicacion"
        ),
    ).dropDuplicates()

    df_bel_web_transfers = p_raw_bel_transfers.filter(
        (f.col("monto_dolares") > 0) | (f.col("monto_quetzales") > 0)
    ).select(
        f.col("operacion_transaccion").alias("id_transaccion"),
        f.col("fecha_transaccion"),
        convert_to_hex_task(f.col("instalacion"), "id_instalacion"),
        convert_to_hex_task(f.col("codigo_cliente_origen"), "id_cliente_origen"),
        convert_to_hex_task(f.col("cuenta_origen"), "cuenta_origen"),
        f.when(
            f.col("descripcion_tipo_cuenta_origen").like("%credito%"),
            "tarjeta_de_credito",
        )
        .when(
            (~(f.col("descripcion_tipo_cuenta_origen").endswith("s"))),
            f.regexp_replace(f.trim("descripcion_tipo_cuenta_origen"), " ", "_") + "s",
        )
        .otherwise(f.regexp_replace(f.trim("descripcion_tipo_cuenta_origen"), " ", "_"))
        .alias("descripcion_cuenta_origen"),
        convert_to_hex_task(f.col("codigo_cliente_destino"), "id_cliente_destino"),
        convert_to_hex_task(f.col("Cuenta_Destino"), "cuenta_destino"),
        f.when(f.lower("dw_tipo_funcion").like("%ach%"), "interbancaria")
        .when(
            (f.lower("dw_tipo_funcion").like("%movil%"))
            & (f.col("descripcion_tipo_cuenta_destino").isNotNull()),
            "celular",
        )
        .when(
            f.col("descripcion_tipo_cuenta_destino").like("%credito"),
            "tarjeta_de_credito",
        )
        .when(
            (~(f.col("descripcion_tipo_cuenta_destino").endswith("s"))),
            f.regexp_replace(f.trim("descripcion_tipo_cuenta_destino"), " ", "_") + "s",
        )
        .otherwise(
            f.regexp_replace(f.trim("descripcion_tipo_cuenta_destino"), " ", "_")
        )
        .alias("descripcion_cuenta_destino"),
        f.when(
            (f.col("descripcion_moneda_destino") == "No Tiene")
            & (f.col("monto_quetzales") > 0),
            "gtq",
        )
        .when(
            (f.col("descripcion_moneda_destino") == "No Tiene")
            & (f.col("monto_dolares") > 0),
            "usd",
        )
        .otherwise(clean_currency_task(f.col("descripcion_moneda_destino")))
        .alias("descripcion_moneda_destino"),
        f.lower(f.trim(f.col("canal"))).alias("canal"),
        f.when(
            f.trim(f.col("dw_tipo_funcion")).like("%ACH%"),
            "transferencias_ach",
        )
        .when(
            f.trim(f.col("dw_tipo_funcion")).like("%fondos%"),
            "transferencias_propias",
        )
        .otherwise(
            f.regexp_replace(
                f.regexp_replace(
                    f.regexp_replace(
                        f.lower(f.trim("dw_tipo_funcion")),
                        r"transferencia\b",
                        "transferencias",
                    ),
                    r"\ba\b|\bde\b",
                    "",
                ),
                r"\s+",
                "_",
            )
        )
        .alias("descripcion_operacion"),
        f.coalesce(f.round("monto_quetzales", 2), f.lit("0"))
        .cast("float")
        .alias("monto_gtq"),
        f.coalesce(f.round("monto_dolares", 2), f.lit("0"))
        .cast("float")
        .alias("monto_usd"),
    )

    df_web_transfers = (
        df_bel_web_transfers.join(
            df_customer_products,
            (
                (
                    df_bel_web_transfers["id_cliente_origen"]
                    == df_customer_products["id_cliente"]
                )
                & (
                    df_bel_web_transfers["cuenta_origen"]
                    == df_customer_products["cuenta_corporativa"]
                )
            ),
            how="left",
        )
        .select(
            df_bel_web_transfers["*"],
            f.when(
                f.col("descripcion_cuenta_origen").isNull(),
                f.col("descripcion_aplicacion"),
            )
            .otherwise(f.col("descripcion_cuenta_origen"))
            .alias("descripcion_cuenta_origen"),
            f.when(
                f.col("monto_gtq") > 0,
                f.lit("gtq"),
            )
            .otherwise(f.lit("usd"))
            .alias("descripcion_moneda_origen"),
        )
        .drop(
            df_bel_web_transfers.descripcion_cuenta_origen,
        )
    )

    return df_web_transfers


@flow(name="bel_web_transfers_flow")
def bel_web_transfers_flow():
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

    df_raw_bel_transfers = raw_data["raw_bel_web_transfers"]
    df_raw_customer_products = raw_data["raw_products"]

    df_bel_web_transfers_final = clean_bel_web_transaction_task(
        df_raw_bel_transfers, df_raw_customer_products
    )

    save_data_flow(df_bel_web_transfers_final)
