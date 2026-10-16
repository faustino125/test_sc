from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    bad_situation_product_flag_task,
    clean_currency_task,
    convert_to_hex_task,
    replace_null_or_empty_values,
    std_situation_account_task,
)


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_informacion", "id_cliente", "cuenta_corporativa"])
@task(
    name="clean_debit_card_task",
    tags=["data cleaning", "preprocessing", "debit card"],
)
def clean_debit_card_task(p_raw_debit_card: DataFrame) -> DataFrame:
    """Cleans the raw debit card data by selecting relevant columns and arranging them.

    This function applies the following transformations:
    1. Replaces null or empty values in the data.
    2. Arranges the columns in the specified order.

    Args:
        p_raw_debit_card (DataFrame): The raw debit card data to be cleaned.

    Returns:
        DataFrame: The cleaned debit card data.
    """
    df_clean_account_description = p_raw_debit_card.select(
        "*",
        f.lower(
            f.regexp_replace(
                f.col("dw_situacion_cuenta_descripcion"), r"^\s+|\s+$", str()
            )
        ).alias("descripcion_situacion_cuenta"),
    )
    df_debit_card = df_clean_account_description.select(
        convert_to_hex_task(f.col("codigo_cliente"), "cliente"),
        convert_to_hex_task(f.col("dw_cuenta_corporativa"), "cuenta"),
        convert_to_hex_task(f.col("dw_cuenta_mon_aho"), "cuenta_ahorro_monetaria"),
        f.col("clase_tarjeta").cast("int").alias("id_clase_tarjeta"),
        f.lower("dw_clase_tarjeta_descripcion").alias("descripcion_clase_tarjeta"),
        f.col("dw_aplicacion_codigo").cast("int").alias("id_producto"),
        f.lower(f.regexp_replace(f.trim("dw_aplicacion_descripcion"), " ", "_")).alias(
            "descripcion_producto"
        ),
        f.col("dw_moneda_codigo").cast("int").alias("id_moneda"),
        clean_currency_task("dw_moneda_codigo").alias("descripcion_moneda"),
        f.col("situacion_cuenta").cast("int").alias("id_situacion_cuenta"),
        f.col("descripcion_situacion_cuenta"),
        std_situation_account_task(f.col("descripcion_situacion_cuenta")),
        bad_situation_product_flag_task(
            std_situation_account_task(f.col("descripcion_situacion_cuenta"))
        ),
        f.col("tipo_tarjeta").cast("int").alias("id_tipo_tarjeta"),
        f.lower("dw_tipo_tarjeta_descripcion").alias("descripcion_tipo_tarjeta"),
        f.to_date("fecha_asegurado").alias("fecha_asegurado"),
        f.when(
            f.to_date("fecha_asegurado").isNotNull()
            & f.col("descripcion_situacion_cuenta").isin(["sobregiro", "vigente"]),
            1,
        )
        .otherwise(0)
        .alias("flag_asegurado"),
        f.to_date("fecha_autorizacion").alias("fecha_autorizacion"),
        f.to_date("fecha_emision_plastico").alias("fecha_emision_plastico"),
        f.to_date("fecha_ultima_modif_situacion").alias("fecha_ultima_modificacion"),
        f.to_date("fecha_venc_plastico").alias("fecha_vencimiento_plastico"),
        f.col("monto_max_consumo_ext")
        .cast("float")
        .alias("monto_max_consumo_exterior_gtq"),
        f.col("monto_max_consumo_ext_d")
        .cast("float")
        .alias("monto_max_consumo_exterior_usd"),
        f.col("monto_max_consumo_local")
        .cast("float")
        .alias("monto_max_consumo_local_gtq"),
        f.col("monto_max_consumo_local_d")
        .cast("float")
        .alias("monto_max_consumo_local_usd"),
        f.col("monto_max_retiro_ext")
        .cast("float")
        .alias("monto_max_retiro_exterior_gtq"),
        f.col("monto_max_retiro_ext_d")
        .cast("float")
        .alias("monto_max_retiro_exterior_usd"),
        f.col("monto_max_retiro_local")
        .cast("float")
        .alias("monto_max_retiro_local_gtq"),
        f.col("monto_max_retiro_local_d")
        .cast("float")
        .alias("monto_max_retiro_local_usd"),
        f.col("fecha_informacion"),
    )

    # CIF '00000000'
    df_debit_card_final = df_debit_card.filter(
        f.col("id_cliente") != "0xB02132081808B493C61E86626EE6C2E29326A662"
    )

    return df_debit_card_final


@flow(name="debit_card_flow")
def debit_card_flow():
    """
    Load, process, and save debit card account data in the data lake.

    The flow performs the following operations:
    1. Loads debit card account raw data using the specified date range.
    2. Cleans and processes the debit card account data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        debit card account data.
    """
    raw_data = load_raw_data_flow()

    raw_debit_card = raw_data["raw_debit_card"]

    df_debit_card_final = clean_debit_card_task(raw_debit_card)

    save_data_flow(df_debit_card_final)
