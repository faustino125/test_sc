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
@task(name="clean_physical_channel_task", tags=["data cleaning", "physical channel"])
def clean_physical_channel_task(p_raw_physical_channel: DataFrame) -> DataFrame:
    """
    Ingest physical channel data into physical channel data cleaning layer.

    Args:
        p_raw_physical_channel (DataFrame): Physical channel raw data.

    Returns:
        DataFrame: Processed physical channel DataFrame.
    """

    df_physical_channel = p_raw_physical_channel.select(
        convert_to_hex_task("Codigo_Cliente", "cliente"),
        convert_to_hex_task("Dw_cuenta_Corporativa", "cuenta"),
        f.trim(f.col("Transaccion")).alias("id_transaccion"),
        f.trim(f.col("Dw_transaccion_descripcion")).alias(
            "descripcion_tipo_transaccion"
        ),
        f.trim(f.col("agencia")).alias("id_agencia"),
        f.trim(f.col("Dw_agencia_descripcion")).alias("descripcion_agencia"),
        f.col("Moneda").cast("int").alias("id_moneda"),
        f.trim(f.col("Dw_moneda_descripcion")).alias("descripcion_moneda"),
        f.col("Dw_Aplicacion_Codigo").cast("int").alias("id_aplicacion"),
        f.lower(
            f.regexp_replace(
                f.when(
                    f.col("Dw_Aplicacion_Descripcion") == "PLAN DORADO DE INVERSION",
                    f.lit("plan_dorado_inv"),
                )
                .when(
                    f.col("Dw_Aplicacion_Descripcion") == "PLAN FUTURO PROGRAMADO",
                    f.lit("plan_futuro_prog"),
                )
                .otherwise(f.col("Dw_Aplicacion_Descripcion")),
                " ",
                "_",
            )
        ).alias("descripcion_aplicacion"),
        f.col("Dw_Producto_Codigo").cast("int").alias("id_producto"),
        f.col("Dw_Producto_Descripcion").alias("descripcion_producto"),
        f.trim(f.col("Autorizacion")).alias("autorizacion"),
        f.trim(f.col("Documento")).alias("documento"),
        f.col("Dw_sumTotal").cast("float").alias("total_operacion"),
        f.col("reversion").cast("int").alias("id_reversion"),
        f.trim(f.col("Dw_reversion_descripcion")).alias("descripcion_reversion"),
        f.col("fecha_transaccion"),
    ).drop_duplicates()

    return df_physical_channel


@flow(name="physical_channel_flow")
def physical_channel_flow():
    """
    Loads, processes, and saves physical channel data in datalake.

    The flow performs the following operations:
    1. Loads raw physical channel data using the specified date range.
    2. Cleans and processes the physical channel data.
    3. Saves the processed data to the appropriate environment
       using the specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but it saves the physical channel data.
    """
    raw_data = load_raw_data_flow()

    df_physical_channel = raw_data["raw_branch_operations"]

    df_physical_channel_final = clean_physical_channel_task(df_physical_channel)

    save_data_flow(df_physical_channel_final)
