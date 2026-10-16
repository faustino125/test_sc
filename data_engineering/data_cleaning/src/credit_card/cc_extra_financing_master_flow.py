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
@arrange_columns(p_start_cols=["fecha_informacion", "id_cliente", "cuenta_corporativa"])
@task(
    name="clean_cc_extra_financing_master_task", tags=["data cleaning", "preprocessing"]
)
def clean_cc_extra_financing_master_task(
    p_raw_cc_extra_financing: DataFrame,
) -> DataFrame:
    """
    Cleans and processes credit card extra financing data
    It standardizes certain fields, creates new calculated columns,
    and fills empty values with default values.

    Args:
        p_raw_cc_extra_financing (DataFrame): Raw credit card extra financing.

    Returns:
        DataFrame: Processed credit card extra financing DataFrame.
    """
    df_cc_extra_financing = p_raw_cc_extra_financing.select(
        convert_to_hex_task(f.col("codigo_Cliente"), "cliente"),
        convert_to_hex_task(f.col("cuenta_corporativa"), "cuenta"),
        f.col("fecha_extrafinanciamiento").cast("date").alias("fecha_financiamiento"),
        f.col("plazo").cast("int").alias("plazo_meses"),
        f.when(f.col("saldo_pendiente") > 0, "activo")
        .otherwise("cancelado")
        .alias("estado_financiamiento"),
        f.col("num_promocion").alias("id_promocion"),
        f.regexp_replace(
            f.trim(f.lower(f.col("dw_num_promocion_descripcion"))), " ", "_"
        ).alias("descripcion_promocion"),
        f.col("codigo_moneda").alias("id_moneda"),
        clean_currency_task(f.lower("dw_moneda_descripcion")).alias(
            "descripcion_moneda"
        ),
        f.col("monto_total").cast("decimal(18,2)").alias("monto_total"),
        f.when(f.col("saldo_pendiente") == 0, 0)
        .when(
            f.col("saldo_pendiente") < f.col("valor_cuota_mensual"),
            f.col("saldo_pendiente"),
        )
        .otherwise(f.col("valor_cuota_mensual"))
        .cast("decimal(18,2)")
        .alias("cuota_mensual"),
        f.col("saldo_pendiente").cast("decimal(18,2)").alias("saldo_pendiente"),
        f.col("secuencia"),
        f.col("cod_establecimiento").alias("codigo_establecimiento"),
        f.col("fecha_informacion"),
    )

    df_final = df_cc_extra_financing.select(
        f.col("*"),
        f.when(
            (f.col("codigo_establecimiento").isin("18844100", "18843300"))
            | (f.col("id_promocion") == "018")
            | (f.col("id_promocion") == "002"),
            "convenio_de_pago",
        )
        .when(f.col("id_promocion") == "001", "extrafinanciamiento")
        .when(f.col("id_promocion").isin("013", "005", "015"), "neocuotas")
        .when(f.col("id_promocion") == "016", "traslado_a_cuotas")
        .when(f.col("id_promocion") == "017", "unificacion_saldos")
        .otherwise("otros")
        .alias("agrupacion_promocion"),
    )

    df_final = df_final.filter(f.col("agrupacion_promocion") != "otros")

    return df_final


@flow(name="cc_extra_financing_master_flow")
def cc_extra_financing_master_flow():
    """
    Load, process, and save credit card extra financing data in the data lake.

    This flow performs the following operations:
    1. Loads raw credit card extra financing data using the specified date range.
    2. Cleans and processes the credit card extra financing data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed data.
    """
    raw_data = load_raw_data_flow()

    df_cc_extra_financing = raw_data["raw_cc_extra_financing"]

    df_cc_extra_financing = clean_cc_extra_financing_master_task(df_cc_extra_financing)

    save_data_flow(df_cc_extra_financing)
