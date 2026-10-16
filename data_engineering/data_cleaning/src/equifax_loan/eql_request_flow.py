from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    clean_description_column_task,
    convert_to_hex_task,
    replace_null_or_empty_values,
)


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_catalogo"])
@task(
    name="clean_request_task",
    tags=["data cleaning", "preprocessing", "equifax Loan"],
)
def clean_request_task(
    p_raw_request: DataFrame,
) -> DataFrame:
    """
    Cleans and processes equifax's request raw data.

    This task processes the equifax's  request full load raw
    data, by applying transformations and cleaning steps.It standardizes
    certain fields, creates new calculated columns, and fills empty values
    with default values.

    Args:
        p_raw_request (DataFrame): person request raw data.

    Returns:
        DataFrame: Processed equifax's request DataFrame.
    """
    df_request_final = p_raw_request.select(
        convert_to_hex_task(f.col("numero_solicitud"), "id_numero_solicitud"),
        convert_to_hex_task(f.col("numero_caso_crm"), "id_crm"),
        f.col("moneda").cast("int").alias("id_moneda"),
        f.when(f.col("moneda") == 1, "gtq")
        .when(f.col("moneda") == 2, "usd")
        .otherwise(f.lower("dw_descripcion_moneda"))
        .alias("descripcion_moneda"),
        f.col("precio_venta").cast("float").alias("precio_venta"),
        f.col("monto_solicitud").cast("float").alias("monto_solicitud"),
        f.col("porcentaje_enganche").cast("float").alias("porcentaje_enganche"),
        f.col("valor_enganche").cast("float").alias("monto_enganche"),
        f.col("forma_pago").cast("int").alias("id_forma_pago"),
        clean_description_column_task(f.lower("dw_descripcion_forma_pago")).alias(
            "descripcion_forma_pago"
        ),
        f.col("valor_cuota").cast("float").alias("monto_cuota"),
        f.col("bandera_autorizacion").cast("int").alias("id_bandera_autorizacion"),
        f.when(f.col("bandera_autorizacion") == 0, "en_proceso")
        .when(f.col("bandera_autorizacion") == 1, "autorizada")
        .when(f.col("bandera_autorizacion") == 2, "rechazada")
        .when(f.col("bandera_autorizacion") == 3, "desistida")
        .otherwise("en_proceso")
        .alias("descripcion_bandera_autorizacion"),
        f.col("empresa").cast("int").alias("id_empresa"),
        clean_description_column_task(f.lower("dw_nombre_empresa")).alias(
            "descripcion_empresa"
        ),
        f.col("estado_solicitud").cast("int").alias("id_estado_solicitud"),
        clean_description_column_task(f.lower("descripcion_estado_solicitud")).alias(
            "descripclcion_estado_solicitud"
        ),
        f.col("etapa_tramite").cast("int").alias("id_etapa_tramite"),
        clean_description_column_task(f.lower("dw_descripcion_etapa_tramite")).alias(
            "descripcion_etapa_tramite"
        ),
        f.col("tipo_credito").cast("int").alias("id_tipo_credito"),
        clean_description_column_task(f.lower("dw_descripcion_tipo_credito")).alias(
            "descripcion_tipo_credito"
        ),
        f.col("garantias").cast("int").alias("id_garantia"),
        clean_description_column_task(f.lower("dw_descripcion_garantia")).alias(
            "descripcion_garantia"
        ),
        f.col("tasa_solicitud").cast("float").alias("tasa_solicitud"),
        f.col("plazo_anios_solicitado").cast("int").alias("plazo_anios_solicitado"),
        f.col("plazo_meses_solicitado").cast("int").alias("plazo_meses_solicitado"),
        f.col("plazo_dias_solicitado").cast("int").alias("plazo_dias_solicitado"),
        f.col("plazo_gracia").cast("int").alias("plazo_gracia"),
        f.col("vivienda_tasa_banco").cast("float").alias("vivienda_tasa_banco"),
        f.col("vivienda_tasa_gastos_administrativos")
        .cast("float")
        .alias("vivienda_tasa_gastos_administrativos"),
        f.col("vivienda_tasa_otros").cast("float").alias("vivienda_tasa_otros"),
        f.col("vivienda_comision_descuento")
        .cast("float")
        .alias("vivienda_comision_descuento"),
        f.col("tasa_fha").cast("float").alias("tasa_fha"),
        f.to_date(f.col("dw_fecha_autorizacion"), "yyyy-MM-dd").alias(
            "fecha_autorizacion"
        ),
        f.col("etapa_autorizacion").cast("int").alias("id_etapa_autorizacion"),
        clean_description_column_task(
            f.lower("dw_descripcion_etapa_tramite_aut")
        ).alias("descripcion_etapa_tramite_aut"),
        f.col("plazo_annio_autorizado").cast("int").alias("plazo_anios_autorizado"),
        f.col("plazo_meses_autorizado").cast("int").alias("plazo_meses_autorizado"),
        f.col("plazo_dias_autorizado").cast("int").alias("plazo_dias_autorizado"),
        clean_description_column_task(
            f.lower("dw_descripcion_estatus_autorizacion")
        ).alias("descripcion_estado_autorizacion"),
        f.col("tasa_autorizada").cast("float").alias("tasa_autorizada"),
        f.to_date(f.col("fecha_crea"), "yyyy-MM-dd").alias("fecha_crea"),
        f.to_date(f.col("fecha_etapa"), "yyyy-MM-dd").alias("fecha_etapa"),
        f.col("codigo_destino_sib").cast("int").alias("id_destino_sib"),
        clean_description_column_task(f.lower("descripcion_destino_sib")).alias(
            "descripcion_destino_sib"
        ),
        f.col("monto_autorizado").cast("float").alias("monto_autorizado"),
        f.col("monto_pagos_especiales").cast("float").alias("monto_pagos_especiales"),
        f.col("valor_seguro").cast("float").alias("monto_seguro"),
        f.col("valor_ultima_cuota").cast("float").alias("monto_ultima_cuota"),
        f.col("valor_gasto_administrativo")
        .cast("float")
        .alias("monto_gasto_administrativo"),
        f.col("comision_est_comercial").cast("float").alias("comision_est_comercial"),
        f.last_day(f.add_months(f.current_date(), -1)).alias("fecha_catalogo"),
    )

    return df_request_final


@flow(name="eqfLoan_person_request_flow")
def eql_request_flow():
    """
    Load, process, and save request data into the data lake.

    The flow performs the following operations:
    1. Loads request raw data.
    2. Cleans and processes request full_load data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        request data.
    """
    raw_data = load_raw_data_flow()

    df_request = raw_data["raw_eql_request"]

    df_request_final = clean_request_task(df_request)

    save_data_flow(df_request_final)
