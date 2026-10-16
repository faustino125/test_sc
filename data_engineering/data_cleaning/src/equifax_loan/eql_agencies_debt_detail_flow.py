from prefect import flow, task
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    clean_description_column_task,
    convert_to_hex_task,
    get_cols_task,
    replace_null_or_empty_values,
)


@task(
    name="past_due_flag_task",
    tags=["data cleaning", "preprocessing", "equifax loan"],
)
def past_due_flag_task(p_id_classification_type: Column) -> Column:
    """
    build past due flag

    Args:
        p_id_classification_type (Column): classification type.

    Returns:
        Column: past due flag.
    """
    return (
        f.when(p_id_classification_type == "", None)
        .when(p_id_classification_type == "a", 0)
        .when(p_id_classification_type == "ab", 30)
        .when(p_id_classification_type == "b", 60)
        .when(p_id_classification_type == "bc", 90)
        .when(p_id_classification_type.isin("c", "cb"), 120)
        .when(p_id_classification_type == "d", 121)
        .otherwise("")
    )


@task(
    name="map_status_task",
    tags=["data cleaning", "preprocessing", "equifax loan"],
)
def map_status_task(p_id_classification_type: Column) -> Column:
    """
    Retrives the current status of the specified entity

    Args:
        p_classification_type (Column): classification type.

    Returns:
        Column: classification type aggrupations.
    """
    return (
        f.when(p_id_classification_type == "", "sin_clasificacion")
        .when(p_id_classification_type == "a", "al_dia")
        .when(
            p_id_classification_type.isin("ab", "b", "bc", "c", "cb", "d", "o"), "mora"
        )
        .when(p_id_classification_type == "ad", "cobro_administrativo")
        .when(p_id_classification_type == "bq", "bloqueada")
        .when(p_id_classification_type == "ca", "cancelada")
        .when(p_id_classification_type == "cj", "cobro_juridico")
        .when(p_id_classification_type == "f", "cheque_rechazado")
        .when(p_id_classification_type == "m", "malo")
        .when(p_id_classification_type == "pt", "puntual")
        .when(p_id_classification_type == "r", "regular")
        .when(p_id_classification_type == "z", "incobrable")
        .otherwise("")
    )


@task(
    name="asset_type_task",
    tags=["data cleaning", "preprocessing", "equifax loan"],
)
def asset_type_task(
    p_reference1_type: Column, p_reference2_type: Column, p_reference3_type: Column
) -> Column:
    """
    Identify the field that contains the asset type
    Args:
        p_reference1_type: Type of reference
        p_reference2_type: Type of reference
        p_reference3_type: Type of reference
    Returns:
        Column: Active type descriptions.

    """
    return (
        f.when(f.length(p_reference3_type) > 1, p_reference3_type)
        .when(f.length(p_reference2_type) > 1, p_reference2_type)
        .otherwise(p_reference1_type)
    )


@task(
    name="active_type_aggrupation_task",
    tags=["data cleaning", "preprocessing", "equifax loan"],
)
def asset_type_aggrupation_task(p_active_type: Column) -> Column:
    """
    Builts, based on the asset type descriptions, a classification that will be
    used to build aggrupations later on.
    Args:
        p_active_type (Column): Active type descriptions.
    Returns:
        Column: Active type aggrupation.
    """
    return (
        f.when((p_active_type.like("%tarjeta%")) & (p_active_type.like("%cr%")), "tc")
        .when(
            (p_active_type.like("%consumo%"))
            | ((p_active_type.like("%prestamo%")) & (p_active_type.like("%personal%")))
            | ((p_active_type.like("%otros%")) & (p_active_type.like("%creditos%")))
            | (p_active_type.like("%microcredito%"))
            | (p_active_type.like("%vehiculo%"))
            | (p_active_type.like("%microfinanza%"))
            | (p_active_type.like("%hipotecari%"))
            | (p_active_type.like("%créditos%"))
            | (p_active_type.like("%prendatario%"))
            | (p_active_type.like("%prendario%"))
            | (p_active_type.like("%prestamo%"))
            | (p_active_type.like("%construccion%"))
            | (p_active_type.like("%vivienda%")),
            "prestamo",
        )
        .when(p_active_type.like("%extrafinanciamiento%"), "tc_extrafinanciamientos")
        .when(
            (p_active_type.like("%cartera%")) & (p_active_type.like("%comercial%")),
            "cartera_comercial",
        )
        .when(p_active_type.like("%fiduciari%"), "fiduciario")
        .when(
            (p_active_type.like("%leasing%")) | (p_active_type.like("%arrendamiento%")),
            "leasing",
        )
        .when(p_active_type.like("%convenio%"), "convenio_pago")
        .otherwise("otras_deudas")
    )


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_transaccion"])
@task(
    name="clean_agencies_debt_detail_task",
    tags=["data cleaning", "preprocessing", "equifax loan"],
)
def clean_agencies_debt_detail_task(
    p_raw_debt_detail: DataFrame,
) -> DataFrame:
    """
    Cleans and processes equifax's agencies debt detail raw data.

    This task processes the equifax's agencies debt detail transactional raw
    data, by applying transformations and cleaning steps.It standardizes
    certain fields, creates new calculated columns, and fills empty values
    with default values.

    Args:
        p_raw_debt_detail (DataFrame): agencies debt detail raw data.

    Returns:
        DataFrame: Processed equifax's agencies debt detail DataFrame.
    """
    df_debt_detail = p_raw_debt_detail.select(
        convert_to_hex_task(f.col("bec_id"), "id_buro_credito"),
        convert_to_hex_task(f.col("be_id"), "id_buro_externo"),
        convert_to_hex_task(f.col("codigo_cliente_persona"), "id_cliente_persona"),
        convert_to_hex_task(f.col("numero_solicitud"), "id_numero_solicitud"),
        convert_to_hex_task(f.col("id_referencia_credito"), "id_referencia_credito"),
        f.lower("buroref").alias("buro_referencia"),
        f.to_date(f.col("fecha_referencia"), "yyyy-MM-dd").alias("fecha_referencia"),
        f.split(f.lower("tipo_referencia"), ",").alias("tipo_referencia"),
        f.when(f.lower("vinculo") == "deudor", "D")
        .when(f.lower("vinculo") == "fiador", "I")
        .alias("id_tipo_deuda"),
        f.when(f.lower("vinculo") == "deudor", "deuda_directa")
        .when(f.lower("vinculo") == "fiador", "deuda_indirecta")
        .alias("descripcion_tipo_deuda"),
        f.to_date(f.col("fecha_concesion"), "yyyy-MM-dd").alias("fecha_concesion"),
        f.col("cod_estado").cast("int").alias("id_estado_deuda"),
        clean_description_column_task(f.lower("estado")).alias(
            "descripcion_estado_deuda"
        ),
        clean_description_column_task(f.lower("cod_clasificacion")).alias(
            "id_clasificacion"
        ),
        clean_description_column_task(f.lower("clasificacion")).alias(
            "descripcion_clasificacion"
        ),
        f.lower("moneda").alias("moneda"),
        f.col("monto_otorgado").cast("float").alias("monto_otorgado"),
        f.col("saldo").alias("saldo"),
        f.col("saldo_pendiente").cast("float").alias("saldo_pendiente"),
        f.col("saldo_vencido").cast("float").alias("saldo_vencido_deuda"),
        f.col("cuota").cast("float").alias("cuota"),
        f.col("comportamiento").alias("comportamiento_deuda"),
        f.col("fecha_transaccion"),
    )

    df_debt_detail = df_debt_detail.select(
        df_debt_detail["*"],
        map_status_task(f.col("id_clasificacion")).alias("agrupacion_clasificacion"),
        past_due_flag_task(f.col("id_clasificacion")).alias("flag_dias_mora"),
        f.regexp_replace(
            clean_description_column_task(f.col("tipo_referencia")[0]), "_s_a", ""
        ).alias("entidad"),
        asset_type_task(
            clean_description_column_task(f.col("tipo_referencia")[1]),
            clean_description_column_task(f.col("tipo_referencia")[2]),
            clean_description_column_task(f.col("tipo_referencia")[3]),
        ).alias("descripcion_tipo_activo"),
    ).drop(f.col("tipo_referencia"))

    df_debt_detail_final = df_debt_detail.select(
        df_debt_detail["*"],
        asset_type_aggrupation_task(f.col("descripcion_tipo_activo")).alias(
            "agrupacion_tipo_activo"
        ),
        get_cols_task(f.col("flag_dias_mora"), 30, 1, "flag_mora_1_30_dias", "int"),
        get_cols_task(f.col("flag_dias_mora"), 60, 1, "flag_mora_31_60_dias", "int"),
        get_cols_task(f.col("flag_dias_mora"), 90, 1, "flag_mora_61_90_dias", "int"),
        get_cols_task(f.col("flag_dias_mora"), 120, 1, "flag_mora_91_120_dias", "int"),
        get_cols_task(
            f.col("flag_dias_mora"), 121, 1, "flag_mora_121_o_mas_dias", "int"
        ),
        f.when(f.col("descripcion_estado_deuda").isin("activo", "vigente"), "vigente")
        .when(
            f.col("descripcion_estado_deuda").isin("cancelado", "cuenta_cancelada"),
            "cancelado",
        )
        .when(
            f.col("descripcion_estado_deuda").isin(
                "cobro_judicial", "cobro_juridico", "proceso_juridico"
            ),
            "juridico",
        )
        .when(f.col("descripcion_estado_deuda") == "sustituidaextraviada", "extraviada")
        .otherwise(f.col("descripcion_estado_deuda"))
        .alias("agrupacion_estado_deuda"),
    ).drop(f.col("flag_dias_mora"))

    return df_debt_detail_final


@flow(name="eqfLoan_agencies_debt_detail_flow")
def eql_agencies_debt_detail_flow():
    """
    Load, process, and save agencies debt detail transactional data into the data lake.

    The flow performs the following operations:
    1. Loads agencies debt detail transactional raw data using the specified date range.
    2. Cleans and processes the agencies debt detail transactional data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        agencies debt detail transactional data.
    """
    raw_data = load_raw_data_flow()

    df_debt_detail = raw_data["raw_eql_agencies_debt_detail"]

    df_debt_detail_final = clean_agencies_debt_detail_task(df_debt_detail)

    save_data_flow(df_debt_detail_final)
