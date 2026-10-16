from prefect import flow, task  # type: ignore
from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql import functions as f  # type: ignore
from pyspark.sql.types import StringType  # type: ignore

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    active_legal_client_flag_task,
    bad_situation_product_flag_task,
    convert_to_hex_task,
    days_between_dates_task,
    replace_null_or_empty_values,
    std_situation_account_task,
)


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_informacion", "id_cliente", "cuenta_corporativa"])
@task(
    name="clean_current_account_task",
    tags=["data cleaning", "preprocessing", "current account"],
)
def clean_current_account_task(p_raw_current_account: DataFrame) -> DataFrame:
    """
    Cleans and processes customers current accounts data.

    This task processes the customer's current accounts raw data,
    by applying transformations and cleaning steps.
    It standardizes certain fields, creates new calculated columns, and fills
    empty values with default values.

    Args:
        p_raw_current_account (DataFrame): Customer's current account raw data.

    Returns:
        DataFrame: Processed customer DataFrame.
    """
    dict_transaction = {
        "pago_cheque": [56, 55, 52],
        "deposito": [1],
        "transferencia_credito": [6],
        "transferencia_debito": [63],
        "nota_credito": [2],
        "nota_debito": [53],
        "nota_debito_sobregiro": [62],
        "retiro_cajero": [59],
        "estado_cuenta": [58],
        "correccion_positiva": [3],
        "rechazo_cheque": [
            51,
        ],
        "sin_descripcion": [0, 4],
    }

    std_transaction_description_udf = f.udf(
        lambda code: next(
            (key for key, val in dict_transaction.items() if code in val), None
        ),
        StringType(),
    )

    df_current_account = p_raw_current_account.select(
        convert_to_hex_task(f.col("Codigo_Cliente"), "cliente"),
        convert_to_hex_task(f.col("Cuenta_Corporativa"), "cuenta"),
        convert_to_hex_task(f.col("Agencia_Apertura"), "id_agencia_apertura"),
        f.col("Fecha_Apertura").cast("date").alias("fecha_apertura"),
        days_between_dates_task(
            f.col("Fecha_Apertura"), f.col("fecha_informacion")
        ).alias("dias_apertura_cuenta_monetaria"),
        f.when(
            f.col("DW_SITUACION_CUENTA_DESCRIPCION") == "VIGENTE",
            f.floor(
                f.months_between(f.col("fecha_informacion"), f.col("Fecha_Apertura"))
            ),
        ).alias("meses_cuenta_monetaria_valida"),
        f.when(
            f.col("DW_SITUACION_CUENTA_DESCRIPCION") == "VIGENTE",
            days_between_dates_task(
                f.col("Fecha_Apertura"), f.col("fecha_informacion")
            ),
        ).alias("dias_cuenta_monetaria_valida"),
        f.col("Fecha_Cancelacion").cast("date").alias("fecha_cancelacion"),
        f.lower("dw_aplicacion_descripcion").alias("descripcion_aplicacion"),
        f.col("DW_PRODUCTO_CODIGO").alias("id_producto"),
        f.col("dw_producto_descripcion").alias("descripcion_producto"),
        f.col("Tipo_Cuenta").alias("id_tipo_cuenta"),
        f.regexp_replace(f.lower("DW_TIPO_CUENTA_DESCRIPCION"), r"(\s+)$", str()).alias(
            "descripcion_tipo_cuenta"
        ),
        f.col("Tipo_Cta_Super").cast("int").alias("id_tipo_cuenta_sib"),
        f.col("DW_TIPO_CTA_SUPER_DESCRIPCION").alias("descripcion_tipo_cuenta_sib"),
        f.col("DW_MONEDA_CODIGO").alias("id_moneda"),
        f.col("Saldo_Total").alias("saldo_total"),
        f.col("saldo_promedio").alias("saldo_promedio"),
        f.col("Fecha_Camb_Sit_Cta").cast("date").alias("fecha_cambio_situacion_cuenta"),
        f.lower(f.trim("DW_SITUACION_CUENTA_DESCRIPCION")).alias(
            "descripcion_situacion_cuenta"
        ),
        std_situation_account_task(f.col("DW_SITUACION_CUENTA_DESCRIPCION")),
        f.col("DW_CODIGO_UBICACION").cast("int").alias("id_ubicacion"),
        f.col("dw_codigo_unidad").cast("int").alias("id_unidad"),
        days_between_dates_task(
            f.col("Fecha_Apertura"), f.col("Fecha_Cancelacion")
        ).alias("dias_apertura_cancelacion"),
        f.months_between("Fecha_Cancelacion", "Fecha_Apertura")
        .cast("int")
        .alias("meses_apertura_cancelacion"),
        f.to_date("Fecha_Ult_Movimiento").alias("fecha_ultimo_movimiento"),
        std_transaction_description_udf(f.col("codigo_ultimo_movimiento")).alias(
            "ultimo_movimiento_descripcion"
        ),
        days_between_dates_task(
            f.col("Fecha_Ult_Movimiento"), f.col("Fecha_Cancelacion")
        ).alias("dias_ultimo_movimiento_cancelacion"),
        f.months_between(f.col("Fecha_Cancelacion"), f.col("Fecha_Ult_Movimiento"))
        .cast("int")
        .alias("meses_ultimo_movimiento_cancelacion"),
        days_between_dates_task(
            f.col("Fecha_Ult_Movimiento"), f.col("fecha_informacion")
        ).alias("dias_ultimo_movimiento"),
        f.months_between(f.col("fecha_informacion"), f.col("Fecha_Ult_Movimiento"))
        .cast("int")
        .alias("meses_ultimo_movimiento"),
        f.when(
            f.col("Tipo_Cta_Super").isin(
                [
                    "025",
                    "05",
                    "126",
                    "06",
                    "07",
                    "01",
                    "021",
                    "02",
                    "022",
                    "024",
                    "04",
                    "03",
                    "128",
                    "08",
                ]
            )
            | (f.col("Tipo_Cuenta") == 4),
            1,
        )
        .otherwise(0)
        .alias("flag_institucional"),
        f.col("fecha_informacion"),
    )

    df_current_account = df_current_account.select(
        f.col("*"),
        bad_situation_product_flag_task(
            f.col("situacion_cuenta_homologado"),
        ),
        active_legal_client_flag_task(
            f.col("situacion_cuenta_homologado"),
            f.col("saldo_promedio"),
        ),
    )

    return df_current_account


@flow(name="current_account_flow")
def current_account_flow():
    """
    Load, process, and save customer current account data in the data lake.

    The flow performs the following operations:
    1. Loads customers current account raw data using the specified date range.
    2. Cleans and processes the customers current account data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        customers current account data.
    """
    raw_data = load_raw_data_flow()

    df_current_account = raw_data["raw_current_account"]

    df_current_account_final = clean_current_account_task(df_current_account)

    save_data_flow(df_current_account_final)
