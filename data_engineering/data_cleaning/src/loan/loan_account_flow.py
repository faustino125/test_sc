from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    active_legal_client_flag_task,
    bad_situation_product_flag_task,
    clean_currency_task,
    convert_to_hex_task,
    replace_null_or_empty_values,
    std_situation_account_task,
)


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_informacion", "id_cliente", "cuenta_corporativa"])
@task(name="clean_loan_account_task", tags=["data cleaning", "loan"])
def clean_loan_account_task(p_raw_loan_account: DataFrame) -> DataFrame:
    """
    Cleans and processes costumer loan account data.

    This task processes raw customer loan data by applying transformations and
    cleaning procedures. It standardizes specific fields, and fills any
    missing or null values with appropriate default values.

    Args:
        p_raw_loan_account (DataFrame): Raw customer loan data.

    Returns:
        DataFrame: Processed loan DataFrame.
    """
    df_loan = p_raw_loan_account.select(
        convert_to_hex_task(f.col("DW_CODIGO_CLIENTE"), "cliente"),
        convert_to_hex_task(f.col("SIB"), "id_sib"),
        convert_to_hex_task(f.col("DW_CUENTA_CORPORATIVA"), "cuenta"),
        f.regexp_replace(f.lower(f.trim("DW_PRODUCTO_DESCRIPCION")), " ", "_").alias(
            "descripcion_producto"
        ),
        f.regexp_replace(f.lower(f.trim("DW_SUBPRODUCTO_DESCRIPCION")), " ", "_").alias(
            "descripcion_sub_producto"
        ),
        f.lower(f.trim("DW_STATUS_DESCRIPCION")).alias("descripcion_situacion_cuenta"),
        std_situation_account_task(f.col("DW_STATUS_DESCRIPCION")),
        f.col("MONTO").cast("float").alias("monto_prestamo"),
        f.col("SALDO").cast("float").alias("saldo_prestamo"),
        f.regexp_replace(f.lower(f.trim("antiguedad_cartera")), " ", "_").alias(
            "antiguedad_cartera"
        ),
        clean_currency_task(f.col("DW_MONEDA_DESCRIPCION")).alias("descripcion_moneda"),
        f.to_date(f.col("DW_FECHA_CONSECION")).alias("fecha_consecion"),
        f.col("DW_CUOTA_A_PAGAR").cast("float").alias("cuota_prestamo"),
        f.to_date("DW_FECHA_CANCELACION", "yyyy-MM-dd").alias("fecha_cancelacion"),
        f.col("FLAG_CUPO").alias("cupo_flag"),
        f.col("CUOTAS_TOTALES").cast("int").alias("cuotas_totales"),
        f.col("CUOTAS_VENCIDAS").cast("int").alias("cuotas_vencidas"),
        f.col("DIA_PAGO_C").cast("int").alias("dias_pago_capital"),
        f.col("DIAS_ATRASO_CAPITAL").cast("int").alias("dias_atraso_capital"),
        f.regexp_replace(
            f.lower(f.trim("dw_clasificacion_descripcion")), " ", "_"
        ).alias("descripcion_clasificacion"),
        f.regexp_replace(f.lower(f.trim("DW_EMPRESA_DESCRIPCION")), " ", "_").alias(
            "descripcion_empresa"
        ),
        f.col("DW_NEW_PRODUCTO_CODIGO").cast("int").alias("nuevo_codigo_producto"),
        f.regexp_replace(
            f.lower(f.trim("DW_NEW_PRODUCTO_DESCRIPCION")), " ", "_"
        ).alias("nueva_descripcion_producto"),
        f.col("PLAZO").cast("int").alias("plazo"),
        f.col("TASA_INTERES_NETA").cast("float").alias("tasa_interes_neta"),
        (
            f.when(f.upper(f.trim(f.col("DW_MONEDA_DESCRIPCION"))) == "QUETZALES", "1")
            .when(f.upper(f.trim(f.col("DW_MONEDA_DESCRIPCION"))) == "DOLARES", "2")
            .otherwise(f.col("DW_MONEDA_DESCRIPCION"))
            .alias("id_moneda")
        ),
        f.to_date(f.col("DW_FECHA_PRIMER_DESEMBOLSO")).alias("fecha_primer_desembolso"),
        f.to_date(f.col("DW_FECHA_ULTIMO_PAGO_CAPITAL")).alias(
            "fecha_ultimo_pago_capital"
        ),
        f.to_date(f.col("FECHA_PROXIMO_PAGO_CAPITAL")).alias(
            "fecha_proximo_pago_capital"
        ),
        f.to_date(f.col("FECHA_VENCIMIENTO")).alias("fecha_vencimiento"),
        f.col("TASA").cast("float").alias("tasa_interes"),
        f.col("DIAS_ATRASO_INTERES").cast("int").alias("dias_atraso_interes"),
        f.col("CODIGO_RIESGO").alias("codigo_riesgo"),
        f.col("NIVEL").alias("nivel"),
        f.regexp_replace(f.lower(f.trim("DW_CLASIFICACION_CARTERA")), " ", "_").alias(
            "descripcion_cartera"
        ),
        f.regexp_replace(f.lower(f.trim("DW_NIVEL_DESCRIPCION")), " ", "_").alias(
            "descripcion_nivel"
        ),
        f.col("CODIGO_COMPANIA").cast("int").alias("id_compania"),
        f.col("TASA_MORA").cast("float").alias("tasa_mora"),
        f.lower(f.trim("DW_GARANTIA_DESCRIPCION")).alias("garantia_descripcion"),
        f.col("DW_MORA_INTERESES_PERIODO")
        .cast("float")
        .alias("intereses_mora_periodo"),
        f.col("DW_MORA_DIAS_CAPITAL").cast("int").alias("dias_mora_capital"),
        f.col("fecha_informacion"),
    ).drop_duplicates()

    df_loan = df_loan.select(
        f.col("*"),
        bad_situation_product_flag_task(f.col("situacion_cuenta_homologado")),
        active_legal_client_flag_task(
            f.col("situacion_cuenta_homologado"),
            f.col("saldo_prestamo"),
        ),
    )

    return df_loan


@flow(name="loan_account_flow")
def loan_account_flow():
    """
    Load, process, and save customer loan data in the data lake.

    The flow performs the following operations:
    1. Loads raw customer loan data using the specified date range.
    2. Cleans and processes the customer loan data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        customer loan data.
    """
    raw_data = load_raw_data_flow()
    df_loan = raw_data["raw_loan_account"]

    df_loan_final = clean_loan_account_task(df_loan)

    save_data_flow(df_loan_final)
