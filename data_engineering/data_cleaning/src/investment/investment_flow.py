from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    active_legal_client_flag_task,
    bad_situation_product_flag_task,
    convert_to_hex_task,
    merge_dif_schema_task,
    replace_null_or_empty_values,
    std_situation_account_task,
)


@task(name="fixed_terms_task", tags=["fixed term"])
def fixed_terms_task(p_fixed_terms: DataFrame) -> DataFrame:
    """Transforms and selects relevant columns from the input DataFrame for fixed terms.

    Parameters:
        p_fixed_terms (DataFrame): DataFrame containing client and account information.

    Returns:
        DataFrame: A new DataFrame with selected and transformed columns.
    """
    df_fixed_terms = p_fixed_terms.select(
        convert_to_hex_task(f.col("DW_CODIGO_CLIENTE"), "cliente"),
        convert_to_hex_task(f.col("DW_CUENTA_CORPORATIVA"), "cuenta"),
        f.col("DW_APLICACION_CODIGO").cast("int").alias("id_producto"),
        f.lower(f.regexp_replace(f.trim("DW_APLICACION_DESCRIPCION"), " ", "_")).alias(
            "descripcion_producto"
        ),
        f.col("TIPO_CUENTA").cast("int").alias("id_tipo_cuenta"),
        f.lower(f.regexp_replace(f.trim("DW_TIPO_CUENTA_DESCRIPCION"), " ", "_")).alias(
            "descripcion_tipo_cuenta"
        ),
        f.col("DW_MONDEDA_CODIGO").alias("id_moneda"),
        f.trim(f.lower("DW_MONEDA_DESCRIPCION")).alias("descripcion_moneda"),
        f.trim(f.lower("DW_SITUACION_CUENTA_DESCRIPCION")).alias("situacion_cuenta"),
        std_situation_account_task(f.col("DW_SITUACION_CUENTA_DESCRIPCION")),
        f.lower(
            f.regexp_replace(f.trim("DW_TIPO_CARTERA_DESCRIPCION"), " ", "_")
        ).alias("tipo_carterizacion"),
        f.col("Saldo").cast("float").alias("saldo"),
        f.col("DW_SALDO_ANTERIOR").cast("float").alias("saldo_anterior"),
        f.col("DW_INTERES_MES").cast("float").alias("interes_mensual"),
        f.col("Tasa_Interes_Negociada").cast("float").alias("tasa_interes_negociada"),
        f.col("DW_SALDO_PROMEDIO6M").cast("float").alias("saldo_promedio_6_meses"),
        f.to_date("DW_FECHA_ULTIMA_ACTIVIDAD_CLIENTE").alias("fecha_ultima_actividad"),
        f.when(
            f.col("SITUACION_CUENTA").isin("1", "7"), f.col("fecha_informacion")
        ).alias("fecha_ultimo_movimiento"),
        f.to_date("FECHA_CANCELACION").alias("fecha_cancelacion"),
        f.to_date("FECHA_VENCIMIENTO").alias("fecha_vencimiento"),
        f.to_date("FECHA_APERTURA").alias("fecha_apertura"),
        f.when(f.col("FECHA_VENCIMIENTO").isNull(), 0)
        .when(f.col("FECHA_VENCIMIENTO") == f.col("FECHA_CANCELACION"), 0)
        .otherwise(1)
        .alias("pf_cancelacion_anticipada_flag"),
        f.col("PLAZO_CERTIFICADO").cast("int").alias("plazo"),
        f.col("VALOR_CANCELACION").cast("float").alias("pf_valor_cancelacion"),
        f.col("MONTO_ORIGINAL").cast("float").alias("monto_inicial"),
        f.col("SALDO_DESINVERSION").cast("float").alias("pf_saldo_desinversion"),
        f.col("MOTIVO_CANCELACION").cast("int").alias("pf_id_motivo_cancelacion"),
        f.lower(
            f.regexp_replace(f.trim("DW_MOTIVO_CANCELACION_DESCRIPCION"), " ", "_")
        ).alias("pf_descripcion_motivo_cancelacion"),
        f.when(
            f.col("Codigo_Institucion").isin(
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
            | (f.col("TIPO_CUENTA") == 4),
            1,
        )
        .otherwise(0)
        .alias("flag_institucional"),
        f.col("fecha_informacion"),
    )

    df_fixed_terms = df_fixed_terms.select(
        f.col("*"),
        bad_situation_product_flag_task(
            f.col("situacion_cuenta_homologado"),
        ),
        active_legal_client_flag_task(
            f.col("situacion_cuenta_homologado"),
            f.col("saldo"),
        ),
    )

    return df_fixed_terms


@task(name="funds_task", tags=["funds"])
def funds_task(p_funds: DataFrame, p_cols: list) -> DataFrame:
    """Transforms and selects relevant.

    Parameters:
        p_funds (DataFrame): The input DataFrame containing fund-related data.
        p_cols (dict): A dictionary mapping old column names to new column names.

    Returns:
        DataFrame: A DataFrame with selected and transformed columns.
    """
    for old_name, new_name in p_cols.items():
        p_funds = p_funds.withColumnRenamed(old_name, new_name)

    df_funds = p_funds.select(
        convert_to_hex_task(f.col("DW_CODIGO_CLIENTE"), "cliente"),
        convert_to_hex_task(f.col("DW_CUENTA_CORPORATIVA"), "cuenta"),
        f.col("DW_APLICACION_CODIGO").cast("int").alias("id_producto"),
        f.lower(f.regexp_replace(f.trim("DW_APLICACION_DESCRIPCION"), " ", "_")).alias(
            "descripcion_producto"
        ),
        f.col("CODIGO_SUB_PRODUCTO").cast("int").alias("id_tipo_cuenta"),
        f.lower(
            f.regexp_replace(f.trim("DW_CODIGO_SUB_PRODUCTO_DESCRIPCION"), " ", "_")
        ).alias("descripcion_tipo_cuenta"),
        f.col("DW_CODIGO_MONEDA").alias("id_moneda"),
        f.trim(f.lower("DW_MONEDA_DESCRIPCION")).alias("descripcion_moneda"),
        f.lower(
            f.regexp_replace(f.trim("DW_SITUACION_CUENTA_DESCRIPCION"), " ", "_")
        ).alias("situacion_cuenta"),
        std_situation_account_task(f.col("DW_SITUACION_CUENTA_DESCRIPCION")),
        f.lower(
            f.regexp_replace(f.trim("DW_TIPO_CARTERA_DESCRIPCION"), " ", "_")
        ).alias("tipo_carterizacion"),
        f.col("SALDO").cast("float").alias("saldo"),
        f.col("SALDO_ANTERIOR").cast("float").alias("saldo_anterior"),
        f.col("INTERES_MES").cast("float").alias("interes_mensual"),
        f.col("TASA_INTERES").cast("float").alias("tasa_interes_negociada"),
        f.coalesce(f.col("DW_SALDO_PROMEDIO6M"), f.lit(0))
        .cast("float")
        .alias("saldo_promedio_6_meses"),
        f.to_date("DW_FECHA_ULTIMA_ACTIVIDAD_CLIENTE").alias("fecha_ultima_actividad"),
        f.when(
            f.lower("DW_SITUACION_CUENTA_DESCRIPCION").like("%cancel%"),
            f.to_date("FECHA_ULTIMA_APORTACION"),
        ).alias("fecha_cancelacion"),
        f.to_date("FECHA_APERTURA").alias("fecha_apertura"),
        f.trim(f.lower("TIPO_FONDO")).alias("tipo_fondo"),
        f.col("SITUACION_JURIDICA").cast("int").alias("id_situacion_juridica"),
        f.lower("DW_SITUACION_JURIDICA_DESCRIPCION").alias(
            "descripcion_situacion_juridica"
        ),
        f.col("PRIMERA_APORTACION").cast("float").alias("monto_inicial"),
        f.col("APORTACION_PACTADA").cast("float").alias("aportacion_pactada"),
        f.col("DW_VAL_APORT_PENDIENTES").cast("float").alias("aporte_pendiente"),
        f.col("PLAZO_DEL_FONDO").cast("int").alias("plazo"),
        f.col("PLAZO_AMPLIACION").cast("int").alias("plazo_ampliacion"),
        f.to_date("FECHA_REINVERSION").alias("fecha_inversion"),
        f.to_date("FECHA_ULTIMA_APORTACION").alias("fecha_ultima_aportacion"),
        f.to_date("DW_FECHA_PIMERA_APORTACION").alias("fecha_primera_aportacion"),
        f.col("fecha_informacion"),
    )

    df_funds = df_funds.select(
        f.col("*"),
        bad_situation_product_flag_task(
            f.col("situacion_cuenta_homologado"),
        ),
        active_legal_client_flag_task(
            f.col("situacion_cuenta_homologado"),
            f.col("saldo"),
        ),
    )

    return df_funds


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_informacion", "id_cliente", "cuenta_corporativa"])
@task(
    name="clean_investment_task",
    tags=["data cleaning", "preprocessing", "investment"],
)
def clean_investment_task(
    p_raw_fixed_term: DataFrame,
    p_raw_scheduled_future_plan: DataFrame,
    p_raw_golden_investment_plan: DataFrame,
) -> DataFrame:
    """Cleans and processes raw investment account data from various sources.
        The clean_investment_task performs the following operations:
        - Cleaning fixed term data.
        - Define renaming dictionaries for funds.
        - Cleaning scheduled future plan and golden investment plan.
        - Define columns to update.
        - Join dataframes and drop unnecessary columns.
        - Combine all investment dataframes into a list for merging.
        - Merge dataframes with different schemas.

    Args:
        p_raw_fixed_term (DataFrame): Raw fixed term data.
        p_raw_scheduled_future_plan (DataFrame): Raw scheduled future plan data.
        p_raw_golden_investment_plan (DataFrame): Raw golden investment plan data.

    Returns:
        DataFrame: Processed investment account data.
    """
    df_fixed_term = fixed_terms_task(p_raw_fixed_term)

    dic_scheduled_rename = {
        "DW_TIPO_PFU": "TIPO_FONDO",
        "FECHA_ADICION": "FECHA_APERTURA",
        "TASA_INTERES": "TASA_INTERES",
    }
    dic_golden_rename = {
        "CODIGO_CLIENTE": "DW_CODIGO_CLIENTE",
        "CUENTA_CORPORATIVA": "DW_CUENTA_CORPORATIVA",
        "APLICACION": "DW_APLICACION_DESCRIPCION",
        "SUB_PRODUCTO_DESCRIPCION": "DW_CODIGO_SUB_PRODUCTO_DESCRIPCION",
        "PRODUCTO_DESCRIPCION": "DW_PRODUCTO_DESCRIPCION",
        "DW_TIPO_FONDO": "TIPO_FONDO",
        "FECHA_APERTURA": "FECHA_APERTURA",
        "TASA_INTRRES": "TASA_INTERES",
    }

    df_scheduled_future_plan = funds_task(
        p_raw_scheduled_future_plan, dic_scheduled_rename
    )

    df_golden_inv_plan = funds_task(p_raw_golden_investment_plan, dic_golden_rename)

    list_dfs = [df_scheduled_future_plan, df_golden_inv_plan, df_fixed_term]

    df_investment = merge_dif_schema_task(list_dfs)

    return df_investment


@flow(name="investment_flow")
def investment_flow():
    """
    Load, process, and save customer investment account data in the data lake.

    The flow performs the following operations:
    1. Loads customers investment account raw data using the specified date range.
    2. Cleans and processes the customers investment account data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        customers investment account data.
    """
    raw_data = load_raw_data_flow()

    raw_fixed_term = raw_data["raw_fixed_term"]
    raw_scheduled_future_plan = raw_data["raw_scheduled_future_plan"]
    raw_golden_investment_plan = raw_data["raw_golden_investment_plan"]

    df_investment_final = clean_investment_task(
        raw_fixed_term,
        raw_scheduled_future_plan,
        raw_golden_investment_plan,
    )

    save_data_flow(df_investment_final)
