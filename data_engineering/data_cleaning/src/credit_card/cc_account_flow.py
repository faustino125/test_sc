from prefect import flow, task
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    active_legal_client_flag_task,
    bad_situation_product_flag_task,
    convert_to_hex_task,
    get_months_between_task,
    replace_null_or_empty_values,
    std_situation_account_task,
)


@task(name="get_nonrenewal_flag_task", tags=["data cleaning", "non_renewal_flag"])
def get_nonrenewal_flag_task(
    p_non_renewal_col: str,
    p_field_alias: str,
    p_upgrade_flag=False,
) -> DataFrame:
    """Flag for labeling non renewal customers.

    Args:
        p_non_renewal_col (Column): column used to create flag for p_non_renewal_col.
        p_field_alias (str): alias for new field.
        p_upgrade_flag (bool): upgrades column flag.

    Returns:
        Column: column with non_renewal_flag
    """
    non_renewal_causes = [
        "ARREGLO DE PAGO",
        "AUT GERENCIA SIN PLASTICO",
        "AUTORIZADO GERENCIA",
        "BLOQUEO COBROS",
        "CANCELACION CON SALDO PEND",
        "CLIENTE DE BAJA EN EMBAJADA",
        "FUERA DEL PAIS MAL CLIENTE",
        "INCOBRABLE BI",
        "INCOBRABLES BI",
        "JURIDICAS RECUPERADAS",
        "JURIDICO",
        "JURIDICO INTERNO",
        "MAL CLIENTE",
        "MORA",
        "POR FALLECIMIENTO",
        "REPOSICION DENEGADA",
        "RES. INCOBRABLE CODIGO CP CJ Y MORA",
        "RES. INCOBRABLE TH FALLECIO",
        "S/MEMO COBROS",
        "SE DESTRUYE X MORAS",
        "SERJURSA",
        "SOLICITADA POR GERENCIA",
        "T.H. FUERA DEL PAIS",
    ]

    upgrade_causes = [
        "NO EXISTE MOTIVO NO RENOVACION",
        "CAMBIO DE CATEGORIA",
    ]

    non_renewal_flag = (
        f.when(
            (
                (f.col(p_non_renewal_col).isin(non_renewal_causes))
                | (f.col(p_non_renewal_col).like("LIC%"))
            )
            & f.lit(not p_upgrade_flag),
            1,
        )
        .when(
            (
                (
                    f.col(p_non_renewal_col).isin(upgrade_causes)
                    | f.col(p_non_renewal_col).like("CAMBIO A%")
                )
                & f.lit(p_upgrade_flag)
            ),
            1,
        )
        .otherwise(0)
        .alias(p_field_alias)
    )

    return non_renewal_flag


@task(name="get_situation_group_task")
def get_situation_group_task(p_orig_situation_desc: Column, p_col_alias: str) -> Column:
    """
    Using the original situation account data,
    return the situation account group

    Args:
        p_orig_situation_desc (Column): The column to be validated
        p_field_alias (str): Name for the output Column

    Returns:
        Column: Column with situation group description.
    """
    STATUS_CATALOG = {
        "activa": [1, 8, 10],
        "cancelada": [3],
        "bloqueada": [5, 6],
        "legal_incobrable": [9, 15],
        "no_disponible": [4, 7],
        "inactiva": [2],
        "fraude": [11],
    }

    def map_status(desc):
        for status, situations in STATUS_CATALOG.items():
            if desc in situations:
                return status
        return "desconocida"

    map_status_udf = f.udf(map_status)
    return map_status_udf(p_orig_situation_desc).alias(p_col_alias)


@task(name="payment_date_task", tags=["date", "processing"])
def payment_date_task(p_statement_date: Column, p_card_type: Column):
    """
    Return the payment date based on the statement date

    Args:
        p_statement_date (Column): credit card statement date
        p_card_type (Column): credit card type

    Returns:
        Column: Payment date column
    """

    next_date = f.concat(
        f.when(f.month(p_statement_date) == 12, f.year(p_statement_date) + 1).otherwise(
            f.year(p_statement_date)
        ),
        f.lit("-"),
        f.when(f.month(p_statement_date) == 12, 1).otherwise(
            f.month(p_statement_date) + 1
        ),
        f.lit("-"),
        f.when(
            (f.day(p_statement_date) == 29)
            & (f.month(p_statement_date) == 1)
            & ((f.year(p_statement_date) % 4) != 0),
            28,
        ).otherwise(f.day(p_statement_date)),
    ).cast("date")

    payment_date = (
        f.when(
            (f.col(p_card_type) == "tlt") & (f.day(next_date) == 6),
            f.date_add(next_date, -5),
        )
        .when(f.col(p_card_type) == "tlt", f.date_add(next_date, -6))
        .when(next_date.isNull(), f.add_months(next_date, 1))
        .otherwise(next_date)
    )

    return payment_date


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_informacion", "id_cliente", "cuenta_corporativa"])
@task(name="clean_cc_account_task", tags=["data cleaning", "preprocessing"])
def clean_cc_account_task(
    p_raw_credit_card_accounts: DataFrame, p_raw_cc_statement_summary
) -> DataFrame:
    """
    Cleans and processes credit card accounts data by
    applying transformations and cleaning steps.
    It standardizes certain fields, creates new calculated columns,
    and fills empty values with default values.

    Args:
        p_raw_credit_card_accounts (DataFrame): Raw credit card accounts data.
        p_raw_cc_statement_summary (DataFrame): Raw cc statement summary data

    Returns:
        DataFrame: Processed credit card accounts DataFrame.
    """

    df_cc_statement_summary = p_raw_cc_statement_summary.select(
        convert_to_hex_task(f.col("Codigo_Cliente"), "cliente"),
        convert_to_hex_task(f.col("Cuenta_Corporativa"), "cuenta"),
        f.col("fecha_corte").cast("date").alias("fecha_corte"),
        f.col("pagos_al_corte_gtq")
        .cast("double")
        .alias("pagos_realizados_al_corte_gtq"),
        f.col("pagos_al_corte_usd")
        .cast("double")
        .alias("pagos_realizados_al_corte_usd"),
        f.col("creditos_al_corte_por_traslado_de_saldo_gtq")
        .cast("decimal(18,2)")
        .alias("creditos_por_traslado_de_saldo_al_corte_gtq"),
        f.col("creditos_al_corte_por_traslado_de_saldo_usd")
        .cast("decimal(18,2)")
        .alias("creditos_por_traslado_de_saldo_al_corte_usd"),
        f.col("creditos_al_corte_por_regularizacion_gtq")
        .cast("decimal(18,2)")
        .alias("creditos_por_regularizacion_al_corte_gtq"),
        f.col("creditos_al_corte_por_regularizacion_usd")
        .cast("decimal(18,2)")
        .alias("creditos_por_regularizacion_al_corte_usd"),
        f.col("fecha_informacion"),
    )

    df_cc_account = p_raw_credit_card_accounts.filter(
        (f.trim(f.col("DW_GRUPO_DESCRIPCION")) != "TC PRUEBAS E83")
    ).select(
        f.col("fecha_informacion"),
        convert_to_hex_task(f.col("Codigo_Cliente"), "cliente"),
        convert_to_hex_task(f.col("Cuenta_Corporativa"), "cuenta"),
        convert_to_hex_task(f.col("categoria"), "id_categoria_tc"),
        convert_to_hex_task(f.col("dw_Cuenta_Anterior"), "cuenta_corporativa_anterior"),
        f.col("FECHA_AUTORIZACION").cast("date").alias("fecha_apertura"),
        f.when(f.col("Limite_Q") > 0, f.col("Limite_Q"))
        .otherwise(f.col("Limite_D") * 8)
        .cast("double")
        .alias("limite_gtq"),
        f.col("Saldo_Capital_Q").cast("double").alias("saldo_capital_gtq"),
        f.col("Saldo_Capital_D").cast("double").alias("saldo_capital_usd"),
        f.trim(f.col("DW_MOTIVO_NO_RENOVACION_DESCRIPCION")).alias(
            "motivo_de_no_renovacion"
        ),
        f.lower("DW_TIPO_TARJETA").alias("tipo_tarjeta"),
        f.lower(f.col("Emisor")).alias("emisor"),
        f.col("SITUACION_CUENTA").cast("int").alias("id_situacion_cuenta"),
        f.when(f.col("SITUACION_CUENTA") == "15", f.lit("incobrable"))
        .otherwise(
            f.lower(
                f.regexp_replace(
                    f.trim(f.col("Situacion_Cuenta_descripcion")), " ", "_"
                )
            )
        )
        .alias("descripcion_situacion_cuenta"),
        std_situation_account_task(f.col("Situacion_Cuenta_descripcion")),
        f.col("FECHA_VEN_PLASTICO").cast("date").alias("fecha_vencimiento"),
        f.col("Dias_Mora_Q").alias("dias_mora_gtq"),
        f.col("Dias_Mora_D").alias("dias_mora_usd"),
        f.when(
            (f.col("TOTAL_CICLO_VENCIDOQ").isNotNull())
            & (f.col("saldo_confirmadoQ") > 0),
            f.col("TOTAL_CICLO_VENCIDOQ").cast("int"),
        )
        .otherwise(0)
        .alias("ciclo_mora_gtq"),
        f.when(
            (f.col("TOTAL_CICLO_VENCIDOD").isNotNull())
            & (f.col("saldo_confirmadoD") > 0),
            f.col("TOTAL_CICLO_VENCIDOD").cast("int"),
        )
        .otherwise(0)
        .alias("ciclo_mora_usd"),
        f.col("SALDO_ANT_CONFQ").cast("double").alias("saldo_al_corte_gtq"),
        f.col("SALDO_ANT_CONFD").cast("double").alias("saldo_al_corte_usd"),
        f.col("saldo_confirmadoQ").cast("double").alias("saldo_al_dia_gtq"),
        f.col("saldo_confirmadoD").cast("double").alias("saldo_al_dia_usd"),
    )

    df_final = (
        df_cc_account.join(
            df_cc_statement_summary,
            ["id_cliente", "cuenta_corporativa", "fecha_informacion"],
            "left",
        )
        .select(
            df_cc_account["*"],
            f.col("fecha_corte"),
            payment_date_task("fecha_corte", "tipo_tarjeta")
            .cast("date")
            .alias("fecha_vencimiento_pago"),
            f.col("pagos_realizados_al_corte_gtq"),
            f.col("pagos_realizados_al_corte_usd"),
            f.col("creditos_por_traslado_de_saldo_al_corte_gtq"),
            f.col("creditos_por_traslado_de_saldo_al_corte_usd"),
            f.col("creditos_por_regularizacion_al_corte_gtq"),
            f.col("creditos_por_regularizacion_al_corte_usd"),
            bad_situation_product_flag_task(
                f.col("situacion_cuenta_homologado"),
            ),
            active_legal_client_flag_task(
                f.col("situacion_cuenta_homologado"),
                (f.col("saldo_capital_gtq") + f.col("saldo_capital_usd")),
            ),
            get_nonrenewal_flag_task("motivo_de_no_renovacion", "flag_no_renovacion"),
            get_nonrenewal_flag_task(
                "motivo_de_no_renovacion", "flag_upgrade_no_renovacion", True
            ),
            get_months_between_task(
                f.col("fecha_vencimiento"),
                f.col("fecha_informacion"),
                "meses_para_expiracion",
            ),
            get_situation_group_task(f.col("id_situacion_cuenta"), "grupo_situacion"),
            f.when(
                (f.col("dias_mora_gtq") > f.col("dias_mora_usd")),
                f.col("dias_mora_gtq"),
            )
            .otherwise(f.col("dias_mora_usd"))
            .alias("dias_mora"),
            f.when(
                (f.col("ciclo_mora_gtq") > f.col("ciclo_mora_usd")),
                f.col("ciclo_mora_gtq"),
            )
            .otherwise(f.col("ciclo_mora_usd"))
            .alias("ciclo_mora"),
        )
        .drop("motivo_de_no_renovacion")
    )

    return df_final


@flow(name="cc_account_flow")
def cc_account_flow():
    """
    Load, process, and save credit card accounts data in the data lake.

    This flow performs the following operations:
    1. Loads raw credit card accounts data using the specified date range.
    2. Cleans and processes the credit card accounts data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed credit
        card accounts data.
    """
    raw_data = load_raw_data_flow()
    df_cc_account = raw_data["raw_cc_account"]
    df_cc_statement_summary = raw_data["raw_cc_statement_summary"]

    df_cc_account = clean_cc_account_task(df_cc_account, df_cc_statement_summary)

    save_data_flow(df_cc_account)
