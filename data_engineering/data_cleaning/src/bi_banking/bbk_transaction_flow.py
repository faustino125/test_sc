import functools as ft

from prefect import flow, task
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    clean_currency_task,
    convert_to_hex_task,
    replace_null_or_empty_values,
)


def get_pivot_name_task(p_module_name: Column, p_transaction_desc: Column) -> Column:
    """Returns fixed name of the pivot column to use in data transformation layer.

    Args:
        p_module_name: name of the bbk module.
        p_transaction_desc: description of the bbk transaction.

    Returns:
        Columns: Column with the module name and transaction description combination.
    """
    MODULE_CATALOG = {
        "mismo_modulo": [
            "ach",
            "banca_sat",
            "conexion_regional",
            "declaraguate",
            "tarjeta_prepago",
            "pago_electronico",
        ],
        "oper_generales": ["operaciones_generales"],
        "pago_servicios": ["pago_de_servicios"],
        "igss_agexport": ["pago_igss_agexport"],
        "transf_exterior": ["transferencia_al_exterior"],
    }

    TRANSACTION_CATALOG = {
        "misma_trx": [
            "ahorros_saldo",
            "claro_postpago",
            "claro_prepago",
            "cobros",
            "eegsa",
            "energuate",
            "otros",
            "pago_prestamo",
            "pago_proveedores",
            "proveedores",
            "telgua",
            "tigo_postpago",
            "tigo_prepago",
            "transferencia",
        ],
        "pago_planilla": [
            "pago_planilla_abierto",
            "pago_planilla",
            "planillas/nóminas",
        ],
        "no_trx": [
            "banca_sat",
            "declaraguate",
            "recarga_tarjeta_prepago",
            "transferencia_al_exterior",
        ],
        "ahorros_estado_cta": ["ahorros_estado_de_cuenta"],
        "concentracion_fondos_bi": ["concentración_de_fondos_a_bi"],
        "consulta_facturas": ["consulta_de_facturas"],
        "linea_cred_consulta_saldos": ["linea_de_credito_consulta_de_saldos"],
        "linea_cred_estado_cta": ["linea_de_credito_estado_de_cuenta"],
        "linea_cred_pagos": ["linea_de_credito_pagos"],
        "linea_cred_transferencias": ["linea_de_credito_transferencias"],
        "monet_bloquear_cheques": ["monetarios_bloquear_cheques"],
        "monet_bloquear_cta": ["monetarios_bloquear_cuenta"],
        "monet_desbloq_cheques": ["monetarios_des_bloquear_cheques"],
        "monet_desbloq_cta": ["monetarios_des_bloquear_cuenta"],
        "monet_estado_cta": ["monetarios_estado_de_cuenta"],
        "monet_saldo": ["monetarios_saldo"],
        "monet_solicitud_chequera": ["monetarios_solicitud_de_chequera"],
        "prestamo_estado_cta": ["préstamos_estado_de_cuenta"],
        "prestamo_pago": ["préstamos_pago"],
        "pago_agexport": ["ps_agexpront"],
        "pago_igss": ["ps_pago_patronal_del_igss"],
        "tc_estado_cta": ["tarjeta_c._estado_de_cuenta"],
        "tc_pago": ["tarjeta_c._pago"],
        "tc_saldo": ["tarjeta_c._saldo"],
        "transf_fondos": ["transferencias_de_fondos"],
    }

    def map_module(module_nm):
        for new_module, old_module in MODULE_CATALOG.items():
            if module_nm in old_module:
                return module_nm if new_module == "mismo_modulo" else new_module

    def map_transaction(transaction_dsc):
        for new_transaction, old_transaction in TRANSACTION_CATALOG.items():
            if transaction_dsc in old_transaction:
                return (
                    transaction_dsc
                    if new_transaction == "misma_trx"
                    else new_transaction
                )
        return "otros"

    map_module_udf = f.udf(map_module, StringType())
    map_transaction_udf = f.udf(map_transaction, StringType())

    str_transaction = map_transaction_udf(p_transaction_desc).alias(
        "descripcion_operacion_pivote"
    )
    str_module = map_module_udf(p_module_name).alias("modulo_pivote")

    return (
        f.when(str_transaction == "no_trx", str_module)
        .otherwise(f.concat(str_module, f.lit("_"), str_transaction))
        .alias("modulo_operacion")
    )


def get_union_bibanking(*dfs):
    """Gets the union of a set of DataFrame.

    Args:
        *dfs: DataFrame set.

    Returns:
         DataFrame: DataFrame union.
    """
    df_list = []
    for df in dfs:
        df_union = df.select(
            f.col("codigo_cliente"),
            f.col("cuenta_corporativa"),
            f.col("id_moneda"),
            clean_currency_task(f.trim("descripcion_moneda")).alias(
                "descripcion_moneda"
            ),
            f.col("monto"),
            f.regexp_replace(f.lower(f.trim("modulo")), " ", "_").alias("modulo"),
            f.regexp_replace(
                f.regexp_replace(f.trim(f.lower("descripcion_operacion")), "-", " "),
                "\\s+",
                "_",
            ).alias("descripcion_operacion"),
            f.col("flag_cuenta_distinta"),
            f.col("autorizacion"),
            f.col("fecha_transaccion"),
        )

        df_list.append(df_union)

    return ft.reduce(DataFrame.unionAll, df_list)


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_transaccion", "id_cliente", "cuenta_corporativa"])
@task(name="clean_bbk_transaction_task")
def clean_bbk_transaction_task(
    p_raw_bbk_bancasat: DataFrame,
    p_raw_bbk_declaraguate: DataFrame,
    p_raw_bbk_services_payments: DataFrame,
    p_raw_bbk_external_transfers: DataFrame,
    p_raw_bbk_ach_transfers: DataFrame,
    p_raw_bbk_prepaid: DataFrame,
    p_raw_bbk_igss_agexport: DataFrame,
    p_raw_bbk_electronic_payment: DataFrame,
    p_raw_bbk_general_operations: DataFrame,
    p_raw_bbk_regional_connection: DataFrame,
) -> DataFrame:
    """Clean data about BI-Banking transactions

    Args:
        p_raw_bbk_bancasat (DataFrame): Raw data BANCASAT
        p_raw_bbk_declaraguate (DataFrame): Raw data DECLARAGUATE
        p_raw_bbk_services_payments (DataFrame): Raw data services payment
        p_raw_bbk_external_transfers (DataFrame): Raw data transfers abroad
        p_raw_bbk_ach_transfers (DataFrame): Raw data BI-Banking ach transfers
        p_raw_bbk_prepaid (DataFrame): Raw data prepaid cards
        p_raw_bbk_igss_agexport (DataFrame): Raw data igss/agexport payments
        p_raw_bbk_electronic_payment (DataFrame): Raw data electronic payments
        p_raw_bbk_general_operations (DataFrame): Raw data general operations
        p_raw_bbk_regional_connection (DataFrame): Raw data regional operations

    Returns:
        DataFrame: Processed BI-Banking transactions data
    """
    df_bibanking_union = get_union_bibanking(
        p_raw_bbk_bancasat,
        p_raw_bbk_declaraguate,
        p_raw_bbk_services_payments,
        p_raw_bbk_external_transfers,
        p_raw_bbk_ach_transfers,
        p_raw_bbk_prepaid,
        p_raw_bbk_igss_agexport,
        p_raw_bbk_electronic_payment,
        p_raw_bbk_general_operations,
        p_raw_bbk_regional_connection,
    )

    df_bbk_trx = df_bibanking_union.select(
        convert_to_hex_task(f.col("codigo_cliente"), "cliente"),
        convert_to_hex_task(f.col("cuenta_corporativa"), "cuenta"),
        f.col("id_moneda"),
        f.col("descripcion_moneda"),
        f.col("monto").cast("float").alias("monto"),
        f.col("modulo"),
        f.col("descripcion_operacion"),
        get_pivot_name_task(f.col("modulo"), f.col("descripcion_operacion")),
        f.col("flag_cuenta_distinta"),
        f.col("autorizacion"),
        f.col("fecha_transaccion"),
    )

    return df_bbk_trx


@flow(name="bbk_transaction_flow")
def bbk_transaction_flow():
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
    raw_data_bbk = load_raw_data_flow()

    raw_bbk_bancasat = raw_data_bbk["raw_bbk_bancasat"]
    raw_bbk_declaraguate = raw_data_bbk["raw_bbk_declaraguate"]
    raw_bbk_services_payments = raw_data_bbk["raw_bbk_services_payments"]
    raw_bbk_external_transfers = raw_data_bbk["raw_bbk_external_transfers"]
    raw_bbk_ach_transfers = raw_data_bbk["raw_bbk_ach_transfers"]
    raw_bbk_prepaid = raw_data_bbk["raw_bbk_prepaid"]
    raw_bbk_igss_agexport = raw_data_bbk["raw_bbk_igss_agexport"]
    raw_bbk_electronic_payment = raw_data_bbk["raw_bbk_electronic_payment"]
    raw_bbk_general_operations = raw_data_bbk["raw_bbk_general_operations"]
    raw_bbk_regional_connection = raw_data_bbk["raw_bbk_regional_connection"]

    df_bbk_transaction_final = clean_bbk_transaction_task(
        raw_bbk_bancasat,
        raw_bbk_declaraguate,
        raw_bbk_services_payments,
        raw_bbk_external_transfers,
        raw_bbk_ach_transfers,
        raw_bbk_prepaid,
        raw_bbk_igss_agexport,
        raw_bbk_electronic_payment,
        raw_bbk_general_operations,
        raw_bbk_regional_connection,
    )

    save_data_flow(df_bbk_transaction_final)
