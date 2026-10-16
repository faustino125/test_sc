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
    days_between_dates_task,
    replace_null_or_empty_values,
    std_situation_account_task,
)


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_informacion", "id_cliente", "cuenta_corporativa"])
@task(
    name="clean_saving_account_task",
    tags=["data cleaning", "preprocessing", "saving account"],
)
def clean_saving_account_task(p_raw_saving_account: DataFrame) -> DataFrame:
    """
    Cleans and processes customers saving accounts data.

    This task processes the customer's saving accounts raw data,
    by applying transformations and cleaning steps.
    It standardizes certain fields, creates new calculated columns, and fills
    empty values with default values.

    Args:
        p_raw_saving_account (DataFrame): Customer's saving account raw data.

    Returns:
        DataFrame: Processed customer DataFrame.
    """

    df_saving_account = p_raw_saving_account.select(
        convert_to_hex_task(f.col("Codigo_Cliente"), "cliente"),
        convert_to_hex_task(f.col("Cuenta_Corporativa"), "cuenta"),
        f.col("DW_MONEDA_CODIGO").alias("id_moneda"),
        f.col("dw_moneda_descripcion").alias("descripcion_moneda"),
        f.round("Saldo_Total", 2).cast("float").alias("saldo_total"),
        f.round("Saldo_Promedio").cast("float").alias("saldo_promedio"),
        f.round("Val_Deposito", 2).cast("float").alias("monto_depositos"),
        f.round("Val_Retiros", 2).cast("float").alias("monto_retiros"),
        f.lower(f.trim("situacion_cuenta")).alias("descripcion_situacion_cuenta"),
        std_situation_account_task(f.col("situacion_cuenta")),
        f.trim("DW_NEW_PRODUCTO_CODIGO").alias("id_producto"),
        f.regexp_replace(
            f.lower(f.trim("DW_NEW_PRODUCTO_DESCRIPCION")), " ", "_"
        ).alias("descripcion_producto"),
        f.lower(f.trim("DW_TIPO_CUENTA_DESCRIPCION")).alias("descripcion_tipo_cuenta"),
        days_between_dates_task(
            f.col("Fecha_Apertura"), f.col("fecha_informacion")
        ).alias("dias_apertura_cuenta_ahorro"),
        f.col("Fecha_Apertura").cast("date").alias("fecha_apertura"),
        f.col("Fecha_Ult_dep").cast("date").alias("fecha_ultimo_deposito"),
        f.col("Fecha_Ult_Retiro").cast("date").alias("fecha_ultimo_retiro"),
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

    df_saving_account = df_saving_account.select(
        f.col("*"),
        bad_situation_product_flag_task(f.col("situacion_cuenta_homologado")),
        active_legal_client_flag_task(
            f.col("situacion_cuenta_homologado"),
            f.col("saldo_total"),
        ),
        f.coalesce(
            f.when(
                f.col("situacion_cuenta_homologado") == "vigente",
                f.months_between(
                    f.col("fecha_informacion"),
                    f.col("fecha_apertura"),
                ).cast("int"),
            ),
            f.lit(0),
        ).alias("meses_cuentas_vigentes"),
    )

    return df_saving_account


@flow(name="saving_account_flow")
def saving_account_flow():
    """
    Load, process, and save customer saving account data in the data lake.

    The flow performs the following operations:
    1. Loads customers saving account raw data using the specified date range.
    2. Cleans and processes the customers saving account data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        customers saving account data.
    """
    raw_data = load_raw_data_flow()

    df_saving_account = raw_data["raw_saving_account"]

    df_saving_account_final = clean_saving_account_task(df_saving_account)

    save_data_flow(df_saving_account_final)
