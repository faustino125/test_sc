import pyspark.sql.functions as f
from prefect import flow, task
from pyspark.sql import DataFrame

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    bad_situation_product_flag_task,
    convert_to_hex_task,
    replace_null_or_empty_values,
    std_situation_account_task,
)


@replace_null_or_empty_values()
@arrange_columns(
    p_start_cols=[
        "fecha_informacion",
        "id_cliente",
        "cuenta_corporativa",
        "cuenta_corporativa_principal",
    ]
)
@task(name="clean_cc_additional_account_task", tags=["data cleaning", "preprocessing"])
def clean_cc_additional_account_task(p_raw_additional_accounts: DataFrame) -> DataFrame:
    """
    Cleans and processes additional credit card accounts data.

    This task processes the raw additional credit card accounts data by
    applying transformations and cleaning steps.
    It standardizes certain fields, creates new calculated columns,
    and fills empty values with
    default values.

    Args:
        p_raw_additional_accounts (DataFrame): Raw additional credit card accounts data.

    Returns:
        DataFrame: Processed additional credit card accounts DataFrame.
    """
    df_final = p_raw_additional_accounts.select(
        f.col("fecha_informacion"),
        convert_to_hex_task(f.col("CODIGO_CLIENTE"), "cliente"),
        convert_to_hex_task(
            f.col("Cuenta_Corporativa_Principal"), "cuenta_corporativa_principal"
        ),
        convert_to_hex_task(f.col("Cuenta_Corporativa_Adicional"), "cuenta"),
        f.col("Fecha_adicion").cast("date").alias("fecha_apertura"),
        f.when(f.col("Situacion_Adicional") == "15", "desconocido")
        .otherwise(
            f.lower(
                f.regexp_replace(
                    f.trim(f.col("dw_situacion_adicional_descripcion")), " ", "_"
                )
            )
        )
        .alias("situacion_adicional"),
        std_situation_account_task(f.col("dw_situacion_adicional_descripcion")),
        bad_situation_product_flag_task(
            std_situation_account_task(f.col("dw_situacion_adicional_descripcion"))
        ),
    )

    return df_final


@flow(name="cc_additional_account_flow")
def cc_additional_account_flow():
    """
    Load, process, and save additional credit card accounts data in the data lake.

    This flow performs the following operations:
    1. Loads raw additional credit card accounts data using the specified date range.
    2. Cleans and processes the additional credit card accounts data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed additional
        credit card accounts data.
    """
    raw_data = load_raw_data_flow()

    df_cc_categories = raw_data["raw_cc_additional_account"]

    df_cc_categories = clean_cc_additional_account_task(df_cc_categories)

    save_data_flow(df_cc_categories)
