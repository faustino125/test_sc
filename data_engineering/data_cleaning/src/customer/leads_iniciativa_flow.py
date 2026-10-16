from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    convert_to_hex_task,
    replace_null_or_empty_values,
)


@task(name="exclusion_campanias", tags=["exclusion_campanias"])
def exclusion_campanias(p_raw_exclusion_campanias: DataFrame) -> DataFrame:
    """Transforms and selects relevant.

    Parameters:
        p_raw_exclusion_campanias (DataFrame): The input DataFrame
        containing exclusion campaign data.

    Returns:
        DataFrame: A DataFrame with selected and transformed columns.
    """
    df_exclusion = p_raw_exclusion_campanias.select(
        convert_to_hex_task(f.col("CODIGO_CLIENTE"), "cliente"),
        f.col("dw_codigo_motivo").cast("int").alias("id_motivo_exclusion"),
        f.when(
            f.col("dw_excluir") == "SI",
            f.lit(1),
        )
        .otherwise(0)
        .alias("flag_exclusion"),
        f.col("fecha_informacion"),
    )
    return df_exclusion


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_informacion", "cliente"])
@task(
    name="clean_leads_task",
    tags=["data cleaning", "preprocessing", "leads"],
)
def clean_leads_task(
    p_raw_iniciativas_leads: DataFrame,
    p_raw_exclusion_campanias: DataFrame,
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
        raw_exclusion_campanias (DataFrame): Raw exclusion campanias data.
        raw_iniciativas_leads (DataFrame): Raw iniciativas leads  data.

    Returns:
        DataFrame: Processed investment account data.
    """

    df_exclusion = exclusion_campanias(p_raw_exclusion_campanias)

    df_iniciativas_leads = p_raw_iniciativas_leads.select(
        convert_to_hex_task(f.col("Codigo_Cliente"), "cliente"),
        f.to_date(f.col("fecha_inicio"), "yyyy-MM-dd").alias("fecha_inicio_lead"),
        f.to_date(f.col("fecha_fin"), "yyyy-MM-dd").alias("fecha_fin_lead"),
        f.col("banca").cast("string").alias("banca_lead"),
        f.col("monto_pre_autorizado").cast("float").alias("monto_pre_autorizado_lead"),
        f.col("canal_tradicional").cast("string").alias("canal_tradicional_lead"),
        f.col("tasa_cn").cast("float").alias("tasa_cn_lead"),
        f.col("tasa_ss").cast("float").alias("tasa_ss_lead"),
        f.col("plazo").cast("int").alias("plazo_lead"),
        f.col("contactable").cast("string").alias("contactable_lead"),
        f.col("score_credito").cast("float").alias("score_credito_lead"),
        f.col("desembolso_belapp_disponible")
        .cast("float")
        .alias("desembolso_belapp_disponible_lead"),
        f.col("fecha_informacion"),
    )

    df_leads = df_exclusion.join(
        df_iniciativas_leads, ["id_cliente", "fecha_informacion"], "full"
    )

    return df_leads


@flow(name="leads_iniciativa_flow")
def leads_iniciativa_flow():
    """
    Load, process, and save customer leads iniciativa data in the data lake.

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

    df_exclusion_campanias = raw_data["raw_exclusion_campanias"]
    df_iniciativas_leads = raw_data["raw_iniciativas_leads"]

    df_leads_final = clean_leads_task(df_iniciativas_leads, df_exclusion_campanias)

    save_data_flow(df_leads_final)
