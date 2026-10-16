from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow


@task(
    name="clean_sib_risk_categories_task",
    tags=["data cleaning", "catalog", "sib"],
)
def clean_sib_risk_categories_task(
    p_risk_categories: DataFrame,
) -> DataFrame:
    """
    Cleans and processes SIB risk categories data.

    This task processes the SIB risk categories raw data,
    by applying transformations and cleaning steps.
    It select the necessary fields.

    Args:
        p_risk_categories (DataFrame): SIB risk categories raw data.

    Returns:
        DataFrame: Processed SIB risk categories products DataFrame.
    """
    return p_risk_categories.select(
        f.col("fecha_catalogo"),
        f.col("id_categoria_riesgo"),
        f.col("descripcion_categoria_riesgo"),
    )


@flow(name="sib_risk_categories_flow")
def sib_risk_categories_flow():
    """
    Load, process, and save SIB risk categories data
    in the data lake.

    The flow performs the following operations:
    1. Loads SIB risk categories raw data.
    2. Cleans and processes the SIB risk categories data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        SIB risk categories data.
    """
    raw_data = load_raw_data_flow()

    df_risk_categories_final = clean_sib_risk_categories_task(
        raw_data["raw_sib_risk_category"]
    )

    save_data_flow(df_risk_categories_final)
