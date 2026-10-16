from prefect import flow, task
from pyspark.sql import DataFrame

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente", "cuenta_corporativa"])
@task(name="transform_debit_card_task", tags=["data transformation", "processing"])
def transform_debit_card_task(
    p_cleaned_debit_card: DataFrame,
) -> DataFrame:
    """
    Transforms and processes debit card monthly data.

    This task processes the clean debit card data that was previously
    cleaned in the data cleaning layer.
    Args:
        p_cleaned_debit_card (DataFrame): Cleaned debit card data.

    Returns:
        DataFrame: Transformed base debit card DataFrame.
    """
    return p_cleaned_debit_card.drop("fecha_informacion", "_observ_start_dt")


@flow(name="debit_card_flow")
def debit_card_flow():
    """
    Loads, transforms and saves debit card features in the data lake.

    The flow performs the following operations:
    1. Loads raw cards transactions data using the specified date range.
    2. Transforms and processes the debit card features data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        debit card features data.
    """
    raw_data = load_raw_data_flow()

    df_debit_card_features = transform_debit_card_task(
        raw_data["cleaned_debit_card"],
    )

    save_data_flow(df_debit_card_features)
