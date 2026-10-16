from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns


@arrange_columns(
    p_start_cols=[
        "_observ_end_dt",
        "_particion",
        "id_cliente",
        "cuenta_corporativa",
    ]
)
@task(name="transform_product_task", tags=["data transformation", "processing"])
def transform_product_task(
    p_cleaned_customer_product: DataFrame,
) -> DataFrame:
    """
    Transforms and processes customer product information to create product features.
    This task transform customer, products data.

    Args:
        p_cleaned_customer_product (DataFrame): Cleaned customer-product data.

    Returns:
        DataFrame: Customer-Product features DataFrame.
    """

    df_products = p_cleaned_customer_product.select(
        f.col("id_cliente"),
        f.col("cuenta_corporativa"),
        f.col("id_aplicacion"),
        f.col("descripcion_aplicacion"),
        f.col("_observ_end_dt"),
    )

    return df_products


@flow(name="product_flow")
def product_flow():
    """
    Loads, transforms and saves customer-product features in the data lake.

    The flow performs the following operations:
    1. Loads cleaned customer-product data using the specified date range.
    2. Transforms and processes the data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        customer-product features data.
    """
    cleaned_data = load_raw_data_flow()

    df_customer_features = transform_product_task(
        cleaned_data["cleaned_customer_products"],
    )

    save_data_flow(df_customer_features)
