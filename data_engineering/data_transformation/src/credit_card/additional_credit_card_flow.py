from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente", "cuenta_corporativa"])
@task(
    name="transform_additional_credit_card_task",
    tags=["data transformation", "processing"],
)
def transform_additional_credit_card_task(
    p_cleaned_additional_cc: DataFrame,
) -> DataFrame:
    """
    Transforms and processes additional credit card information to create features.

    Args:
        p_cleaned_additional_cc (DataFrame): Cleaned additional credit card account
        base data.

    Returns:
        DataFrame: additional credit card features DataFrame.
    """
    return p_cleaned_additional_cc.select(
        f.col("id_cliente"),
        f.col("cuenta_corporativa"),
        f.col("cuenta_corporativa_principal"),
        f.col("situacion_adicional"),
        f.col("situacion_cuenta_homologado").alias("situacion_adicional_homologado"),
        f.col("producto_mala_situacion_flag"),
        f.col("fecha_apertura").alias("fecha_apertura_adicional"),
        f.col("_observ_end_dt"),
    )


@flow(name="additional_credit_card_flow")
def additional_credit_card_flow():
    """
    Loads, transforms and saves additional credit card features in the data lake.

    The flow performs the following operations:
    1. Loads cleaned additional credit card data using the specified date range.
    2. Transforms and processes the additional credit card features data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        additional credit card features data.
    """
    raw_data = load_raw_data_flow()

    df_additional_cc_features = transform_additional_credit_card_task(
        raw_data["cleaned_additional_cc"],
    )

    save_data_flow(df_additional_cc_features)
