from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_transformation.src.utils.utils import (
    add_exchange_rate_task,
    create_currency_col_task,
)


@task(name="fn_compute_flags_currency", tags=["data_transformation", "task", "helper"])
def fn_compute_flags_currency(
    df: DataFrame,
    feature: str,
    col: str,
    filter_flag: list,
) -> DataFrame:
    """
     Creates a flag column based on the values of a given column.

    Args:
        df (DataFrame): Input DataFrame.
        feature (str): Name of the column to be created.
        col (str): Name of the column to be evaluated.
        filter_flag (list): List of allowed values in the `col` column.

    Returns:
        DataFrame: DataFrame with the new flag column.
    """

    return df.withColumn(
        feature, f.when((~f.col(col).isin(filter_flag)), 1).otherwise(0)
    )


@task(name="process_loan_account_task", tags=["data transformation", "processing"])
def process_loan_account_task(
    p_cleaned_loan_account: DataFrame, p_cleaned_daily_rate: DataFrame
) -> DataFrame:
    """
    Transforms and processes loan  account monthly data.

    This task processes the clean loan account and daily exchange rate
    data by applying transformations and calculations to add a balance
    column separated by currency abreviation. it selects a subset of
    relevant columns from the data cleaning layer

    Args:
        p_cleaned_loan _account (DataFrame): Cleaned loan  account data.
        p_cleaned_daily_rate (DataFrame): Cleaned daily exchange rate.

    Returns:
        DataFrame: Transformed base loan  account DataFrame.
    """

    filter_flag = {
        "antiguedad_cartera181D_flag": [
            "al_dia",
            "30_dias",
            "60_dias",
            "90_dias",
            "120_dias",
            "180_dias",
        ],
        "antiguedad_cartera180D_flag": [
            "al_dia",
            "30_dias",
            "60_dias",
            "90_dias",
            "120_dias",
        ],
        "antiguedad_cartera120D_flag": [
            "al_dia",
            "30_dias",
            "60_dias",
            "90_dias",
        ],
        "antiguedad_cartera90D_flag": ["al_dia", "30_dias", "60_dias"],
        "antiguedad_cartera60D_flag": ["al_dia", "30_dias"],
        "antiguedad_cartera30D_flag": ["al_dia"],
    }

    df_account_tflags = p_cleaned_loan_account.drop("_observ_start_dt")

    for feature, filter_f in filter_flag.items():
        df_account_tflags = fn_compute_flags_currency(
            df_account_tflags,
            feature,
            "antiguedad_cartera",
            filter_f,
        )

    df_exchange = add_exchange_rate_task(df_account_tflags, p_cleaned_daily_rate, True)

    df_loan_final = df_exchange.select(
        "*",
        *create_currency_col_task("id_moneda", "monto_prestamo", "tasa_cambio"),
        *create_currency_col_task("id_moneda", "saldo_prestamo", "tasa_cambio"),
        *create_currency_col_task("id_moneda", "cuota_prestamo", "tasa_cambio"),
        (
            f.when(
                f.col("nuevo_codigo_producto").isin("204", "205", "222", "223")
                & (f.col("descripcion_clasificacion") == "consumo"),
                f.lit(1),
            ).otherwise(f.lit(0))
        ).alias("credito_consumo_flag"),
        (
            f.when(
                f.col("garantia_descripcion") == "fiduciario",
                f.lit(1),
            ).otherwise(f.lit(0))
        ).alias("fiduciario_flag")
    )

    return df_loan_final


@arrange_columns(p_start_cols=["fecha_informacion", "_particion", "id_cliente"])
@task(name="transform_loan_account_task", tags=["data transformation", "processing"])
def transform_loan_account_task(
    p_cleaned_loan_account: DataFrame,
    p_cleaned_exchange_rate: DataFrame,
) -> DataFrame:
    """
    Transforms and processes loan  account information to create features.

    This task processes the clean daily exchange data, cleaned loan  account base,
    by applying transformation steps. It creates new columns
    based on joins, creates new data based on transformations, and joins them into a
    unnified dataset.

    Args:
        p_cleaned_loan_account (DataFrame): Cleaned loan  account base data.
        p_cleaned_exchange_rate (DataFrame): Cleaned daily exchange rate data.

    Returns:
        DataFrame: loan  Account features DataFrame.
    """
    df_base_loan_account = process_loan_account_task(
        p_cleaned_loan_account, p_cleaned_exchange_rate
    )

    return df_base_loan_account


@flow(name="loan_account_flow")
def loan_account_flow():
    """
    Loads, transforms and saves loan  account features in the data lake.

    The flow performs the following operations:
    1. Loads raw customer data using the specified date range.
    2. Transforms and processes the customer features data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        loan  account features data.
    """
    raw_data = load_raw_data_flow()

    df_loan_account_features = transform_loan_account_task(
        raw_data["cleaned_loan_account"],
        raw_data["cleaned_exchange_rate"],
    )

    save_data_flow(df_loan_account_features)
