from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow


@task(name="clean_daily_exchange_task", tags=["data cleaning", "foreign exchange"])
def clean_daily_exchange_task(p_raw_exchange_rate: DataFrame) -> DataFrame:
    """
    Ingest daily exchange rate data into foreign exchange data cleaning layer.

    Args:
        p_raw_exchange_rate (DataFrame): Daily exchange rate raw data.

    Returns:
        DataFrame: Processed daily exchange rate DataFrame.
    """
    df_exchange_rate = p_raw_exchange_rate.select(
        f.col("fecha_transaccion"), f.col("Tasa").alias("tasa_cambio")
    )

    return df_exchange_rate


@flow(name="daily_exchange_flow")
def daily_exchange_flow():
    """
    Loads, processes, and saves daily exchange data in datalake.

    The flow performs the following operations:
    1. Loads raw daily exchange data using the specified date range.
    2. Cleans and processes the daily exchange data.
    3. Saves the processed data to the appropriate environment
       using the specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but it saves the processed daily
        exchange data.
    """
    raw_data = load_raw_data_flow()

    df_daily_exchange = raw_data["raw_daily_exchange"]

    df_daily_exchange_final = clean_daily_exchange_task(df_daily_exchange)

    save_data_flow(df_daily_exchange_final)
