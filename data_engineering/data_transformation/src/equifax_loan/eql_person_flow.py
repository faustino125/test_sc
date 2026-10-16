from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente_persona", "id_cliente_bi"])
@task(name="transform_person_task", tags=["data transformation", "processing"])
def transform_person_task(
    p_cleaned_eql_person: DataFrame,
    p_cleaned_customer: DataFrame,
) -> DataFrame:
    """
    Transforms and processes information to create features.

    Args:
        p_cleaned_eql_person (DataFrame): Person data
        p_cleaned_customer (DataFrame): Customer data


    Returns:
        DataFrame: person's features DataFrame.
    """
    df_person = p_cleaned_eql_person.join(
        p_cleaned_customer,
        (
            (p_cleaned_eql_person["id_cliente_bi"] == p_cleaned_customer["id_cliente"])
            & (
                p_cleaned_eql_person["_observ_end_dt"]
                == p_cleaned_customer["_observ_end_dt"]
            )
        ),
        how="left",
    ).select(p_cleaned_eql_person["*"], p_cleaned_customer["id_cliente"])

    df_person_final = df_person.drop(f.col("id_cliente_bi")).withColumnRenamed(
        "id_cliente", "id_cliente_bi"
    )

    return df_person_final


@flow(name="eql_person_flow")
def eql_person_flow():
    """
    Loads, transforms and saves debt detail features in the data lake.

    The flow performs the following operations:
    1. Loads cleaned data using the specified date range.
    2. Transforms and processes the data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        debt detail features data.
    """
    cleaned_data = load_raw_data_flow()

    df_person = transform_person_task(
        cleaned_data["cleaned_eql_person"], cleaned_data["cleaned_customer"]
    )

    save_data_flow(df_person)
