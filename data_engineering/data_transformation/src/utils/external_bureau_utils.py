from prefect import task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f


@task(name="sib_entry_point_mapping", tags=["data transformation", "processing"])
def sib_entry_point_mapping(
    p_customer: DataFrame,
    p_sib_entry_point: DataFrame,
    p_sib_data: DataFrame,
) -> DataFrame:
    """
    Maps external bureau id to BI customer id.

    Args:
        p_customer: BI Customers.
        p_sib_entry_point: Map between SIB persona id and BI customer id
        p_sib_data: SIB data

    Returns:
        DataFrame: SIB data with BI customer id.
    """
    p_customer = p_customer.select(f.col("id_cliente")).distinct()
    df_customer_map = p_sib_entry_point.join(p_customer, on="id_cliente").select(
        f.col("id_cliente"), f.col("id_persona")
    )

    return p_sib_data.join(df_customer_map, on="id_persona")
