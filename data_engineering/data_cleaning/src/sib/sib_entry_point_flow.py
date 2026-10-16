from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    convert_to_hex_task,
)


@task(
    name="clean_sib_entry_point_task",
    tags=["data cleaning", "catalog", "sib"],
)
def clean_sib_entry_point_task(
    p_sib_entry_point: DataFrame,
) -> DataFrame:
    """
    Cleans and processes SIB customer id to BI customer id.

    This task processes the SIB customer id to BI customer id raw
    data, by applying transformations and cleaning steps.
    It select the necessary fields.

    Args:
        p_sib_entry_point (DataFrame): SIB customer id to BI customer id
        raw data.

    Returns:
        DataFrame: Processed SIB customer id to BI customer id DataFrame.
    """

    df_entry_point = p_sib_entry_point.select(
        convert_to_hex_task("id_persona", "id_persona"),
        convert_to_hex_task("dw_codigo_cliente", "cliente"),
        f.when(f.col("dw_match") == "CIF", 1)
        .when(f.col("dw_match").contains("/"), 2)
        .when(f.col("dw_match") == "NIT", 3)
        .when(f.col("dw_match") == "DPI", 4)
        .alias("match"),
        f.col("dw_match"),
        f.col("dw_modulo"),
        f.col("fecha_catalogo"),
        f.date_format(f.col("fecha_catalogo"), "yyyyMM")
        .cast("int")
        .alias("year_month"),
    )

    windowCat = Window.partitionBy(f.col("id_cliente"), f.col("year_month")).orderBy(
        f.col("dw_modulo").desc(), f.col("match").asc()
    )

    df_entry_point = (
        df_entry_point.withColumn("priority", f.row_number().over(windowCat))
        .filter(f.col("priority") == 1)
        .select(
            f.col("id_persona"),
            f.col("id_cliente"),
            f.col("fecha_catalogo"),
            f.col("year_month"),
            f.col("match"),
        )
    )

    df_unique_match = (
        df_entry_point.groupBy("id_persona", "year_month")
        .agg(f.countDistinct(f.col("id_cliente")).alias("cnt"))
        .filter(f.col("cnt") == 1)
        .drop("cnt")
    )

    df_entry_point = df_entry_point.join(
        df_unique_match, on=["id_persona", "year_month"]
    ).drop("year_month")

    multiple_matches_dpi = (
        df_entry_point.groupBy("id_cliente")
        .agg(
            f.sum(f.when(f.col("match") == 4, f.lit(1))).alias("dpi_match"),
            f.count(f.col("*")).alias("cnt"),
        )
        .filter((f.col("cnt") > 1) & (f.col("dpi_match") >= 1))
    )

    df_entry_point = (
        df_entry_point.join(
            multiple_matches_dpi.alias("m"), on=["id_cliente"], how="left"
        )
        .filter((f.col("match") != 4) | (f.col("m.id_cliente").isNull()))
        .select(f.col("id_cliente"), f.col("id_persona"), f.col("fecha_catalogo"))
    )

    return df_entry_point


@flow(name="sib_entry_point_flow")
def sib_entry_point_flow():
    """
    Load, process, and save SIB customer id to BI customer id data
    in the data lake.

    The flow performs the following operations:
    1. Loads SIB customer id to BI customer id raw data.
    2. Cleans and processes the SIB SIB customer id to BI customer id data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        SIB customer id to BI customer id data.
    """
    raw_data = load_raw_data_flow()

    df_sib_entry_point_final = clean_sib_entry_point_task(
        raw_data["raw_sib_entry_point"]
    )

    save_data_flow(df_sib_entry_point_final)
