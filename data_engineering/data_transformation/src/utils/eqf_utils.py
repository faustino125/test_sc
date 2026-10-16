from prefect import task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from data_engineering.data_transformation.src.utils.utils import join_dataframes_task


@task(name="applicant_task", tags=["data transformation", "processing"])
def applicant_task(
    p_df_person_request: DataFrame,
    p_df_person: DataFrame,
    p_cleaned_customer: DataFrame,
) -> DataFrame:
    """
    Transforms and processes information over principal and secondary applicant.

    Args:
        p_df_person_request: person request data
        p_df_person: person data
        p_cleaned_customer: customer data

    Returns:
        DataFrame: principal and secondary applicant
    """

    df_customer = p_cleaned_customer.select(f.col("id_cliente")).distinct()
    df_person = p_df_person.join(
        df_customer,
        p_df_person["id_cliente_bi"] == p_cleaned_customer["id_cliente"],
        how="left",
    ).select(
        f.col("_observ_end_dt"),
        f.col("id_cliente_persona"),
        f.col("id_cliente").alias("id_cliente_bi"),
    )

    df_person_request = join_dataframes_task(
        ["id_cliente_persona", "_observ_end_dt"],
        [p_df_person_request, df_person],
    )

    window_spec = Window.partitionBy(
        "_observ_end_dt", "id_cliente_bi", "id_numero_solicitud"
    ).orderBy(f.col("id_entidad").asc())

    df_applicant = (
        df_person_request.withColumn("rn", f.row_number().over(window_spec))
        .filter(f.col("rn") == 1)
        .select(
            f.col("id_cliente_persona"),
            f.col("id_cliente_bi"),
            f.col("id_numero_solicitud"),
            f.col("id_entidad"),
            f.col("descripcion_entidad"),
            f.col("agrupacion_entidad"),
        )
    )

    return df_applicant


@task(name="lookup_external_bureau_task")
def lookup_external_bureau_task(
    p_external_bureau: DataFrame, p_reference_bureau: str
) -> DataFrame:
    """
    Process to group data by four grouping fields and selects the row
    with the latest date and id

    Args:
        p_external_bureau (DataFrame): External bureau data
        p_reference_bureau (str): type of bureau

    Returns:
        DataFrame: Dataframe  with a search by request and bureau type
    """

    df_external_bureau = p_external_bureau.filter(
        f.col("buro_referencia") == p_reference_bureau
    )
    window_spec = Window.partitionBy(
        "id_numero_solicitud", "id_cliente_bi", "_observ_start_dt", "_observ_end_dt"
    ).orderBy(f.col("fecha_transaccion").desc(), f.col("id_buro_externo").desc())

    df_final = (
        df_external_bureau.withColumn("rn", f.row_number().over(window_spec))
        .filter(f.col("rn") == 1)
        .select(
            f.col("fecha_transaccion"),
            f.col("id_buro_externo"),
            f.col("id_numero_solicitud"),
            f.col("id_cliente_bi"),
            f.col("_observ_start_dt"),
            f.col("_observ_end_dt"),
        )
    )

    return df_final
