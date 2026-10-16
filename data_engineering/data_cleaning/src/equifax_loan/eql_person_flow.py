from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    clean_description_column_task,
    convert_to_hex_task,
    replace_null_or_empty_values,
)


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_catalogo"])
@task(
    name="clean_person_task",
    tags=["data cleaning", "preprocessing", "equifax Loan"],
)
def clean_person_task(
    p_raw_person: DataFrame,
) -> DataFrame:
    """
    Cleans and processes equifax's person raw data.

    This task processes the equifax'person full load raw
    data, by applying transformations and cleaning steps.It standardizes
    certain fields, creates new calculated columns, and fills empty values
    with default values.

    Args:
        p_raw_person (DataFrame): person raw data.

    Returns:
        DataFrame: Processed equifax's person DataFrame.
    """
    df_person_final = p_raw_person.select(
        convert_to_hex_task(f.col("codigo_cliente_persona"), "id_cliente_persona"),
        convert_to_hex_task(f.col("codigo_cliente_bi"), "id_cliente_bi"),
        f.col("profesion").cast("int").alias("id_profesion"),
        clean_description_column_task(f.lower("profesion_descripcion")).alias(
            "descripcion_profesion"
        ),
        f.col("actividad_economica_deudor").cast("int").alias("id_actividad_economica"),
        clean_description_column_task(
            f.lower("descripcion_actividad_economica_deudor")
        ).alias("descripcion_actividad_economica"),
        f.col("estado_civil").cast("int").alias("id_estado_civil"),
        f.regexp_replace(
            clean_description_column_task(f.lower("dw_descripcion_estado_civil")),
            "_a",
            "",
        ).alias("descripcion_estado_civil"),
        f.col("genero").cast("int").alias("id_genero"),
        clean_description_column_task(f.lower("genero_descripcion")).alias(
            "descripcion_genero"
        ),
        f.to_date(f.col("fecha_nacimiento"), "yyyy-MM-dd").alias("fecha_nacimiento"),
        f.col("dw_edad").alias("edad"),
        f.when(f.col("dw_tipo_vivienda_descripcion") == "", None)
        .otherwise(f.col("tipo_vivienda"))
        .cast("int")
        .alias("id_tipo_vivienda"),
        clean_description_column_task(f.lower("dw_tipo_vivienda_descripcion")).alias(
            "descripcion_tipo_vivienda"
        ),
        f.when(f.col("numero_hijos") == "*", None)
        .otherwise(f.col("numero_hijos"))
        .cast("int")
        .alias("cantidad_hijos"),
        f.col("pais").cast("int").alias("id_pais"),
        clean_description_column_task(f.lower("descripcion_pais")).alias(
            "descripcion_pais"
        ),
        f.col("departamento").cast("int").alias("id_departamento"),
        clean_description_column_task(f.lower("descripcion_departamento")).alias(
            "descripcion_departamento"
        ),
        f.col("municipio").cast("int").alias("id_municipio"),
        clean_description_column_task(f.lower("descripcion_municipio")).alias(
            "descripcion_municipio"
        ),
        clean_description_column_task(f.lower("pais_nacionalidad")).alias(
            "nacionalidad"
        ),
        f.col("fecha_catalogo"),
    )

    return df_person_final


@flow(name="eqfLoan_person_flow")
def eql_person_flow():
    """
    Load, process, and save person data into the data lake.

    The flow performs the following operations:
    1. Loads person raw data.
    2. Cleans and processes person full_load data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        person data.
    """
    raw_data = load_raw_data_flow()

    df_person = raw_data["raw_eql_person"]

    df_person_final = clean_person_task(df_person)

    save_data_flow(df_person_final)
