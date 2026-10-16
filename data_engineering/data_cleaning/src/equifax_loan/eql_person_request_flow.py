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
    name="clean_person_request_task",
    tags=["data cleaning", "preprocessing", "equifax Loan"],
)
def clean_person_request_task(
    p_raw_person_request: DataFrame,
) -> DataFrame:
    """
    Cleans and processes equifax's person request raw data.

    This task processes the equifax'person request full load raw
    data, by applying transformations and cleaning steps.It standardizes
    certain fields, creates new calculated columns, and fills empty values
    with default values.

    Args:
        p_raw_person_request (DataFrame): person request raw data.

    Returns:
        DataFrame: Processed equifax's person request DataFrame.
    """
    df_person_request_final = p_raw_person_request.select(
        convert_to_hex_task(f.col("codigo_cliente_persona"), "id_cliente_persona"),
        convert_to_hex_task(
            f.col("codigo_cliente_solper"), "id_cliente_solicitud_persona"
        ),
        convert_to_hex_task(f.col("numero_solicitud"), "id_numero_solicitud"),
        convert_to_hex_task(f.col("SIB"), "id_sib"),
        f.col("producto").cast("int").alias("id_producto_solicitado"),
        clean_description_column_task(f.lower("dw_descripcion_producto")).alias(
            "descripcion_producto_solicitado"
        ),
        f.col("tipo_persona").cast("int").alias("id_tipo_persona"),
        clean_description_column_task(f.lower("dw_descripcion_tipo_persona")).alias(
            "descripcion_tipo_persona"
        ),
        f.col("entidad").cast("int").alias("id_entidad"),
        f.when(f.col("id_entidad") == "164", "solicitante_principal")
        .otherwise("solicitante_secundario")
        .alias("agrupacion_entidad"),
        clean_description_column_task(f.lower("dw_descripcion_entidad")).alias(
            "descripcion_entidad"
        ),
        f.to_date(f.col("fecha_ingreso_trabajo"), "yyyy-MM-dd").alias(
            "fecha_ingreso_trabajo"
        ),
        ((f.col("anios_trabajo") * 12) + f.col("meses_trabajo"))
        .cast("int")
        .alias("meses_trabajo"),
        f.col("salario").cast("float").alias("salario"),
        f.col("fecha_catalogo"),
    )

    return df_person_request_final


@flow(name="eqfLoan_person_request_flow")
def eql_person_request_flow():
    """
    Load, process, and save person request data into the data lake.

    The flow performs the following operations:
    1. Loads person request raw data.
    2. Cleans and processes person request full_load data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        person request data.
    """
    raw_data = load_raw_data_flow()

    df_person_request = raw_data["raw_eql_person_request"]

    df_person_request_final = clean_person_request_task(df_person_request)

    save_data_flow(df_person_request_final)
