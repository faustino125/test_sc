from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.external_bureau_utils import (
    active_type_aggrupation_task,
    guarantee_type_aggrupations_task,
)
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    clean_description_column_task,
    convert_to_hex_task,
    replace_null_or_empty_values,
)


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_transaccion"])
@task(
    name="clean_sib_debt_detail_task",
    tags=["data cleaning", "preprocessing", "equifax Loan"],
)
def clean_sib_debt_detail_task(
    p_raw_debt_detail: DataFrame,
) -> DataFrame:
    """
    Cleans and processes equifax's SIB debt detail raw data.

    This task processes the equifax's SIB debt detail transactional raw
    data, by applying transformations and cleaning steps.It standardizes
    certain fields, creates new calculated columns, and fills empty values
    with default values.

    Args:
        p_raw_debt_detail (DataFrame): SIB debt detail raw data.

    Returns:
        DataFrame: Processed equifax's SIB debt detail DataFrame.
    """
    df_debt_detail = p_raw_debt_detail.select(
        convert_to_hex_task(f.col("bed_id"), "id_buro_deuda"),
        convert_to_hex_task(f.col("deuda"), "id_deuda"),
        convert_to_hex_task(f.col("be_id"), "id_buro_externo"),
        convert_to_hex_task(f.col("numero_solicitud"), "id_numero_solicitud"),
        convert_to_hex_task(f.col("persona"), "id_persona"),
        convert_to_hex_task(f.col("codigo_cliente_persona"), "id_cliente_persona"),
        f.lower("buroref").alias("buro_referencia"),
        clean_description_column_task(f.lower("tipo_activo")).alias(
            "descripcion_tipo_activo"
        ),
        f.col("tipo_deuda").alias("id_tipo_deuda"),
        f.when(f.col("tipo_deuda") == "I", "deuda_indirecta")
        .when(f.col("tipo_deuda") == "D", "deuda_directa")
        .alias("descripcion_tipo_deuda"),
        f.col("cod_estado").cast("int").alias("id_estado_deuda"),
        clean_description_column_task(f.lower("descripcion_estado")).alias(
            "descripcion_estado_deuda"
        ),
        f.lit("gtq").alias("descripcion_moneda"),
        f.col("monto").cast("float").alias("monto_otorgado"),
        f.col("saldo").cast("float").alias("saldo_actual_deuda"),
        clean_description_column_task(
            f.substring_index(f.lower("entidad"), ",", 1)
        ).alias("entidad_deuda"),
        f.col("categoria").alias("id_categoria_riesgo_deuda"),
        f.when(f.col("categoria") == "S", "sin_clasificar")
        .otherwise(
            clean_description_column_task(f.lower("desc_categoria")).alias(
                "descripcion_categoria_riesgo_deuda"
            )
        )
        .alias("descripcion_categoria_riesgo_deuda"),
        clean_description_column_task(f.lower("tipo_garantia")).alias(
            "descripcion_tipo_garantia"
        ),
        guarantee_type_aggrupations_task(f.lower("tipo_garantia")).alias(
            "agrupacion_tipo_garantia"
        ),
        f.col("int_bed_comportamiento").alias("comportamiento_deuda"),
        f.col("fecha_transaccion"),
    )

    df_sib_debt_detail = df_debt_detail.withColumn(
        "agrupacion_tipo_activo",
        active_type_aggrupation_task(f.col("descripcion_tipo_activo")),
    )

    return df_sib_debt_detail


@flow(name="def eqfLoan_sib_debt_detail_flow")
def eql_sib_debt_detail_flow():
    """
    Load, process, and save SIB debt detail transactional data into the data lake.

    The flow performs the following operations:
    1. Loads SIB debt detail transactional raw data using the specified date range.
    2. Cleans and processes the SIB debt detail transactional data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        SIB debt detail transactional data.
    """
    raw_data = load_raw_data_flow()

    df_debt_detail = raw_data["raw_eql_sib_debt_detail"]

    df_debt_detail_final = clean_sib_debt_detail_task(df_debt_detail)

    save_data_flow(df_debt_detail_final)
