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
@arrange_columns(p_start_cols=["fecha_transaccion"])
@task(
    name="clean_debt_history_task",
    tags=["data cleaning", "preprocessing", "equifax Loan"],
)
def clean_debt_history_task(
    p_raw_debt_history: DataFrame,
) -> DataFrame:
    """
    Cleans and processes equifax's debt history raw data.

    This task processes the equifax's debt history transactional raw
    data, by applying transformations and cleaning steps. It standardizes
    certain fields, creates new calculated columns, and fills empty values
    with default values.

    Args:
        p_raw_debt_history (DataFrame): debt history raw data.

    Returns:
        DataFrame: Processed equifax's debt history DataFrame.
    """
    df_debt_history = p_raw_debt_history.select(
        convert_to_hex_task(f.col("int_behd"), "id_historial_deuda"),
        convert_to_hex_task(f.col("id_deuda"), "id_deuda"),
        convert_to_hex_task(f.col("codigo_cliente_persona"), "id_cliente_persona"),
        convert_to_hex_task(f.col("persona"), "id_persona"),
        convert_to_hex_task(f.col("be_id"), "id_buro_externo"),
        convert_to_hex_task(f.col("numero_solicitud"), "id_numero_solicitud"),
        f.lower("buroref").alias("buro_referencia"),
        f.to_date(f.col("fecha"), "yyyy-MM-dd").alias("fecha_historial_deuda"),
        f.when(f.col("peor_calificacion") == "-", None)
        .otherwise(f.col("peor_calificacion"))
        .alias("categoria_riesgo_deuda"),
        clean_description_column_task(f.lower("descripcion_estado")).alias(
            "descripcion_categoria_riesgo_deuda"
        ),
        f.col("total_deuda").cast("float").alias("total_deuda"),
        f.col("total_mora_intereses").cast("float").alias("total_mora_intereses"),
        f.col("total_mora_capital").cast("float").alias("total_mora_capital"),
        f.col("max_mora_capital").cast("int").alias("max_dias_mora_capital"),
        f.col("max_mora_interes").cast("int").alias("max_dias_mora_intereses"),
        f.col("tipo_deuda").alias("tipo_deuda"),
        f.when(f.col("tipo_deuda") == "I", "deuda_indirecta")
        .when(f.col("tipo_deuda") == "D", "deuda_directa")
        .alias("descripcion_tipo_deuda"),
        f.col("fecha_transaccion"),
    )

    return df_debt_history


@flow(name="debt_history_flow")
def eql_sib_debt_history_flow():
    """
    Load, process, and save debt history transactional data into the data lake.

    The flow performs the following operations:
    1. Loads debt history transactional raw data using the specified date range.
    2. Cleans and processes the debt history transactional data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        debt history transactional data.
    """
    raw_data = load_raw_data_flow()

    df_debt_history = raw_data["raw_eql_sib_debt_history"]

    df_debt_history_final = clean_debt_history_task(df_debt_history)

    save_data_flow(df_debt_history_final)
