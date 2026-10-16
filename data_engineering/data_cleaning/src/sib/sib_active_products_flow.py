import pyspark.sql.functions as f
from prefect import flow, task
from pyspark.sql import DataFrame

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.data_cleaning.src.utils.external_bureau_utils import (
    active_type_aggrupation_task,
    clean_risk_category_task,
    guarantee_type_aggrupations_task,
)
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    clean_description_column_task,
    convert_to_hex_task,
)


@task(
    name="clean_product_task",
    tags=["data cleaning", "preprocessing", "external bureau"],
)
def clean_product_task(p_product_data: DataFrame) -> DataFrame:
    """
    Cleans active products.

    Args:
        p_product_data (DataFrame): Active products.

    Returns:
        DataFrame: Cleaned active products.
    """
    return (
        p_product_data.filter(f.col("id_moneda").isin(1, 7))
        .select(
            f.col("fecha_transaccion"),
            convert_to_hex_task("id_persona", "id_persona"),
            f.col("comportamiento"),
            f.col("descripcion_moneda"),
            clean_description_column_task(
                f.lower(f.col("descripcion_tipo_activo"))
            ).alias("descripcion_tipo_activo"),
            clean_description_column_task(
                f.lower(f.col("descripcion_tipo_garantia"))
            ).alias("descripcion_tipo_garantia"),
            guarantee_type_aggrupations_task(
                f.lower(f.col("descripcion_tipo_garantia"))
            ).alias("agrupacion_tipo_garantia"),
            f.when(f.col("descripcion_vinculo").contains("titular"), "D")
            .otherwise("I")
            .alias("id_tipo_deuda"),
            clean_description_column_task(f.lower(f.col("descripcion_vinculo"))).alias(
                "descripcion_vinculo"
            ),
            f.col("fecha_actualizacion"),
            f.col("fecha_concesion"),
            f.col("fecha_vencimiento"),
            clean_risk_category_task(f.col("id_categoria_riesgo")),
            f.col("id_moneda"),
            clean_description_column_task(f.lower(f.col("nombre_entidad"))).alias(
                "nombre_entidad"
            ),
            f.col("saldo"),
            f.col("vencido"),
            f.col("capital_original"),
        )
        .select(
            f.col("*"),
            f.when(f.col("id_tipo_deuda") == "D", "deuda_directa")
            .otherwise("deuda_indirecta")
            .alias("descripcion_tipo_deuda"),
            active_type_aggrupation_task(f.col("descripcion_tipo_activo")).alias(
                "agrupacion_tipo_activo"
            ),
        )
    )


@flow(name="sib_active_products_flow")
def sib_active_products_flow():
    """
    Load, process, and save SIB products with active status data
    in the data lake.

    The flow performs the following operations:
    1. Loads SIB products with active status raw data.
    2. Cleans and processes the SIB products with active status data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        SIB products with active status data.
    """
    raw_data = load_raw_data_flow()

    df_active_final = clean_product_task(raw_data["raw_sib_active_products"])

    save_data_flow(df_active_final)
