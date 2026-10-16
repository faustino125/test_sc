from prefect import flow, task
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
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
    name="payment_frequency_task",
    tags=["data cleaning", "payments", "sib"],
)
def payment_frequency_task(
    col: Column,
) -> Column:
    """
    Crates description for payment type.

    Args:
        col (Column): Payment id.

    Returns:
        Column: Payment type description
    """
    return (
        f.when(col == "M", "mensual")
        .when(col == "O", "otra")
        .when(col == "V", "al_vencimiento")
        .when(col == "N", "quincenal")
        .when(col == "A", "anual")
        .when(col == "S", "semestral")
        .when(col == "T", "trimestral")
        .when(col == "B", "bimestral")
    )


@arrange_columns(p_start_cols=["fecha_transaccion", "id_persona"])
@task(
    name="clean_sib_debt_detail_task",
    tags=["data cleaning", "transactional", "sib"],
)
def clean_sib_debt_detail_task(
    p_sib_debt_detail: DataFrame,
) -> DataFrame:
    """
    Cleans and processes SIB debt detail.

    This task processes the SIB raw data,
    by applying transformations and cleaning steps.
    It select the necessary fields.

    Args:
        p_sib_debt_detail (DataFrame): SIB debt detail raw data.

    Returns:
        DataFrame: Processed SIB debt detail DataFrame.
    """
    return (
        p_sib_debt_detail.filter(f.col("id_moneda").isin(1, 7))
        .select(
            convert_to_hex_task("id_persona", "id_persona"),
            clean_description_column_task(f.lower(f.col("descripcion_estado"))).alias(
                "descripcion_estado"
            ),
            f.col("id_estado"),
            f.col("descripcion_moneda"),
            f.col("id_moneda"),
            clean_description_column_task(
                f.lower(f.col("descripcion_tipo_activo"))
            ).alias("descripcion_tipo_activo"),
            f.col("id_tipo_activo"),
            f.col("descripcion_tipo_deuda"),
            f.col("id_tipo_deuda"),
            clean_description_column_task(
                f.lower(f.col("descripcion_tipo_garantia"))
            ).alias("descripcion_tipo_garantia"),
            f.col("id_tipo_garantia"),
            guarantee_type_aggrupations_task(
                f.lower(f.col("descripcion_tipo_garantia"))
            ).alias("agrupacion_tipo_garantia"),
            f.col("f_pago_cap").alias("id_frecuencia_de_pago_capital"),
            payment_frequency_task(f.col("f_pago_cap")).alias(
                "frecuencia_de_pago_capital"
            ),
            f.col("f_pago_int").alias("id_frecuencia_de_pago_intereses"),
            payment_frequency_task(f.col("f_pago_int")).alias(
                "frecuencia_de_pago_intereses"
            ),
            f.col("fecha_concesion"),
            f.col("fecha_vencimiento"),
            clean_risk_category_task(f.col("id_categoria_riesgo")),
            f.col("id_deuda"),
            clean_description_column_task(f.lower(f.col("vinculo"))).alias(
                "descripcion_vinculo"
            ),
            clean_description_column_task(f.lower(f.col("entidad_deuda"))).alias(
                "nombre_entidad"
            ),
            f.col("capital_original"),
            f.col("saldo").alias("saldo_deuda"),
            f.col("id_situacion"),
            f.when(f.col("id_situacion") == 1, "activo")
            .when(f.col("id_situacion") == 2, "cancelacion_normal")
            .when(f.col("id_situacion") == 8, "robo_o_perdida_tc")
            .when(f.col("id_situacion") == 5, "incobrable_pendiente_pago_deudor")
            .when(f.col("id_situacion") == 9, "cambio_de_tipo_tc")
            .when(f.col("id_situacion") == 3, "cancelacion_por_novacion")
            .when(f.col("id_situacion") == 10, "cancelacion_convenio_pago")
            .when(
                f.col("id_situacion") == 12,
                "venta_o_cesion_activo_posterior_a_declararse_incobrable",
            )
            .when(f.col("id_situacion") == 6, "cancelacion_adjudicacion_de_garantias")
            .when(f.col("id_situacion") == 7, "cancelacion_adjudicacion_de_bienes")
            .when(f.col("id_situacion") == 4, "cancelacion_cambio_garantia")
            .when(
                f.col("id_situacion") == 11, "pagado_posterior_a_declararse_incobrable"
            )
            .when(f.col("id_situacion") == 13, "venta_o_cesion_con_situacion_activo")
            .alias("descripcion_situacion_producto"),
            f.col("fecha_transaccion"),
        )
        .withColumn(
            "agrupacion_tipo_activo",
            active_type_aggrupation_task(f.col("descripcion_tipo_activo")),
        )
    )


@flow(name="sib_debt_detail_flow")
def sib_debt_detail_flow():
    """
    Load, process, and save SIB debt detail
    in the data lake.

    The flow performs the following operations:
    1. Loads SIB debt detail raw data.
    2. Cleans and processes the SIB debt detail.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        SIB debt detail.
    """
    raw_data = load_raw_data_flow()

    df_sib_debt_detail_final = clean_sib_debt_detail_task(
        raw_data["raw_sib_debt_detail"]
    )

    save_data_flow(df_sib_debt_detail_final)
