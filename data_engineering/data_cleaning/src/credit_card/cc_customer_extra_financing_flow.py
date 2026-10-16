from prefect import flow, task
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    convert_to_hex_task,
    replace_null_or_empty_values,
)


@task(name="is_eligible_for_extra_financing_task", tags=["data cleaning"])
def is_eligible_for_financing_task(
    p_available_amount: Column,
    p_available_installment_amount: Column,
    p_limit_amount: Column,
):
    """Evaluates the customer's eligibility for financing

    Args:
        p_available_amount (col): available_amount
        p_available_installment_amount(col): available_installment_amount
        p_limit_amount(col): limit


    Returns:
        Column: is_eligible_for_financing
    """
    return (
        f.when(
            (f.col(p_available_amount).isNull())
            | (f.col(p_available_installment_amount).isNull())
            | (f.col(p_limit_amount).isNull()),
            f.lit(-1),
        )
        .when(
            (f.col(p_available_amount) <= 0)
            | (f.col(p_available_installment_amount) <= 0)
            | (f.col(p_limit_amount) == 0),
            f.lit(0),
        )
        .otherwise(f.lit(1))
    )


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_informacion", "id_cliente"])
@task(
    name="clean_customer_extra_financing_task", tags=["data cleaning", "preprocessing"]
)
def clean_customer_extra_financing_task(
    p_raw_customer_extra_financing: DataFrame,
) -> DataFrame:
    """
    Cleans and processe customer extra financing data
    It standardizes certain fields, creates new calculated columns,
    and fills empty values with default values.

    Args:
        p_raw_customer_extra_financing (DataFrame): Raw customer extra financing.

    Returns:
        DataFrame: Processed customer  extra financing DataFrame.
    """
    df_customer_extra_financing = p_raw_customer_extra_financing.select(
        convert_to_hex_task(f.col("codigo_Cliente"), "cliente"),
        f.col("situacion").alias("id_situacion"),
        f.col("dw_descripcion_situacion").alias("descripcion_situacion"),
        f.col("tipo_cliente").alias("tipo_cliente"),
        f.col("limite_plan_precio_contado").alias("limite_neocuotas_gtq"),
        f.col("limite_extrafinanciamiento").alias("limite_extrafinanciamiento_gtq"),
        f.col("saldo").alias("saldo_pendiente_financiamientos_gtq"),
        f.col("saldo_autorizado").alias(
            "saldo_pendiente_financiamientos_autorizado_gtq"
        ),
        f.col("disponible_plan_precio_contado").alias("monto_disponible_neocuotas_gtq"),
        f.col("disponible_extrafinanciamiento").alias(
            "monto_disponible_extrafinanciamiento_gtq"
        ),
        f.col("cupo_cuotas_plan_precio_contado").alias(
            "limite_cuota_mensual_neocuotas_gtq"
        ),
        f.col("cupo_cuotas_extrafinanciamiento").alias(
            "limite_cuota_mensual_extrafinanciamiento_gtq"
        ),
        f.col("saldo_cuotas").alias("saldo_cuota_financiamientos_gtq"),
        f.col("cuotas_autoriza").alias("monto_cuotas_financiamiento_autorizadas_gtq"),
        f.col("disponible_cuotas_plan_precio_contado").alias(
            "cuota_mensual_disponible_neocuotas_gtq"
        ),
        f.col("disponible_cuotas_extrafinanciamiento").alias(
            "cuota_mensual_disponible_extrafinanciamiento_gtq"
        ),
        is_eligible_for_financing_task(
            "disponible_plan_precio_contado",
            "disponible_cuotas_plan_precio_contado",
            "limite_plan_precio_contado",
        ).alias("flag_elegible_neocuotas"),
        is_eligible_for_financing_task(
            "disponible_extrafinanciamiento",
            "disponible_cuotas_extrafinanciamiento",
            "limite_extrafinanciamiento",
        ).alias("flag_elegible_extrafinanciamiento"),
        f.col("fecha_informacion"),
    )

    return df_customer_extra_financing


@flow(name="cc_customer_extra_financing_flow")
def cc_customer_extra_financing_flow():
    """
    Load, process, and save customer extra financing data in the data lake.

    This flow performs the following operations:
    1. Loads raw customer extra financing data using the specified date range.
    2. Cleans and processes customer extra financing data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed data.
    """
    raw_data = load_raw_data_flow()

    df_customer_extra_financing = raw_data["raw_cc_customer_extra_financing"]

    df_customer_extra_financing = clean_customer_extra_financing_task(
        df_customer_extra_financing
    )

    save_data_flow(df_customer_extra_financing)
