from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_transformation.src.utils.eqf_utils import applicant_task
from data_engineering.data_transformation.src.utils.utils import join_dataframes_task


@task(name="customer_income_task", tags=["data transformation", "processing"])
def customer_income_task(p_df_person_request: DataFrame) -> DataFrame:
    """
    Returns customer's income

    Args:
        p_df_person_request: person request

    Returns:
        DataFrame: customer's income
    """

    df_person_request = p_df_person_request.filter(f.col("salario") > 0)

    df_income = df_person_request.groupBy(
        f.col("_observ_end_dt"),
        f.col("id_cliente_persona"),
        f.col("id_numero_solicitud"),
    ).agg(
        f.min("fecha_ingreso_trabajo").alias("min_fecha_ingreso_trabajo"),
        f.max("meses_trabajo").alias("max_meses_trabajo"),
        f.sum(f.lit(1)).alias("empleos_reportados_cnt"),
        f.sum("salario").alias("salario_reportado"),
    )

    return df_income


@arrange_columns(
    p_start_cols=[
        "_observ_end_dt",
        "id_numero_solicitud",
        "id_cliente_persona",
        "id_cliente_bi",
        "id_entidad",
        "descripcion_entidad",
        "agrupacion_entidad",
        "fecha_reporte_ingresos",
    ]
)
@task(name="transform_debt_detail_task", tags=["data transformation", "processing"])
def transform_income_task(
    p_cleaned_eql_person_request: DataFrame,
    p_cleaned_eql_person: DataFrame,
    p_cleaned_customer: DataFrame,
    p_cleaned_eql_request: DataFrame,
) -> DataFrame:
    """
    Transforms and processes information to create features.

    Args:
        p_cleaned_external_bureau (DataFrame): Cleaned external bureau.
        p_cleaned_eql_person_request (DataFrame): Cleaned person request.
        p_cleaned_eql_person (DataFrame): Cleaned person.
        p_cleaned_customer (DataFrame): Cleaned customer.
        p_cleaned_eql_request (DataFrame): Cleaned request.

    Returns:
        DataFrame: Income features DataFrame.
    """

    df_applicant = applicant_task(
        p_cleaned_eql_person_request, p_cleaned_eql_person, p_cleaned_customer
    )

    df_income = customer_income_task(p_cleaned_eql_person_request)

    df_request = p_cleaned_eql_request.select(
        f.col("id_numero_solicitud"),
        f.col("fecha_crea").alias("fecha_reporte_ingresos"),
    )

    df_income = join_dataframes_task(
        ["id_numero_solicitud", "id_cliente_persona"],
        [df_income, df_applicant],
        "inner",
    )

    df_income_final = join_dataframes_task(
        ["id_numero_solicitud"],
        [df_request, df_income],
        "inner",
    ).select(
        f.col("_observ_end_dt"),
        df_request["*"],
        f.col("id_cliente_persona"),
        f.col("id_cliente_bi"),
        f.when(
            (f.col("min_fecha_ingreso_trabajo").isNull())
            & ~(f.col("max_meses_trabajo").isNull()),
            f.add_months(f.col("fecha_reporte_ingresos"), -f.col("max_meses_trabajo")),
        )
        .otherwise(f.col("min_fecha_ingreso_trabajo"))
        .alias("min_fecha_ingreso_trabajo"),
        f.when(
            f.col("max_meses_trabajo").isNull(),
            f.floor(
                f.months_between(
                    f.col("fecha_reporte_ingresos"), f.col("min_fecha_ingreso_trabajo")
                )
            ),
        )
        .otherwise(f.col("max_meses_trabajo"))
        .cast("int")
        .alias("max_meses_trabajo"),
        f.col("empleos_reportados_cnt"),
        f.col("salario_reportado"),
    )

    return df_income_final


@flow(name="eql_sib_debt_detail_flow")
def eql_income_flow():
    """
    Loads, transforms and saves debt detail features in the data lake.

    The flow performs the following operations:
    1. Loads cleaned data using the specified date range.
    2. Transforms and processes the data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        debt detail features data.
    """
    cleaned_data = load_raw_data_flow()

    df_income = transform_income_task(
        cleaned_data["cleaned_eql_person_request"],
        cleaned_data["cleaned_eql_person"],
        cleaned_data["cleaned_customer"],
        cleaned_data["cleaned_eql_request"],
    )

    save_data_flow(df_income)
