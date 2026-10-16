from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_transformation.src.utils.eqf_utils import applicant_task
from data_engineering.data_transformation.src.utils.utils import join_dataframes_task


@task(name="request_task", tags=["data transformation", "processing"])
def request_task(
    p_person_request: DataFrame,
    p_person: DataFrame,
    p_customer: DataFrame,
) -> DataFrame:
    """
    Returns person request

    Args:
        p_person_request (DataFrame): Person request data
        p_person (DataFrame): Person data
        p_customer (DataFrame): Customer data

    Returns:
        DataFrame: person request's DataFrame.
    """
    df_person_request = p_person_request.filter(f.col("id_entidad") == 164)

    df_applicant = applicant_task(df_person_request, p_person, p_customer)

    df_person_request = df_person_request.select(
        f.col("id_numero_solicitud"), f.col("id_sib")
    ).distinct()

    df_person_request_final = join_dataframes_task(
        ["id_numero_solicitud"], [df_applicant, df_person_request], "left"
    )

    return df_person_request_final


@task(name="request_task", tags=["data transformation", "processing"])
def sib_loan_account_task(
    p_loan_account: DataFrame, p_loan_case: DataFrame
) -> DataFrame:
    """
    Returns loan account number

    Args:
        p_loan_account (DataFrame): Loan account data
        p_loan_case (DataFrame): Loan Case data

    Returns:
        DataFrame: Sib loan account DataFrame.
    """
    df_loan_case = p_loan_case.select(
        f.col("id_crm"), f.col("cuenta_corporativa").alias("numero_prestamo")
    )

    df_loan = p_loan_account.select(
        f.col("id_cliente").alias("id_cliente_bi"),
        f.col("fecha_consecion").alias("fecha_concesion"),
        f.col("id_sib"),
        f.col("cuenta_corporativa").alias("numero_prestamo"),
    )

    df_loan_case_final = (
        join_dataframes_task(["numero_prestamo"], [df_loan, df_loan_case], "inner")
        .select(
            f.col("id_cliente_bi"),
            df_loan["numero_prestamo"],
            f.col("id_crm"),
        )
        .distinct()
    )

    return df_loan, df_loan_case_final


@arrange_columns(
    p_start_cols=[
        "_observ_end_dt",
        "fecha_crea",
        "id_numero_solicitud",
        "id_empresa",
        "descripcion_empresa",
        "numero_prestamo",
        "id_tipo_credito",
        "descripcion_tipo_credito",
        "id_bandera_autorizacion",
        "descripcion_bandera_autorizacion",
    ]
)
@task(name="transform_request_task", tags=["data transformation", "processing"])
def transform_request_task(
    p_cleaned_eql_person_request: DataFrame,
    p_cleaned_eql_person: DataFrame,
    p_cleaned_customer: DataFrame,
    p_cleaned_loan_account: DataFrame,
    p_cleaned_loan_case: DataFrame,
    p_cleaned_eql_request: DataFrame,
) -> DataFrame:
    """
    Transforms and processes information to create features.

    Args:
        p_cleaned_eql_person_request (DataFrame): Person request data
        p_cleaned_eql_person (DataFrame): Person data
        p_cleaned_customer (DataFrame): Customer data
        p_cleaned_loan_account (DataFrame): Loan account data
        p_cleaned_loan_case (DataFrame): Loan case data
        p_cleaned_eql_request (DataFrame):  Request data

    Returns:
        DataFrame: request's features DataFrame.
    """

    df_person_request = request_task(
        p_cleaned_eql_person_request, p_cleaned_eql_person, p_cleaned_customer
    )

    df_main_request = join_dataframes_task(
        ["id_numero_solicitud"],
        [p_cleaned_eql_request.drop("_observ_end_dt"), df_person_request],
        "left",
    )

    df_loan, df_loan_case = sib_loan_account_task(
        p_cleaned_loan_account, p_cleaned_loan_case
    )

    df_request = df_main_request.select(
        f.last_day(f.add_months(f.current_date(), -1)).alias("_observ_end_dt"),
        df_main_request["*"],
    )

    df_request_loan = df_request.join(
        df_loan,
        (
            (df_loan["id_cliente_bi"] == df_request["id_cliente_bi"])
            & (df_loan["id_sib"] == df_request["id_sib"])
            & (df_loan["fecha_concesion"] >= df_request["fecha_crea"])
        ),
        how="left",
    ).withColumnRenamed("numero_prestamo", "cuenta")

    df_request_loan_case = join_dataframes_task(
        ["id_crm", "id_cliente_bi"], [df_request_loan, df_loan_case], "left"
    ).select(
        df_request_loan["*"],
        f.when(f.col("cuenta").isNull(), f.col("numero_prestamo"))
        .otherwise(f.col("cuenta"))
        .alias("numero_prestamo"),
    )

    exclude_cols = [
        "id_cliente_bi",
        "cuenta",
        "id_sib",
        "id_crm",
        "id_cliente_persona",
        "id_entidad",
        "descripcion_entidad",
        "agrupacion_entidad",
        "numero_prestamo",
        "fecha_concesion",
    ]
    df_request_final = df_request_loan_case.groupBy(
        [c for c in df_request_loan_case.columns if c not in exclude_cols]
    ).agg(f.max("numero_prestamo").alias("numero_prestamo"))

    return df_request_final


@flow(name="product_flow")
def eql_request_flow():
    """
    Loads, transforms and saves request's features in the data lake.

    The flow performs the following operations:
    1. Loads cleaned data using the specified date range.
    2. Transforms and processes the data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        request's features data.
    """
    cleaned_data = load_raw_data_flow()

    df_request = transform_request_task(
        cleaned_data["cleaned_eql_person_request"],
        cleaned_data["cleaned_eql_person"],
        cleaned_data["cleaned_customer"],
        cleaned_data["cleaned_loan_account"],
        cleaned_data["cleaned_loan_case"],
        cleaned_data["cleaned_eql_request"],
    )

    save_data_flow(df_request)
