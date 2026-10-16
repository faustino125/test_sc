from prefect import flow, task
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    clean_flag_task,
    convert_to_hex_task,
    days_between_dates_task,
    replace_null_or_empty_values,
    years_between_dates_task,
)


@task(
    name="blacklisted_professions_flag_task",
    tags=["flag", "blacklisted_profession", "policy_for_first_credit_card"],
)
def blacklisted_professions_flag_task(
    p_economic_activity: str, p_profession: str, p_work: str
) -> Column:
    """
    Flags if a p_ is blacklisted based on rules defined by BI Credit.

    This task checks if the provided profession, economic activity, or work field
    contains any blacklisted professions that are restricted. The blacklisted
    professions are predefined in the task logic.

    Args:
        p_economic_activity (str): The economic activity of the customer.
        p_profession (str): The profession of the customer.
        p_work (str): The work description of the customer.

    Returns:
        Column: A column with 1 if the profession is blacklisted, otherwise 0.
    """

    black_list_professions = [
        "POLICIA",
        "POLICIAL",
        "POLICIAS",
        "POLICIACO",
        "POLICIANA NACIONAL CIVIL",
        "PNC",
        "POLICIA NACIONAL CIVIL",
        "POLICIANACIONAL CIVIL",
        "MILITAR",
        "MILITARES",
        "MILITARY",
        "DIPUTADO",
        "PARTIDOS POLITICOS",
        "CONSERJERIA",
        "CONSERJE",
        "CONSERJES",
        "SEGURIDA",
        "SEGURIDAD",
        "SEGURIDAS",
        "MISIONERO",
        "ADMINISTRADOR RELIGIOSO",
        "POLITICO",
        "CANTINA",
        "BASURA",
        "MASAJE",
        "MASAJES",
        "MASAJEAR",
        "BARRENDERO",
        "BAR",
        "AGRICULTOR",
        "AGRICULTURA",
    ]

    pattern_regex = "|".join(
        [f"\\b{professions}\\b" for professions in black_list_professions]
    )

    professions_flag = f.when(
        f.trim(p_economic_activity).rlike(pattern_regex)
        | f.trim(p_profession).rlike(pattern_regex)
        | f.trim(p_work).rlike(pattern_regex)
        | f.trim(p_work).like("%AGRICULT%"),
        1,
    ).otherwise(0)

    return professions_flag


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_informacion", "id_cliente"])
@task(name="clean_customer_task", tags=["data cleaning", "preprocessing"])
def clean_customer_task(p_raw_data_customer: DataFrame) -> DataFrame:
    """
    Cleans and processes customer data.

    This task processes the raw customer data by applying transformations and cleaning
    steps. It standardizes certain fields, creates new calculated columns,and fills
    empty values with default values.

    Args:
        p_raw_data_customer (DataFrame): Raw customer data.

    Returns:
        DataFrame: Processed customer DataFrame.
    """
    grupos_bolson_cbi = [
        "9995",
        "9996",
        "9997",
        "9998",
        "9999",
        "0994",
        "2361",
        "2424",
        "2428",
        "2455",
        "2495",
        "2606",
        "2680",
        "2720",
        "2728",
        "2751",
        "2763",
        "2785",
        "2997",
        "3046",
        "3697",
        "4287",
        "4369",
        "4370",
        "4373",
        "4383",
        "4386",
        "4389",
        "4391",
    ]

    df_customer = p_raw_data_customer.select(
        convert_to_hex_task(f.col("CODIGO_CLIENTE"), "cliente"),
        f.regexp_replace(f.lower(f.trim("DW_CARTERIZACION")), " ", "_").alias(
            "carterizacion"
        ),
        f.lower(f.trim("sexo")).alias("genero"),
        years_between_dates_task(
            f.col("Fecha_Nacimiento").cast("date"),
            f.col("fecha_informacion"),
        ).alias("edad"),
        f.trim("dw_pro_ctaplanilla8").cast("int").alias("cuenta_planilla8"),
        f.lower(f.trim("Estado_Civil")).alias("estado_civil"),
        clean_flag_task(f.col("DW_ES_5X1"), "flag_cliente_5x1"),
        f.col("DW_TIPO_CLIENTE_CODIGO").cast("int").alias("id_tipo_cliente"),
        f.regexp_replace(f.lower(f.trim("Nacionalidad")), " ", "_").alias(
            "nacionalidad"
        ),
        f.col("DW_PRO_FONDOS").cast("int").alias("conteo_fondos_inversion"),
        f.col("DW_PRO_FONDOS_SALDO_D")
        .cast("float")
        .alias("saldo_fondos_inversion_usd"),
        f.col("DW_PRO_FONDOS_SALDO_Q")
        .cast("float")
        .alias("saldo_fondos_inversion_gtq"),
        f.when(f.col("DW_PRO_BIMOVIL") > 0, f.lit(1))
        .otherwise(f.lit(0))
        .alias("flag_bimovil"),
        f.col("DW_PRO_BIMOVIL").cast("int").alias("conteo_bimovil"),
        clean_flag_task(f.col("DW_MALDEUDOR"), "flag_mal_deudor"),
        f.col("dw_pro_Clubbi").cast("int").alias("conteo_club_bi"),
        f.regexp_replace(f.lower(f.trim("DW_BANCA_FAVORITA")), " ", "_").alias(
            "banca_favorita"
        ),
        f.lower(f.trim("profesion")).alias("profesion"),
        f.lower(f.trim("Actividad_Economica")).alias("actividad_economica"),
        f.lower(f.trim("trabajo")).alias("trabajo"),
        blacklisted_professions_flag_task(
            f.col("Actividad_Economica"), f.col("Profesion"), f.col("Trabajo")
        )
        .cast("int")
        .alias("flag_profesion_lista_negra"),
        f.col("Fecha_Registro").cast("date").alias("fecha_constitucion_empresa"),
        f.col("Fecha_Ingreso").cast("date").alias("fecha_ingreso"),
        days_between_dates_task(
            f.to_date(f.col("Fecha_Ingreso"), "yyyy-MM-dd"),
            f.col("fecha_informacion"),
        ).alias("antiguedad_cliente_dias"),
        days_between_dates_task(
            f.to_date(f.col("fecha_constitucion_empresa"), "yyyy-MM-dd"),
            f.col("fecha_informacion"),
        ).alias("antiguedad_empresa_dias"),
        f.when(
            f.substring(f.col("grupo_corporativo"), 1, 4).isin(grupos_bolson_cbi),
            f.lit("0000"),
        )
        .when(
            f.col("grupo_corporativo").isin("", "00", "000"),
            f.lit("0000"),
        )
        .otherwise(f.substring(f.col("grupo_corporativo"), 1, 4))
        .alias("grupo_corporativo"),
        f.year("fecha_informacion").alias("anio_informacion"),
        f.col("fecha_informacion"),
    ).dropDuplicates()

    return df_customer


@flow(name="customer_flow")
def customer_flow():
    """
    Load, process, and save customer data in the data lake.

    The flow performs the following operations:
    1. Loads raw customer data using the specified date range.
    2. Cleans and processes the customer data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        customer data.
    """
    raw_data = load_raw_data_flow()

    df_customer = raw_data["raw_customer"]

    df_customer_final = clean_customer_task(df_customer)

    save_data_flow(df_customer_final)
