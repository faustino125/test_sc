from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window

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
    name="get_last_updated_row_task",
    tags=["sort rows"],
)
def get_last_updated_row(p_raw: DataFrame) -> DataFrame:
    """Get most recent record from ire form.

    Args:
        raw (DataFrame): raw ire_information.

    Returns:
        DataFrame: most recent ire form record.
    """
    p_raw = p_raw.withColumn(
        "max_date",
        f.greatest(
            *[
                f.to_date(f.col("Fecha_Tran"), "yyyy-MM-dd"),
                f.to_date(f.col("Fecha_Mod"), "yyyy-MM-dd"),
                f.to_date(f.col("dw_fecha_ult_act"), "yyyy-MM-dd"),
                f.to_date(f.col("dw_fecha_ultima_act_elec"), "yyyy-MM-dd"),
                f.to_date(f.col("DW_FECHA_ACTUALIZACION"), "yyyy-MM-dd"),
            ]
        ),
    )

    windowSpec = Window.partitionBy(
        f.col("Codigo_Cliente"), f.col("fecha_informacion")
    ).orderBy(
        f.col("max_date").desc(),
        f.col("DW_FECHA_ACTUALIZACION").desc(),
        (
            f.when(
                f.col("Fecha_Tran") > f.col("dw_fecha_ult_act"), f.col("Fecha_Tran")
            ).otherwise(f.col("dw_fecha_ult_act"))
        ).desc(),
    )

    return p_raw.withColumn("row_number", f.row_number().over(windowSpec))


@task(
    name="date_ddmmyyyy_to_yyyy_mm_dd_task",
    tags=["date", "date_format"],
)
def date_ddmmyyyy_to_yyyy_mm_dd_task(p_date_val: str, p_field_alias: str) -> DataFrame:
    """
    Task that format the original date from a column into a yyyy-mm-dd format

    Args:
        date_val (str): Original date value.
        field_alias (str): new_name for the date column.

    Returns:
        formated_date: formated date value.
    """
    formated_date = (
        f.when(
            f.substring(p_date_val, 5, 4)
            .cast("int")
            .between(1900, f.year(f.current_date())),
            f.when(
                f.to_date(p_date_val, "ddMMyyyy").isNotNull(),
                f.date_format(f.to_date(p_date_val, "ddMMyyyy"), "yyyy-MM-dd"),
            ).otherwise(f.date_format(f.to_date(p_date_val, "MMddyyyy"), "yyyy-MM-dd")),
        )
        .cast("date")
        .alias(p_field_alias)
    )

    return formated_date


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_informacion", "id_cliente"])
@task(
    name="clean_legal_client_task",
    tags=["data cleaning", "IRE information"],
)
def clean_legal_client_task(raw_ire_information: DataFrame) -> DataFrame:
    """
    Ingest,clean and format legal client raw data from IRE forms.

    Args:
        raw_ire_information (DataFrame): Raw data from IRE forms.

    Returns:
        DataFrame: Processed ire information DataFrame.
    """
    df_ire_information_base = get_last_updated_row(raw_ire_information)

    df_legal_client = df_ire_information_base.select(
        convert_to_hex_task(f.col("Codigo_Cliente"), "cliente"),
        f.col("row_number").alias("fila"),
        f.col("DW_INGRESO_MEN_APROX").cast("float").alias("ingresos_mensuales"),
        f.col("DW_EGRESO_MEN_APROX").cast("float").alias("egresos_mensuales"),
        f.col("Sec_Formulario").cast("int").alias("formulario"),
        f.lower(f.col("DW_CLAS_ACT_INT")).alias("clase_actividad_economica"),
        f.lower(f.col("DW_TIPO_ACT_INT")).alias("tipo_actividad_economica"),
        f.lower(f.col("DESC_OTRA_ACT_INT")).alias("descrip_otra_act_economica"),
        date_ddmmyyyy_to_yyyy_mm_dd_task(
            f.col("FECHA_ESC_PUB"), "fecha_escrit_publica"
        ),
        date_ddmmyyyy_to_yyyy_mm_dd_task(
            f.col("FECHA_DOC_CREACION"), "fecha_creacion_documento"
        ),
        f.col("NUM_SUBS_AGEN_OF").cast("int").alias("cant_subagentes"),
        f.col("NUM_EMPLEADOS").cast("int").alias("cant_empleados"),
        clean_flag_task(f.col("ES_CPE"), "cpe_flag"),
        f.to_date(f.col("RL_FECHA_NACIMIENTO"), "yyyy-MM-dd").alias(
            "fecha_nac_rl",
        ),
        years_between_dates_task(
            f.col("RL_FECHA_NACIMIENTO"), f.col("fecha_informacion")
        ).alias("edad_rl"),
        clean_flag_task(f.col("RL_PEP"), "pep_rl_flag"),
        clean_flag_task(f.col("RL_PMHCH_PEP"), "relacion_pep_rl_flag"),
        clean_flag_task(f.col("RL_ASOCIADO_PEP"), "asociacion_pep_rl_flag"),
        f.coalesce(
            date_ddmmyyyy_to_yyyy_mm_dd_task(
                f.col("FECHA_ESC_PUB"), "fecha_escrit_publica"
            ),
            date_ddmmyyyy_to_yyyy_mm_dd_task(
                f.col("FECHA_DOC_CREACION"), "fecha_creacion_documento"
            ),
        ).alias("ire_fecha_constitucion_empresa"),
        f.col("fecha_informacion"),
    )

    df_legal_client = df_legal_client.select(
        df_legal_client["*"],
        days_between_dates_task(
            f.col("ire_fecha_constitucion_empresa"), f.col("fecha_informacion")
        ).alias("ire_antiguedad_empresa_dias"),
    ).dropDuplicates()

    return df_legal_client


@flow(name="legal_client_flow")
def legal_client_flow():
    """
    Loads, processes, and saves data from IRE forms, for legal clients ,in datalake.

    The flow performs the following operations:
    1. Loads raw data from IRE forms using the specified date range.
    2. Cleans and processes the data from IRE forms.
    3. Saves the processed data to the appropriate environment
       using the specified overwrite strategy.

    Returns:
        None: This flow does not return a value,
        but it saves the processed data from IRE forms, for legal clients.
    """
    raw_data = load_raw_data_flow()

    df_legal_client = raw_data["raw_ire_information"]

    df_legal_client_final = clean_legal_client_task(df_legal_client)

    save_data_flow(df_legal_client_final)
