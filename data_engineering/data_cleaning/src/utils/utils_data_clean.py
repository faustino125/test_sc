from prefect import task
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StringType


@task(name="years_between_dates_task")
def years_between_dates_task(p_start_date: str, p_end_date: str) -> int:
    """
    Calculates the integer number of years between two dates.

    This task computes the number of years between two dates.
    If the result of the months_between calculation is null it returns 0.
    Otherwise, it returns the floor division of the months by 12 to get the number of
    full years.

    Args:
        p_start_date (str): The date from which the calculation starts.
        p_end_date (str): The date from which the age is calculated.

    Returns:
        int: The number of full years between the two dates.
    """
    return f.floor(
        f.when(
            (
                ((f.months_between(p_end_date, p_start_date)).isNull())
                | (f.months_between(p_end_date, p_start_date) < 0)
            ),
            f.lit(0),
        ).otherwise((f.months_between(p_end_date, p_start_date)) / 12)
    ).cast("int")


@task(name="get_months_between_task")
def get_months_between_task(p_reference_dt: str, p_second_dt: str, p_field_alias: str):
    """Calculate the difference in months between two given dates:
    (p_reference_dt - p_second_dt).

    Args:
        p_reference_dt (date): reference date.
        p_second_dt (date): second date.
        p_field_alias (str): Field alias.

    Returns:
        Column: difference in months casted as int.
    """
    return (
        f.when(
            ((f.months_between(p_reference_dt, p_second_dt)).isNull())
            | ((f.months_between(p_reference_dt, p_second_dt)) < 0),
            f.lit(0),
        )
        .otherwise((f.months_between(p_reference_dt, p_second_dt)))
        .cast("int")
        .alias(p_field_alias)
    )


def replace_null_or_empty_values():
    """
    Replaces null or empty values inside DataFrame with a `desconocido`
    value. It will not affect numerical or date type values.

    Returns:
       DataFrame: DataFrame with empty and null values replaced with `desconocido`.
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            df_to_clean = func(*args, **kwargs)
            df_to_clean = df_to_clean.select(
                [
                    (
                        f.regexp_replace(f.trim(f.col(field.name)), r"\s+", " ").alias(
                            field.name
                        )
                        if isinstance(field.dataType, StringType)
                        else f.col(field.name)
                    )
                    for field in df_to_clean.schema.fields
                ]
            )
            for column, data_type in df_to_clean.dtypes:
                df_to_clean = df_to_clean.withColumn(
                    column,
                    f.when(
                        ((f.col(column).isNull()) | (f.col(column) == ""))
                        & (f.lit(data_type) == f.lit("string")),
                        "desconocido",
                    )
                    .otherwise(f.col(column))
                    .cast(data_type),
                )
            return df_to_clean

        return wrapper

    return decorator


@task(name="convert_to_hex_task")
def convert_to_hex_task(p_column_to_convert: Column, p_col_alias: str) -> Column:
    """
    Converts a column value to hexadecimal format prefixed with '0x'.

    This task takes a column from a DataFrame and converts its values to a hexadecimal
    string, prefixing it with '0x'. This solves the problem of sensible client IDs that
    had already been converted into hexadecimals but cannot be used for
    comparisons in joins or filter operations because of conversions performed during
    ingestion.

    Args:
        p_column_to_convert (Column): The column whose values are to be converted.
        p_col_alias (Column alias identifier):
            - cliente ->  id_cliente
            - cuenta -> cuenta_corporativa
            - Anything else will be considered as the desired alias for the col.

    Returns:
        Column: The converted column with hexadecimal representation.
    """
    hex_col = f.concat(f.lit("0x"), f.hex(p_column_to_convert).cast("string"))

    if p_col_alias == "cliente":
        hex_col = hex_col.alias("id_cliente")
    elif p_col_alias == "cuenta":
        hex_col = hex_col.alias("cuenta_corporativa")
    else:
        hex_col = hex_col.alias(p_col_alias)

    return hex_col


@task(name="clean_flag_task")
def clean_flag_task(p_raw_data: DataFrame, p_field_alias: str) -> DataFrame:
    """
    Clean field values and format it as a flag, using 1 or 0

    Args:
        p_raw_data (DataFrame): The column to be formatted
        p_field_alias (str): The name of the column in the output DataFrame

    Returns:
        Column: The converted column with flag format.
    """
    true_list = ["S", "SI", "Y", "YES", "1", "A"]

    formatted_flag = (
        f.upper(f.when(f.trim(f.upper(p_raw_data)).isin(true_list), 1).otherwise(0))
        .cast("int")
        .alias(p_field_alias)
    )

    return formatted_flag


@task(name="get_cols_task")
def get_cols_task(p_col: Column, p_var, p_sentence, p_alias, p_type) -> Column:
    """Function that gets column by when.

    Args:
        col (Column): specific column.
        var (_type_): variable in condition.
        sentence (_type_): sentence of condition.

    Returns:
        Column: result of when.
    """
    return (f.when(p_col == p_var, p_sentence).otherwise(0)).alias(p_alias).cast(p_type)


@task(name="merge_dif_schema_task")
def merge_dif_schema_task(p_df_list: list) -> DataFrame:
    """
    Performs a union of DataFrames with different schema.

    Args:
        p_df_list (list): The list of DataFrames to be unified.

    Returns:
        DataFrame: The unified DataFrames based on p_df_list.
    """
    if len(p_df_list) > 1:
        return p_df_list[0].unionByName(
            merge_dif_schema_task(p_df_list[1:]), allowMissingColumns=True
        )
    else:
        return p_df_list[0]


@task(name="std_situation_account_task")
def std_situation_account_task(p_orig_situation_desc: Column) -> Column:
    """
    Using the original situation account data,
    return a standarized situation account description

    Args:
        p_orig_situation_desc (DataFrame): The column to be formated

    Returns:
        Column: The converted column with standarized situation description.
    """
    STATUS_CATALOG = {
        "misma_sit": [
            "bloqueada",
            "cancelada",
            "embargada",
            "extraviada",
            "fraude",
            "inactiva",
            "juridico",
            "pendiente",
            "reinvertido",
            "renovada",
            "restringida",
            "robada",
            "sobregirada",
            "vigente",
        ],
        "vigente": ["amplia-vigente", "vigente al dia"],
        "en_mora": ["vigente en mora", "moroso"],
        "embargada": ["embargado", "embarg am-vg"],
        "juridico": ["vencido en cobro judicial", "juridica", "en cobro juridico"],
        "cancelada": [
            "cancelado amp",
            "cancelado rca",
            "cancelado rsa",
            "cancel venc",
            "cancelado",
        ],
        "proceso_prorroga": ["vencido en proceso de prorroga"],
        "con_problema": ["con problema", "c_p"],
        "cobro_administrativo": ["vencido cobro administrativo"],
        "incobrable": ["cancel/incobrab"],
        "vencida": ["vencido", "vencida"],
    }

    def map_status(desc):
        for status, situations in STATUS_CATALOG.items():
            if desc in situations:
                return desc if status == "misma_sit" else status
        return "desconocida"

    map_status_udf = f.udf(map_status, StringType())
    return map_status_udf(f.lower(f.trim(p_orig_situation_desc))).alias(
        "situacion_cuenta_homologado"
    )


@task(name="sum_fields_task")
def sum_fields_task(p_field_list: list):
    """Function to sum multiple fields.

    Args:
    p_field_list (int list): Values that we want to sum

    Returns:
    sum: Sum of the elements inside the p_field_list
    """
    list_size = len(p_field_list)
    sum = 0
    for i in range(0, list_size):
        sum += p_field_list[i]

    return sum


@task(name="bad_situation_product_flag_task")
def bad_situation_product_flag_task(p_account_situation: Column) -> Column:
    """
    Check if a product has a bad situation, based on predefined rules.

    Args:
        p_account_situation (str): The account situation of the customer.
    Returns:
        Column: A column with 1 if the product is flagged as bad product,
        otherwise 0.
    """
    black_list_situations = [
        "incobrable",
        "con_problema",
        "embargada",
        "juridico",
        "moroso",
        "vencida",
        "cobro_administrativo",
        "proceso_prorroga",
    ]

    return (
        f.when((p_account_situation.isin(black_list_situations)), 1)
        .otherwise(0)
        .cast("int")
        .alias("producto_mala_situacion_flag")
    )


@task(name="days_between_dates_task")
def days_between_dates_task(p_start_date: Column, p_end_date: Column) -> Column:
    """
    Calculates the integer number of days between two dates.

    This task computes the number of days between two dates.
    If the result of the months_between calculation is null it returns 0.
    Otherwise, it returns the difference in days, of the input dates.

    Args:
        p_start_date (Column): The date from which the calculation starts.
        p_end_date (Column): The date from which the age is calculated.

    Returns:
        int: The number of full years between the two dates.
    """
    return f.when(
        (f.datediff(p_end_date, p_start_date)).isNotNull(),
        f.datediff(p_end_date, p_start_date),
    ).cast("int")


@task(name="active_legal_client_flag_task")
def active_legal_client_flag_task(
    p_std_situation_desc: str, p_total_balance: float
) -> Column:
    """
    Check some features to validate if a legal client
    can be considered as active client

    Args:
        p_std_situation_desc (DataFrame): The account's sitation description
        p_total_balance (DataFrame): The account's total balance

    Returns:
        Column: Returns 0 value if a legal client is not and active cliente,
        otherwise, returns 1
    """

    valid_situation = ["vigente", "bloqueada", "en_mora", "sobregirada", "renovada"]

    return (
        f.when(
            (p_std_situation_desc.isin(valid_situation))
            & (p_total_balance.cast("float") > 0),
            1,
        )
        .otherwise(0)
        .alias("flag_cliente_juridico_activo")
    )


@task(
    name="clean_currency_task",
    tags=["data cleaning", "standardization"],
)
def clean_currency_task(currency_col: Column) -> Column:
    """Standardize transfer currency.

    Args:
        currency_col (Column): Currency description value.

    Returns:
        Column: Standardized column, values could be either gtq, usd or desconocida.
    """
    gtq_descriptions = ["quetzales", "gtq", "qtg", "1", "quetzal", "moneda"]
    usd_descriptions = ["dolares", "dólares", "usd", "us$", "2", "dolar", "dólar"]
    standardized_currency = (
        f.when(f.lower(currency_col).isin(gtq_descriptions), "gtq")
        .when(f.lower(currency_col).isin(usd_descriptions), "usd")
        .otherwise("desconocida")
    )
    return standardized_currency


@task(name="add_zeros_to_column")
def add_zeros_to_column(p_col_add_z: Column, p_max_lengt: int) -> Column:
    """
        Function to add leading zeros to a Column

    Args:
        p_col_add_z (Column): Column to add leading Zeros
        p_max_lengt (int): Then Lenght about field.

    Returns:
        Column: Return leading zeros to a Column.
    """
    zeros = f.lit("0" * p_max_lengt)
    return f.concat(zeros, p_col_add_z.cast("string")).substr(-p_max_lengt, p_max_lengt)


@task(name="clean_description_column_task")
def clean_description_column_task(p_col: Column) -> Column:
    """
        Cleans a description column, by applying regular expressions to:
        remove leading and trailing spaces, deleting all characters
        that are not Unicode letters, digits, or spaces, and collapses
        single and multiple internal spaces into a single underscore.
    Args:
        p_col (Column): Column with raw description

    Returns:
        Column: Return processed and clean description.
    """
    cleaned_col = f.regexp_replace(p_col, r"[^\p{L}\p{N} ]+", " ")
    cleaned_col = f.regexp_replace(cleaned_col, r"^\s+|\s+$", "")
    cleaned_col = f.regexp_replace(cleaned_col, r" {2,}", " ")
    cleaned_col = f.regexp_replace(cleaned_col, r" ", "_")

    return cleaned_col
