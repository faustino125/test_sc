from prefect import flow, task
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    convert_to_hex_task,
    merge_dif_schema_task,
    replace_null_or_empty_values,
)


@task(name="evaluate_condition_task", tags=["data cleaning", "foreign exchange"])
def evaluate_condition_task(p_value_condition: str) -> list:
    """Evaluate foreign exchange channel condition so we can standarize the operation
    description.

    Args:
      p_value_condition (str): Value to compare.

    Returns:
      values (list[str]): List with types of operations.
    """

    dict_forex = {
        "trading": ["1", "2"],
        "digital": ["COM", "VEN"],
        "branches": ["COMPRA", "VENTA"],
        "transfers": ["COM", "VEN"],
    }

    return dict_forex.get(p_value_condition)


@task(name="clean_forex_fields_task", tags=["data cleaning", "foreign exchange"])
def clean_forex_fields_task(
    p_conditional_field: str,
    p_purchases_value: str,
    p_sales_value: str,
    p_type_value: str,
    p_channel: str,
) -> Column:
    """
    Ingest daily exchange rate data into foreign exchange data cleaning layer.

    Args:
        p_conditional_field (str): Column to compare
        p_purchases_value (str): Purchase value to return
        p_sales_value (str): Sales value to return
        alias_field (str): Alias of the resulting column
        p_type_value (str): Type of the returned column
        p_channel (str): Foreign exchange channel type

    Returns:
        DataFrame: Dataframe with debit and credit column merged.
    """
    values = evaluate_condition_task(p_channel)

    result = (
        f.when(f.trim(f.col(p_conditional_field)) == values[0], p_purchases_value)
        .when(f.trim(f.col(p_conditional_field)) == values[1], p_sales_value)
        .otherwise("" if p_type_value == "string" else None)
        .cast(p_type_value)
    )

    return result


@task(name="merge_forex_fields_task", tags=["data cleaning", "foreign exchange"])
def merge_forex_fields_task(
    p_conditional_field: str,
    p_purchases_field: str,
    p_sales_field: str,
    p_channel: str,
) -> Column:
    """Merges foreign exchange fields and gives them an alias.

    Args:
      p_conditional_field (str): Column to compare.
      p_purchases_field (str): Debit column to merge.
      p_sales_field (str): Credit column to merge.
      p_channel (str): Forex p_channel type.

    Returns:
      A dataframe with debit and credit column merged.
    """
    values = evaluate_condition_task(p_channel)

    result = (
        f.when(
            f.trim(f.col(p_conditional_field)) == values[0],
            f.trim(f.col(p_purchases_field)),
        )
        .when(
            f.trim(f.col(p_conditional_field)) == values[1],
            f.trim(f.col(p_sales_field)),
        )
        .otherwise("")
    )

    return result


@task(name="clean_forex_trading_task", tags=["data cleaning", "foreign exchange"])
def clean_forex_trading_task(p_raw_forex_trading: DataFrame) -> DataFrame:
    """
    Cleans and processes foreign exchange trading data by applying transformations
    and cleaning steps. It standardizes certain fields, creates new calculated
    columns, and fills empty values with 0.

    Args:
        p_raw_forex_trading (DataFrame): Raw foreign exchange trading data.

    Returns:
        DataFrame: The processed foreign exchange trading data DataFrame.
    """

    df_forex_trading = p_raw_forex_trading.select(
        f.lit("1").cast("int").alias("id_modulo_divisas"),
        f.trim(f.col("modulo_divisas")).alias("modulo_divisas"),
        convert_to_hex_task(f.col("DW_CODIGO_CLIENTE"), "cliente"),
        convert_to_hex_task(f.col("DW_CUENTA_CORPORATIVA"), "cuenta_corporativa"),
        f.col("Tipo_Operacion").cast("int").alias("id_operacion"),
        clean_forex_fields_task(
            "Tipo_Operacion",
            "COMPRA",
            "VENTA",
            "string",
            "trading",
        ).alias("operacion_descripcion"),
        f.trim(f.col("Estatus_Operacion")).cast("int").alias("id_estatus_operacion"),
        f.trim(f.col("DW_DESCRIPCION_ESTATUS_OPER")).alias(
            "descripcion_estatus_operacion"
        ),
        f.trim(f.col("Nombre_Contacto")).alias("nombre_contacto"),
        f.trim(f.col("Tipo_Moneda")).alias("descripcion_moneda"),
        merge_forex_fields_task(
            "Tipo_Operacion", "Tipo_Cambioc", "Tipo_Cambiov", "trading"
        )
        .cast("float")
        .alias("tasa_cambio"),
        merge_forex_fields_task(
            "Tipo_Operacion",
            "Monto_Operacionc",
            "Monto_Operacionv",
            "trading",
        )
        .cast("float")
        .alias("monto_transaccion"),
        f.col("fecha_transaccion"),
    )

    return df_forex_trading


@task(name="clean_forex_digital_task", tags=["data cleaning", "foreign exchange"])
def clean_forex_digital_task(p_raw_forex_digital: DataFrame) -> DataFrame:
    """
    Cleans and processes foreign exchange digital channel data by applying
    transformations and cleaning steps. It standardizes certain fields,
    creates new calculated columns, and fills empty values with 0.

    Args:
        p_raw_forex_digital (DataFrame): Raw foreign exchange digital channel data.

    Returns:
        DataFrame: The processed foreign exchange digital channel DataFrame.
    """
    df_forex_digital = p_raw_forex_digital.select(
        f.when(f.col("modulo") == "BEL", "2")
        .when(f.col("modulo") == "BBK", "3")
        .otherwise("6")
        .cast("int")
        .alias("id_modulo_divisas"),
        f.col("modulo").alias("modulo_divisas"),
        convert_to_hex_task(
            merge_forex_fields_task(
                "Tipo_Com_Ven", "cif_debito", "cif_credito", "digital"
            ),
            "cliente",
        ),
        convert_to_hex_task(
            merge_forex_fields_task(
                "Tipo_Com_Ven",
                "Cuenta_Debito",
                "Cuenta_Credito",
                "digital",
            ),
            "cuenta_corporativa",
        ),
        clean_forex_fields_task("Tipo_Com_Ven", "1", "2", "int", "digital").alias(
            "id_operacion"
        ),
        clean_forex_fields_task(
            "Tipo_Com_Ven",
            "COMPRA",
            "VENTA",
            "string",
            "digital",
        ).alias("operacion_descripcion"),
        f.lit("USD").alias("descripcion_moneda"),
        merge_forex_fields_task(
            "Tipo_Com_Ven", "Tasa_Debito", "Tasa_Credito", "digital"
        )
        .cast("float")
        .alias("tasa_cambio"),
        merge_forex_fields_task(
            "Tipo_Com_Ven",
            "Valor_Debito",
            "Valor_Credito",
            "digital",
        )
        .cast("float")
        .alias("monto_transaccion"),
        f.col("Autorizacion").alias("autorizacion"),
        f.col("fecha_transaccion"),
    )

    return df_forex_digital


@task(name="clean_forex_branches_task", tags=["data cleaning", "foreign exchange"])
def clean_forex_branches_task(p_raw_forex_branches: DataFrame) -> DataFrame:
    """
    Cleans and processes foreign exchange digital channel data by applying
    transformations and cleaningsteps. It standardizes certain fields, creates
    new calculated columns, and fills empty values with 0.

    Args:
        p_raw_forex_branches (DataFrame): Raw foreign exchange digital channel data.

    Returns:
        DataFrame: The processed foreign exchange digital channel DataFrame.
    """
    df_forex_digital = p_raw_forex_branches.select(
        f.lit("4").cast("int").alias("id_modulo_divisas"),
        f.trim(f.col("modulo_divisas")).alias("modulo_divisas"),
        convert_to_hex_task(f.col("CODIGO_CLIENTE"), "cliente"),
        convert_to_hex_task(f.col("NUMERO_CUENTA"), "cuenta_corporativa"),
        clean_forex_fields_task("COMPRA_VENTA", "1", "2", "int", "branches").alias(
            "id_operacion"
        ),
        clean_forex_fields_task(
            "COMPRA_VENTA",
            "COMPRA",
            "VENTA",
            "string",
            "branches",
        ).alias("operacion_descripcion"),
        f.when(f.trim(f.col("CODIGO_MONEDA")) == "2", "USD")
        .when(f.trim(f.col("CODIGO_MONEDA")) == "3", "EUR")
        .otherwise("")
        .alias("descripcion_moneda"),
        merge_forex_fields_task(
            "COMPRA_VENTA", "TASA_CAMBIO", "TASA_CAMBIO_V", "branches"
        )
        .cast("float")
        .alias("tasa_cambio"),
        f.col("MONTO_TRANSACCION").alias("monto_transaccion"),
        f.col("AUTORIZACION").alias("autorizacion"),
        f.col("FLAG_REVERSION").alias("flag_reversion"),
        f.col("fecha_transaccion"),
    )

    return df_forex_digital


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_transaccion", "id_cliente", "cuenta_corporativa"])
@task(name="clean_forex_transaction_task", tags=["data cleaning", "foreign exchange"])
def clean_forex_transaction_task(
    p_raw_forex_trading: DataFrame,
    p_raw_forex_digital: DataFrame,
    p_raw_forex_branches: DataFrame,
) -> DataFrame:
    """
    Unify cleaned forex trading, forex digital and forex branche channels data to
    return a single dataframe containing all the cleaned foreign exchange transactions.
    It also filters the columns to ensure that the date column is the last one in the
    dataframe.

    Args:
        p_raw_forex_trading (DataFrame): Raw foreign exchange trading data.
        p_raw_forex_digital (DataFrame): Raw foreign exchange digital channel data.
        p_raw_forex_branches (DataFrame): Raw foreign exchange digital channel data.

    Returns:
        DataFrame: Processed foreign exchange transaction DataFrame.
    """
    df_forex_trading = clean_forex_trading_task(p_raw_forex_trading)
    df_forex_digital = clean_forex_digital_task(p_raw_forex_digital)
    df_forex_branches = clean_forex_branches_task(p_raw_forex_branches)

    df_forex_transactions = merge_dif_schema_task(
        [df_forex_trading, df_forex_digital, df_forex_branches]
    )

    return df_forex_transactions


@flow(name="forex_transaction_flow")
def forex_transaction_flow():
    """
    Loads, processes, and saves forex transactions data in datalake.

    The flow performs the following operations:
    1. Loads raw forex transaction channels data using the specified date range.
    2. Cleans and processes the forex transactions data.
    3. Saves the processed data to the appropriate environment
       using the specified overwrite strategy.

    Returns:
        None: This flow does not return a value,but it saves
        the processed transactions data.
    """
    raw_data = load_raw_data_flow()

    df_main_address_final = clean_forex_transaction_task(
        raw_data["raw_forex_trading"],
        raw_data["raw_forex_digital"],
        raw_data["raw_forex_branches"],
    )

    save_data_flow(df_main_address_final)
