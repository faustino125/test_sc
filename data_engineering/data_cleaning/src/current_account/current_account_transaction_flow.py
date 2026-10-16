from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    convert_to_hex_task,
    replace_null_or_empty_values,
)


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_transaccion", "id_cliente", "cuenta_corporativa"])
@task(
    name="clean_current_account_transaction_task",
    tags=["data cleaning", "preprocessing", "current account trx"],
)
def clean_current_account_transaction_task(
    p_raw_current_account_transaction: DataFrame,
) -> DataFrame:
    """
    Cleans and processes current account transaction raw data.

    This task processes the current accounts transaction raw data,
    by applying transformations and cleaning steps.
    It standardizes certain fields, creates new calculated columns, and fills
    empty values with default values.

    Args:
        p_raw_current_account_transaction (DataFrame):
        Current account transaction raw data.

    Returns:
        DataFrame: Processed current account transactions DataFrame.
    """
    dict_transaction = {
        "pago_cheque": ["CQ-PAGO CHEQUE", "PC-PAGO DE CHEQUE"],
        "nota_debito": ["ND-NOTA DEBITO"],
        "debito_compensacion": ["SC-SEGUNDA COMPENSACION"],
        "debito_sobregiro": ["ND-NOTA DEBITO SOBREGIRO"],
        "deposito": ["DE+DEPOSITO"],
        "nota_credito": ["NC+NOTA CREDITO"],
    }

    def std_transaction_description(p_raw_description):
        """Return a standard transaction description"""
        return next(
            (key for key, val in dict_transaction.items() if p_raw_description in val),
            None,
        )

    std_transaction_description_udf = f.udf(std_transaction_description, StringType())

    df_current_account_trx = p_raw_current_account_transaction.select(
        convert_to_hex_task(f.col("CODIGO_CLIENTE"), "cliente"),
        convert_to_hex_task(f.col("CUENTA_CORPORATIVA"), "cuenta"),
        f.trim(f.upper("TIPO_TRANSACCION_DESCRIPCION")).alias(
            "descripcion_tipo_transaccion"
        ),
        f.trim(f.upper("DW_CODIGO_MONEDA_DESCRIPCION")).alias("descripcion_moneda"),
        f.col("VALOR").cast("float").alias("monto_transaccion"),
        f.col("CODIGO_MONEDA").alias("id_moneda"),
        std_transaction_description_udf(
            f.trim(f.col("DW_CODIGO_TRANSACCION_DESCRIPCION"))
        ).alias("descripcion_transaccion"),
        f.trim("NUM_LEGAJO").cast("int").alias("legajo"),
        f.lower(
            f.regexp_replace(
                f.regexp_replace(
                    f.regexp_replace(
                        f.col("DW_CATALOGO_DESCRIPCION_ITEM"), r"[^\w\s]", str()
                    ),
                    r"\s+",
                    "_",
                ),
                r"^(_?\d*_)+|(_\w{0,2}_?)$",
                str(),
            )
        ).alias("descripcion_legajo"),
        f.col("fecha_transaccion"),
    )

    return df_current_account_trx


@flow(name="current_account_transaction_flow")
def current_account_transaction_flow():
    """
    Load, process, and save current account transaction data in the data lake.

    The flow performs the following operations:
    1. Loads current account transaction raw data using the specified date range.
    2. Cleans and processes the current account transaction data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        current account transaction data.
    """
    raw_data = load_raw_data_flow()

    df_current_account_trx = raw_data["raw_current_account_transaction"]

    df_current_account_trx_final = clean_current_account_transaction_task(
        df_current_account_trx
    )

    save_data_flow(df_current_account_trx_final)
