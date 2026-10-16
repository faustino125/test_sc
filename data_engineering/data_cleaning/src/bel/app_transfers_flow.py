from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    clean_currency_task,
    convert_to_hex_task,
    replace_null_or_empty_values,
)


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_transaccion", "cuenta_origen"])
@task(
    name="clean_bel_app_transfers_task",
    tags=["data cleaning", "preprocesing"],
)
def clean_bel_app_transfers_task(p_raw_bel_app_transfers: DataFrame) -> DataFrame:
    """Clean raw data BEL app transactions.

    Args:
        p_raw_bel_app_transfers (DataFrame): Raw data with BEL App transactions.

    Returns:
        DataFrame: Processed BEL app transactions DataFrame.
    """
    df_app_transfers = (
        p_raw_bel_app_transfers.filter(
            (
                (f.col("fecha_transaccion").between("2018-01-01", "2019-01-10"))
                & (f.col("estado_operacion") == "0")
            )
            | (
                (f.col("fecha_transaccion") >= "2019-01-11")
                & (f.col("estado_operacion") == "ok")
            )
        )
        .select(
            f.col("id").alias("id_transaccion"),
            f.col("fecha_transaccion"),
            convert_to_hex_task(f.col("instalacion"), "id_instalacion"),
            convert_to_hex_task(f.col("cuenta_origen"), "cuenta_origen"),
            f.coalesce(
                f.trim(f.col("descripcion_tipo_cuenta_origen")),
                f.lit("no_tiene_cuenta_origen"),
            ).alias("descripcion_cuenta_origen"),
            clean_currency_task(f.col("moneda_debito")).alias(
                "descripcion_moneda_origen"
            ),
            convert_to_hex_task(f.col("cuenta_destino"), "cuenta_destino"),
            (
                f.when(
                    f.col("descripcion_tipo_cuenta_destino").isNotNull(),
                    f.trim(f.col("descripcion_tipo_cuenta_destino")),
                )
                .when(
                    (f.col("descripcion_tipo_cuenta_destino").isNull())
                    & (f.col("tipo_operacion") == "transferencia_ach"),
                    "interbancaria",
                )
                .otherwise("cuenta_destino_desconocida")
                .alias("descripcion_cuenta_destino")
            ),
            clean_currency_task(f.col("moneda_credito")).alias(
                "descripcion_moneda_destino"
            ),
            f.col("canal"),
            f.when(
                (f.col("tipo_operacion").like("%envio%"))
                & (f.col("tipo_operacion").like("%movil%"))
                & ~(f.col("tipo_operacion").like("%propia%")),
                "envio_transferencias_movil",
            )
            .when(
                (f.col("tipo_operacion").like("%cobro%"))
                & (f.col("tipo_operacion").like("%movil%")),
                "cobro_transferencias_movil",
            )
            .otherwise(
                f.regexp_replace(
                    f.regexp_replace(
                        f.trim("tipo_operacion"),
                        r"transferencias?",
                        "transferencias",
                    ),
                    r"propia",
                    "propias",
                )
            )
            .alias("descripcion_operacion"),
            f.round("monto_debito", 2).cast("float").alias("monto_debito"),
            f.round("monto_credito", 2).cast("float").alias("monto_credito"),
        )
        .drop_duplicates()
    )

    return df_app_transfers


@flow(name="app_transfers_flow")
def app_transfers_flow():
    """
    Loads, processes, and saves data in datalake.

    The flow performs the following operations:
    1. Loads raw data using the specified date range.
    2. Cleans and processes the all data.
    3. Saves the processed data to the appropriate environment using the specified
    overwrite strategy.

    Returns:
        None: This flow does not return a value, but it saves the processed data.
    """

    raw_data_app_transfers = load_raw_data_flow()

    df_raw_bel_app_transfers = raw_data_app_transfers["raw_bel_app_transfers"]

    df_app_transfers_final = clean_bel_app_transfers_task(df_raw_bel_app_transfers)

    save_data_flow(df_app_transfers_final)
