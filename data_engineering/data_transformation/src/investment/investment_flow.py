from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_transformation.src.utils.utils import (
    add_exchange_rate_task,
    create_currency_col_task,
    join_dataframes_task,
)


@task(
    name="process_investment_account_task", tags=["data transformation", "processing"]
)
def process_investment_transaction_task(
    p_investment_transaction: DataFrame, p_exchange_rate: DataFrame
) -> DataFrame:
    """
    Processes investment transactions by joining them with daily exchange rates
    and performing various aggregations.

    Args:
        p_investment_transaction (DataFrame): DataFrame investment transactions.
        p_exchange_rate (DataFrame): DataFrame containing daily exchange rates.

    Returns:
        DataFrame: Processed DataFrame with aggregated investment transaction data.
    """
    df_transaction = add_exchange_rate_task(
        p_investment_transaction, p_exchange_rate, True
    )

    df_investment = df_transaction.select(
        df_transaction._observ_end_dt,
        f.col("id_cliente"),
        f.col("cuenta_corporativa"),
        f.col("monto"),
        f.col("descripcion_operacion"),
        *create_currency_col_task("id_moneda", "monto", "tasa_cambio"),
    )

    df_investment = (
        df_investment.groupBy("_observ_end_dt", "id_cliente", "cuenta_corporativa")
        .pivot("descripcion_operacion")
        .agg(
            f.sum(f.when(f.col("monto") > 0, 1).otherwise(0)).alias("cnt"),
            f.sum("monto_gtq").cast("float").alias("monto_total_gtq"),
            f.sum("monto_usd").cast("float").alias("monto_total_usd"),
            f.sum("monto_quetzalizado").cast("float").alias("monto_total_quetzalizado"),
        )
    )

    return df_investment


@task(
    name="process_investment_account_task", tags=["data transformation", "processing"]
)
def process_investment_account_task(
    p_investment_account: DataFrame, p_exchange_rate
) -> DataFrame:
    """
    Processes investment accounts joining them with daily exchange rates.

    Args:
        p_investment_account (DataFrame): DataFrame containing investment account data.
        p_exchange_rate (DataFrame): DataFrame containing daily exchange rates.

    Returns:
        DataFrame: Processed DataFrame with investment account data.
    """
    df_investment = add_exchange_rate_task(p_investment_account, p_exchange_rate, True)

    df_investment = df_investment.select(
        df_investment._observ_end_dt,
        f.col("id_cliente"),
        f.col("cuenta_corporativa"),
        f.col("id_producto"),
        f.col("descripcion_producto"),
        f.col("situacion_cuenta"),
        f.col("situacion_cuenta_homologado"),
        f.col("tipo_carterizacion"),
        f.col("plazo"),
        f.col("plazo_ampliacion"),
        f.col("fecha_ultima_actividad"),
        f.col("fecha_cancelacion"),
        f.col("fecha_apertura"),
        f.col("fecha_vencimiento"),
        f.col("fecha_ultimo_movimiento"),
        f.col("id_moneda"),
        f.col("descripcion_moneda"),
        f.when(f.col("id_tipo_cuenta") == "2", "S")
        .otherwise("N")
        .alias("cuenta_mancomunada_flag"),
        f.col("pf_cancelacion_anticipada_flag"),
        f.col("producto_mala_situacion_flag"),
        f.col("flag_cliente_juridico_activo"),
        f.when(
            f.coalesce(f.col("fecha_cancelacion"), df_investment._observ_end_dt)
            < f.col("fecha_apertura"),
            0,
        )
        .otherwise(
            f.floor(
                f.months_between(
                    f.coalesce(
                        f.col("fecha_cancelacion"), df_investment._observ_end_dt
                    ),
                    f.col("fecha_apertura"),
                )
            ).cast("int")
        )
        .alias("vigencia_meses"),
        f.col("flag_institucional"),
        *create_currency_col_task("id_moneda", "saldo", "tasa_cambio"),
        *create_currency_col_task("id_moneda", "saldo_anterior", "tasa_cambio"),
        *create_currency_col_task("id_moneda", "saldo_promedio_6_meses", "tasa_cambio"),
        *create_currency_col_task("id_moneda", "interes_mensual", "tasa_cambio"),
        *create_currency_col_task("id_moneda", "aportacion_pactada", "tasa_cambio"),
        *create_currency_col_task("id_moneda", "aporte_pendiente", "tasa_cambio"),
        *create_currency_col_task("id_moneda", "monto_inicial", "tasa_cambio"),
    )

    return df_investment


@arrange_columns(p_start_cols=["_observ_end_dt", "_particion", "id_cliente"])
@task(name="transform_investment_task", tags=["data transformation", "processing"])
def transform_investment_task(
    p_investment, p_exchange_rate, p_investment_transaction
) -> DataFrame:
    """
    Transforms investment data by processing investment accounts and transactions,
    and then joining the results.

    Args:
        p_investment (DataFrame): The investment data.
        p_exchange_rate (DataFrame): The daily exchange rates data.
        p_investment_transaction (DataFrame): The investment transaction data.

    Returns:
        DataFrame: The transformed investment data after joining investment accounts
        and transactions on specific columns.
    """

    df_investment_account = process_investment_account_task(
        p_investment, p_exchange_rate
    )
    df_investment_transaction = process_investment_transaction_task(
        p_investment_transaction, p_exchange_rate
    )

    df_investment_features = join_dataframes_task(
        ["id_cliente", "cuenta_corporativa", "_observ_end_dt"],
        [df_investment_account, df_investment_transaction],
    )

    return df_investment_features


@flow(name="investment_flow")
def investment_flow():
    """
    Loads, transforms and saves investment features in the data lake.

    The flow performs the following operations:
    1. Loads clean investment data using the specified date range.
    2. Transforms and processes the investment features data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        investment features data.
    """
    raw_data = load_raw_data_flow()

    df_investment_features = transform_investment_task(
        raw_data["cleaned_investment"],
        raw_data["cleaned_exchange_rate"],
        raw_data["cleaned_investment_transaction"],
    )

    save_data_flow(df_investment_features)
