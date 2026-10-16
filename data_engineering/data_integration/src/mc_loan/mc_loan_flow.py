import pyspark.sql.functions as f  # type: ignore
from prefect import flow, task  # type: ignore
from pyspark.sql import DataFrame  # type: ignore

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_integration.src.utils.utils import (
    compute_sum_with_fixed_and_variants,
    fill_na,
    get_top_by_order,
    join_dataframes_task,
)


@task(name="get_credit_line")
def get_credit_line(p_df: DataFrame) -> DataFrame:
    """
    Computes credit_line_flag (id_cliente, _observ_end_dt) indicating whether
    the client had an active credit line within the observation window.

    Args:
        p_df (DataFrame): Source DataFrame.

    Returns:
        DataFrame: Columns:
            - 'id_cliente'
            - '_observ_end_dt'
            - 'credit_line_flag' (1 or 0)
    """
    cond = f.col("pre_fecha_primer_desembolso").between(
        f.col("_observ_start_dt"), f.col("_observ_end_dt")
    ) & (f.col("pre_cupo_flag") == 1)

    return p_df.groupBy("id_cliente", "_observ_end_dt").agg(
        f.max(f.when(cond, f.lit(1)).otherwise(f.lit(0))).alias("credit_line_flag")
    )


@task(name="compute_pivot_loans_task")
def compute_pivot_loans(
    p_df: DataFrame,
    p_pivot_col: str,
    p_codigo_col: str,
) -> DataFrame:
    """
    Creates a pivot DataFrame combining product codes and account situation.
    Args:
        p_df (DataFrame): Source DataFrame.
        p_pivot_col (str): Column with situation values to pivot .
        p_codigo_col (str): Column with product codes.
    Returns:
        DataFrame: Pivoted DataFrame with counts and sums per
        product-situation combination.
    """

    mora_situaciones = [
        "cobro_administrativo",
        "en_mora",
        "juridico",
        "proceso_prorroga",
    ]
    exclude_situaciones = ["desconocida"]
    producto_map = {
        "vehiculos": ["201"],
        "vivienda": ["202"],
        "consumo": ["204", "205", "222", "223"],
    }

    p_df = p_df.withColumn(
        p_pivot_col,
        f.when(f.col(p_pivot_col).isin(mora_situaciones), "en_mora").otherwise(
            f.col(p_pivot_col)
        ),
    )

    situaciones = [
        row[p_pivot_col]
        for row in p_df.select(p_pivot_col)
        .distinct()
        .filter(
            f.col(p_pivot_col).isNotNull()
            & ~f.lower(f.col(p_pivot_col)).isin(
                [s.lower() for s in exclude_situaciones]
            )
        )
        .collect()
    ]

    pivot_values = [
        f"{producto}_{situacion}"
        for producto in producto_map
        for situacion in situaciones
    ]

    pivot_expr = f.lit(None).cast("string")
    for nombre, codigos in producto_map.items():
        pivot_expr = f.when(
            f.col(p_codigo_col).isin(codigos),
            f.concat_ws("_", f.lit(nombre), f.col(p_pivot_col)),
        ).otherwise(pivot_expr)

    df_final = (
        p_df.withColumn("pivot_key", pivot_expr)
        .groupBy("id_cliente", "_observ_end_dt")
        .pivot("pivot_key", pivot_values)
        .agg(
            f.count("cuenta_corporativa").cast("int").alias("prestamos_cnt"),
            f.sum("pre_monto_prestamo_quetzalizado_val")
            .cast("float")
            .alias("total_monto_quetzalizado"),
        )
    )
    return df_final


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente"])
@task(name="mc_loan_task", tags=["data integration", "processing"])
def mc_loan_task(
    p_transformed_loan: DataFrame,
) -> DataFrame:
    """Processes and integrates customer data.

    This task integrates the resulting data from the Data Transformation Layer,
      using features from bi loan.

    Args:
        p_transformed_loan (DataFrame): Transformed loan data.

    Returns:
        DataFrame: Unified and processed Dataframe.
    """
    df_aggregated = p_transformed_loan.groupBy("id_cliente", "_observ_end_dt").agg(
        f.sum("pre_cuota_prestamo_quetzalizado_val")
        .cast("float")
        .alias("cuota_prestamo_quetzalizado_sum"),
        f.sum("pre_cuota_prestamo_gtq_val")
        .cast("float")
        .alias("cuota_prestamo_gtq_sum"),
        f.sum("pre_cuota_prestamo_usd_val")
        .cast("float")
        .alias("cuota_prestamo_usd_sum"),
        f.sum("pre_monto_prestamo_quetzalizado_val")
        .cast("float")
        .alias("total_monto_quetzalizado_sum"),
        f.sum("pre_monto_prestamo_gtq_val").cast("float").alias("total_monto_gtq_sum"),
        f.sum("pre_monto_prestamo_usd_val").cast("float").alias("total_monto_usd_sum"),
        f.sum("pre_saldo_prestamo_quetzalizado_val")
        .cast("float")
        .alias("saldo_prestamo_quetzalizado_sum"),
        f.sum("pre_saldo_prestamo_gtq_val")
        .cast("float")
        .alias("saldo_prestamo_gtq_sum"),
        f.sum("pre_saldo_prestamo_usd_val")
        .cast("float")
        .alias("saldo_prestamo_usd_sum"),
        f.coalesce(
            f.min("pre_fecha_primer_desembolso"), f.min("pre_fecha_consecion")
        ).alias("fecha_prestamo_min"),
        f.coalesce(
            f.max("pre_fecha_primer_desembolso"), f.max("pre_fecha_consecion")
        ).alias("fecha_prestamo_max"),
        f.max("pre_fecha_cancelacion").alias("fecha_cancelacion_max"),
    )

    df_pivot = (
        p_transformed_loan.groupBy("id_cliente", "_observ_end_dt")
        .pivot("pre_situacion_cuenta_homologado")
        .agg(
            f.count("cuenta_corporativa").cast("int").alias("prestamos"),
            f.sum("pre_monto_prestamo_quetzalizado_val")
            .cast("float")
            .alias("total_monto_quetzalizado"),
        )
    )

    df_pivot_with_mora = compute_sum_with_fixed_and_variants(
        p_df=df_pivot,
        p_new_col="prestamos_en_mora",
        p_fixed_substring="prestamos",
        p_variable_substrings=[
            "en_mora",
            "cobro_administrativo",
            "proceso_prorroga",
            "juridico",
        ],
    ).select(
        f.col("id_cliente"),
        f.col("_observ_end_dt"),
        f.col("cancelada_prestamos").alias("prestamos_cancelados_cnt"),
        f.col("cancelada_total_monto_quetzalizado").alias("prestamos_cancelados_sum"),
        f.col("vigente_prestamos").alias("prestamos_vigentes_cnt"),
        f.col("vigente_total_monto_quetzalizado").alias("prestamos_vigentes_sum"),
        f.col("prestamos_en_mora").alias("prestamos_en_mora_cnt"),
    )

    df_past_due = df_past_due = get_top_by_order(
        p_transformed_loan.filter(f.col("pre_antiguedad_cartera30D_flag") == 1),
        ["id_cliente", "_observ_end_dt"],
        [
            "pre_antiguedad_cartera30D_flag",
            "pre_fecha_primer_desembolso",
            "pre_monto_prestamo_quetzalizado_val",
        ],
        p_order_type="desc",
    ).select(
        "id_cliente",
        "_observ_end_dt",
        f.coalesce(
            f.col("pre_fecha_primer_desembolso"), f.col("pre_fecha_consecion")
        ).alias("fecha_prestamo_mora"),
        f.col("pre_antiguedad_cartera30D_flag").alias("antiguedad_cartera30D_flag"),
        f.col("pre_antiguedad_cartera60D_flag").alias("antiguedad_cartera60D_flag"),
        f.col("pre_antiguedad_cartera90D_flag").alias("antiguedad_cartera90D_flag"),
        f.col("pre_antiguedad_cartera120D_flag").alias("antiguedad_cartera120D_flag"),
        f.col("pre_antiguedad_cartera180D_flag").alias("antiguedad_cartera180D_flag"),
        f.col("pre_antiguedad_cartera181D_flag").alias("antiguedad_cartera181D_flag"),
    )

    df_credit = get_credit_line(p_transformed_loan)

    df_situacion_producto = compute_pivot_loans(
        p_df=p_transformed_loan,
        p_pivot_col="pre_situacion_cuenta_homologado",
        p_codigo_col="pre_nuevo_codigo_producto",
    )

    df_final = join_dataframes_task(
        ["id_cliente", "_observ_end_dt"],
        [
            df_aggregated,
            df_pivot_with_mora,
            df_past_due,
            df_credit,
            df_situacion_producto,
        ],
    )

    return fill_na(df_final)


@flow(name="mc_loan_flow")
def mc_loan_flow():
    """
    Load, integrate  and saves result data in data lake.

    The flow performs the following operations:
    1. Loads data using the specified date range.
    2. Processes and integrate features data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        features data.
    """
    data_transformation = load_raw_data_flow()

    df_master_loan = mc_loan_task(
        data_transformation["transformed_loan"],
    )

    save_data_flow(df_master_loan)
