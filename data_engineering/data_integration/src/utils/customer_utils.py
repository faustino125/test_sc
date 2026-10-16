from prefect import task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def tier_level_expr(column_name: str, tiers: tuple[int, int, int]):
    """Build a tier expression for a numeric column using three thresholds."""
    column = f.col(column_name)
    tier_small, tier_low, tier_medium = tiers
    return (
        f.when(column.isNull() | (column <= 0), None)
        .when(column <= tier_small, 1)
        .when(column <= tier_low, 2)
        .when(column <= tier_medium, 3)
        .otherwise(4)
    )


@task(name="legal_segment_mapping_task", tags=["data integration", "processing"])
def legal_segment_mapping_task(df_legal: DataFrame) -> DataFrame:
    """Maps legal segments based on income, catchment, and credit transactions.

    Args:
        df_legal (DataFrame): DataFrame containing legal segment information.

    Returns:
        DataFrame: A DataFrame with legal segment mapping.
    """
    INCOME_TIERS = (560_000, 8_000_000, 60_000_000)
    CATCHMENT_TIERS = (160_000, 1_000_000, 15_000_000)

    df_with_data = df_legal.select(
        f.col("id_cliente"),
        f.col("_observ_end_dt"),
        f.col("cli_grupo_corporativo"),
        f.col("mon_flag_institucional"),
        f.col("aho_flag_institucional"),
        f.col("inv_flag_institucional"),
        tier_level_expr("cli_ingresos_anuales_gtq", INCOME_TIERS).alias(
            "nivel_ingreso"
        ),
        tier_level_expr("total_captacion_quetzalizado", CATCHMENT_TIERS).alias(
            "nivel_captacion"
        ),
        tier_level_expr(
            "cli_ingresos_anuales_monetarios_gtq",
            INCOME_TIERS,
        ).alias("nivel_ingresos_monetarios"),
    )

    non_null_count = (
        f.col("nivel_ingreso").isNotNull().cast("int")
        + f.col("nivel_captacion").isNotNull().cast("int")
        + f.col("nivel_ingresos_monetarios").isNotNull().cast("int")
    )

    nivel_expr = f.when(non_null_count == 0, None).otherwise(
        f.round(
            (
                f.coalesce(f.col("nivel_ingreso"), f.lit(0))
                + f.coalesce(f.col("nivel_captacion"), f.lit(0))
                + f.coalesce(f.col("nivel_ingresos_monetarios"), f.lit(0))
            )
            / non_null_count,
            0,
        ).cast("int")
    )

    df_with_data = df_with_data.withColumn("nivel", nivel_expr)

    grupo_df = (
        df_with_data.filter(~f.col("cli_grupo_corporativo").isin("desconocido", "0000"))
        .groupBy("_observ_end_dt", "cli_grupo_corporativo")
        .agg(f.max("nivel").alias("max_grupo"))
    )

    is_institutional = (
        (f.col("mon_flag_institucional") == 1)
        | (f.col("aho_flag_institucional") == 1)
        | (f.col("inv_flag_institucional") == 1)
    )

    effective_nivel = f.coalesce(f.col("max_grupo"), f.col("nivel"))

    result_df = df_with_data.join(
        grupo_df,
        on=["_observ_end_dt", "cli_grupo_corporativo"],
        how="left",
    ).select(
        f.col("id_cliente"),
        f.col("_observ_end_dt"),
        f.when(f.col("nivel").isNull(), "CLIENTE SIN SALDO")
        .when(
            is_institutional
            & (
                f.col("cli_grupo_corporativo").isNull()
                | f.col("cli_grupo_corporativo").isin("desconocido", "0000")
            ),
            f.when(effective_nivel == 1, "INSTITUCIONAL PEQUEÑA")
            .when(effective_nivel == 2, "INSTITUCIONAL BAJO")
            .when(effective_nivel == 3, "INSTITUCIONAL MEDIO")
            .when(effective_nivel == 4, "INSTITUCIONAL GRANDE")
            .otherwise("CLIENTE SIN SALDO"),
        )
        .otherwise(
            f.when(effective_nivel == 1, "MICROEMPRESA")
            .when(effective_nivel == 2, "PYME BAJO")
            .when(effective_nivel == 3, "PYME MEDIO")
            .when(effective_nivel == 4, "EMPRESA GRANDE")
            .otherwise("CLIENTE SIN SALDO")
        )
        .alias("segmento_juridico"),
    )
    return result_df
