import logging
import os
from typing import Iterable, List, Optional, Tuple

from databricks.connect import DatabricksSession
from prefect import task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from data_engineering.core.utils import load_yaml_file

logger = logging.getLogger(__name__)
spark = DatabricksSession.builder.getOrCreate()


@task(name="add_volume_controls_task", tags=["qa", "spark", "metadata"])
def add_volume_controls_task(p_df_series: DataFrame, p_sens: float) -> DataFrame:
    """
    Calcula métricas de control y estado de calidad de volumen por tabla.
    Args:
        p_df_series (DataFrame): Serie mensual por tabla con registros y clientes.
        p_sens (float): Sensibilidad para límites estadísticos.
    Returns:
        DataFrame: DataFrame con métricas de volumen y fecha.
    """

    w_hist = (
        Window.partitionBy("table_name")
        .orderBy("fecha_informacion")
        .rowsBetween(-18, -1)
    )

    k = f.lit(float(p_sens))

    df_stats = p_df_series.select(
        f.col("table_name"),
        f.col("registros"),
        f.col("clientes_unicos"),
        f.avg("registros").over(w_hist).alias("media_18m_registros"),
        f.stddev_samp("registros").over(w_hist).alias("std_18m_registros"),
        f.count("registros").over(w_hist).alias("n_hist_registros"),
        f.avg("clientes_unicos").over(w_hist).alias("media_18m_clientes"),
        f.stddev_samp("clientes_unicos").over(w_hist).alias("std_18m_clientes"),
        f.count("clientes_unicos").over(w_hist).alias("n_hist_clientes"),
        f.col("fecha_informacion"),
    )

    df_limits = df_stats.select(
        f.col("*"),
        (f.col("media_18m_registros") - k * f.col("std_18m_registros")).alias(
            "limite_inferior_registros"
        ),
        (f.col("media_18m_registros") + k * f.col("std_18m_registros")).alias(
            "limite_superior_registros"
        ),
        (f.col("media_18m_clientes") - k * f.col("std_18m_clientes")).alias(
            "limite_inferior_clientes"
        ),
        (f.col("media_18m_clientes") + k * f.col("std_18m_clientes")).alias(
            "limite_superior_clientes"
        ),
    )

    df_final = df_limits.select(
        f.col("table_name").alias("nombre_tabla"),
        f.col("registros"),
        f.col("clientes_unicos"),
        f.col("media_18m_registros"),
        f.col("std_18m_registros"),
        f.col("n_hist_registros"),
        f.col("limite_inferior_registros"),
        f.col("limite_superior_registros"),
        f.when(f.col("n_hist_registros") < 12, f.lit("SIN_HISTORIA"))
        .when(
            f.col("std_18m_registros").isNull() | (f.col("std_18m_registros") == 0),
            f.lit("OK"),
        )
        .when(
            f.col("registros") < f.col("limite_inferior_registros"),
            f.lit("ALERTA_BAJO"),
        )
        .when(
            f.col("registros") > f.col("limite_superior_registros"),
            f.lit("ALERTA_ALTO"),
        )
        .otherwise(f.lit("OK"))
        .alias("estado_calidad_registros"),
        f.when(
            f.col("std_18m_registros").isNull() | (f.col("std_18m_registros") == 0),
            f.lit(None).cast("double"),
        )
        .otherwise(
            (f.col("registros") - f.col("media_18m_registros"))
            / f.col("std_18m_registros")
        )
        .alias("z_score_registros"),
        f.col("media_18m_clientes"),
        f.col("std_18m_clientes"),
        f.col("n_hist_clientes"),
        f.col("limite_inferior_clientes"),
        f.col("limite_superior_clientes"),
        f.when(f.col("n_hist_clientes") < 12, f.lit("SIN_HISTORIA"))
        .when(
            f.col("std_18m_clientes").isNull() | (f.col("std_18m_clientes") == 0),
            f.lit("OK"),
        )
        .when(
            f.col("clientes_unicos") < f.col("limite_inferior_clientes"),
            f.lit("ALERTA_BAJO"),
        )
        .when(
            f.col("clientes_unicos") > f.col("limite_superior_clientes"),
            f.lit("ALERTA_ALTO"),
        )
        .otherwise(f.lit("OK"))
        .alias("estado_calidad_clientes"),
        f.when(
            f.col("std_18m_clientes").isNull() | (f.col("std_18m_clientes") == 0),
            f.lit(None).cast("double"),
        )
        .otherwise(
            (f.col("clientes_unicos") - f.col("media_18m_clientes"))
            / f.col("std_18m_clientes")
        )
        .alias("z_score_clientes"),
        f.col("fecha_informacion"),
    )

    return df_final


@task(name="build_monthly_profile_for_table_task", tags=["qa", "spark", "metadata"])
def build_monthly_profile_for_table_task(
    p_source_fqn: str,
    p_table_name: str,
    p_df_spine: DataFrame,
    p_columna_cliente: Iterable[str],
) -> DataFrame:
    """
    Construye el perfil mensual por columna para una tabla.
    Args:
        p_source_fqn (str): FQN de la tabla origen.
        p_table_name (str): Nombre de la tabla.
        p_df_spine (DataFrame): Spine mensual de fechas.
        p_columna_cliente (Iterable[str]): Columnas candidatas de cliente.
    Returns:
        DataFrame: Perfil mensual por columna.
    """
    df = spark.table(p_source_fqn)

    dfm = df.where(f.col("_observ_end_dt").isNotNull()).withColumn(
        "fecha_informacion", f.to_date(f.col("_observ_end_dt"))
    )

    client_col = next((c for c in p_columna_cliente if c in df.columns), None)

    if client_col:
        df_monthly_clients = dfm.groupBy("fecha_informacion").agg(
            f.countDistinct(f.col(client_col)).cast("long").alias("unique_clients")
        )
    else:
        df_monthly_clients = dfm.groupBy("fecha_informacion").agg(
            f.lit(None).cast("long").alias("unique_clients")
        )

    numeric_types = {
        "tinyint",
        "smallint",
        "int",
        "bigint",
        "float",
        "double",
        "decimal",
    }
    is_numeric = {
        field.name: field.dataType.simpleString().split("(")[0] in numeric_types
        for field in df.schema.fields
    }

    agg_exprs = []
    for c in df.columns:
        agg_exprs.append(f.sum(f.col(c).isNull().cast("int")).alias(f"{c}__nulls"))
        if is_numeric.get(c, False):
            agg_exprs.append(
                f.sum(f.when(f.col(c) == f.lit(0), f.lit(1)).otherwise(f.lit(0)))
                .cast("long")
                .alias(f"{c}__zeros")
            )

    df_monthly_agg = dfm.groupBy("fecha_informacion").agg(*agg_exprs)
    df_monthly_counts = dfm.groupBy("fecha_informacion").agg(
        f.count("*").cast("long").alias("total_rows")
    )

    df_monthly_wide = (
        p_df_spine.join(df_monthly_counts, on="fecha_informacion", how="left")
        .join(df_monthly_agg, on="fecha_informacion", how="left")
        .join(df_monthly_clients, on="fecha_informacion", how="left")
        .withColumn(
            "total_rows", f.coalesce(f.col("total_rows"), f.lit(0)).cast("long")
        )
        .withColumn(
            "unique_clients", f.coalesce(f.col("unique_clients"), f.lit(0)).cast("long")
        )
    )

    metrics_array = f.array(
        *[
            f.struct(
                f.lit(c).alias("column_name"),
                f.lit(df.schema[c].dataType.simpleString()).alias("data_type"),
                f.coalesce(f.col(f"{c}__nulls").cast("long"), f.lit(0)).alias("nulls"),
                (
                    f.coalesce(f.col(f"{c}__zeros").cast("long"), f.lit(0))
                    if is_numeric.get(c, False)
                    else f.lit(None).cast("long")
                ).alias("zeros"),
                f.lit(is_numeric.get(c, False)).alias("is_numeric"),
            )
            for c in df.columns
        ]
    )

    df_profile = df_monthly_wide.withColumn("m", f.explode(metrics_array)).select(
        f.lit(p_table_name).alias("nombre_tabla"),
        f.col("m.column_name").alias("nombre_columna"),
        f.col("m.data_type").alias("tipo_datos"),
        f.col("total_rows").alias("filas_totales"),
        f.col("unique_clients").alias("clientes_unicos"),
        f.col("m.nulls").alias("total_nulos"),
        f.when(
            f.col("total_rows") > 0, f.round(f.col("m.nulls") / f.col("total_rows"), 4)
        )
        .otherwise(f.lit(0.0))
        .alias("porcentaje_nulos"),
        f.col("m.zeros").alias("total_ceros"),
        f.when(
            (f.col("m.is_numeric") == f.lit(True)) & (f.col("total_rows") > 0),
            f.round(f.col("m.zeros") / f.col("total_rows"), 4),
        )
        .otherwise(
            f.when(f.col("m.is_numeric") == f.lit(True), f.lit(0.0)).otherwise(
                f.lit(None).cast("double")
            )
        )
        .alias("porcentaje_ceros"),
        f.col("fecha_informacion"),
    )
    return df_profile


@task(name="get_source_info_task", tags=["qa", "spark", "metadata"])
def get_source_info_task(
    p_source_catalog: str,
    p_source_schema: str,
) -> List[str]:
    """
    Obtiene la lista de tablas del esquema origen excluyendo prefijos.
    Args:
        p_source_catalog (str): Catálogo origen.
        p_source_schema (str): Esquema origen.
    Returns:
        List[str]: Nombres de tablas del esquema origen.
    """
    TABLE_PREFIX_EXCLUSIONS = ("eqmas_", "eqpr_", "qa_", "mc_qa_", "mc_dbd_", "dbd")
    source_db = f"{p_source_catalog}.{p_source_schema}"

    df_tables = spark.sql(f"SHOW TABLES IN {source_db}")
    table_names = [
        r["tableName"]
        for r in df_tables.collect()
        if (not r["tableName"].lower().startswith(TABLE_PREFIX_EXCLUSIONS))
    ]

    return table_names


@task(name="build_profiles_task", tags=["qa", "profile"])
def build_profiles_task(
    p_source_catalog: str,
    p_source_schema: str,
    p_table_names: Iterable[str],
    p_columna_cliente: Iterable[str],
    p_start_dt="",
) -> Tuple[Optional[DataFrame], List[Tuple[str, str, str]]]:
    """
    Construye el perfil mensual para todas las tablas del esquema.
    Args:
        p_source_catalog (str): Catálogo origen.
        p_source_schema (str): Esquema origen.
        p_table_names (Iterable[str]): Tablas a procesar.
        p_columna_cliente (Iterable[str]): Columnas de cliente.
        p_start_dt (str): Fecha mínima opcional para el spine.
    Returns:
        Tuple[DataFrame | None, list]: (perfil final, errores).
    """

    df_final_profile = None
    errors: List[Tuple[str, str, str]] = []

    for t in p_table_names:
        source_fqn = f"{p_source_catalog}.{p_source_schema}.{t}"
        try:
            df_spine = build_monthly_spine_for_table_task(source_fqn, p_start_dt)

            df_prof = build_monthly_profile_for_table_task(
                source_fqn,
                t,
                df_spine,
                p_columna_cliente,
            )

            df_final_profile = (
                df_prof
                if df_final_profile is None
                else df_final_profile.unionByName(df_prof)
            )
        except Exception as e:
            errors.append((t, source_fqn, str(e)))

    return df_final_profile, errors


def build_monthly_spine_for_table_task(p_source_fqn: str, p_start_dt="") -> DataFrame:
    """
    Construye el spine mensual desde el primer _observ_end_dt
    hasta el último día del mes anterior.
    Args:
        p_source_fqn (str): FQN de la tabla origen.
    Returns:
        DataFrame: Spine mensual con fecha_informacion.
    """
    df_min = (
        spark.table(p_source_fqn)
        .where(f.col("_observ_end_dt").isNotNull())
        .select(f.min(f.to_date(f.col("_observ_end_dt"))).alias("min_dt"))
    ).collect()[0]["min_dt"]

    min_dt = f.to_date(f.lit(p_start_dt)) if p_start_dt else df_min

    end_dt = f.last_day(f.add_months(f.trunc(f.current_date(), "MONTH"), -1))

    df_spine = (
        spark.range(1)
        .select(
            f.explode(
                f.sequence(
                    f.last_day(f.lit(min_dt)),
                    end_dt,
                    f.expr("interval 1 month"),
                )
            ).alias("d")
        )
        .select(f.last_day(f.col("d")).alias("fecha_informacion"))
    )

    return df_spine


@task(name="build_volumes_task", tags=["qa", "volume"])
def build_volumes_task(
    p_source_catalog: str,
    p_source_schema: str,
    p_table_names: Iterable[str],
    p_columna_cliente: Iterable[str],
    p_start_dt="",
    p_sens: float = 2.2,
) -> Tuple[Optional[DataFrame], List[Tuple[str, str, str]]]:
    """
    Construye métricas de volumen mensual para todas las tablas.
    Args:
        p_source_catalog (str): Catálogo origen.
        p_source_schema (str): Esquema origen.
        p_table_names (Iterable[str]): Tablas a procesar.
        p_columna_cliente (Iterable[str]): Columnas candidatas de cliente.
        p_start_dt (str): Fecha mínima opcional para el spine.
        p_sens (float): Sensibilidad para límites de volumen.
    Returns:
        Tuple[DataFrame | None, list]: (volumen final, errores).
    """

    df_final_volume = None
    errors: List[Tuple[str, str, str]] = []

    for t in p_table_names:
        source_fqn = f"{p_source_catalog}.{p_source_schema}.{t}"
        try:
            df_spine = build_monthly_spine_for_table_task(source_fqn, p_start_dt)

            df = spark.table(source_fqn)
            df_base = df.where(f.col("_observ_end_dt").isNotNull()).withColumn(
                "fecha_informacion", f.to_date(f.col("_observ_end_dt"))
            )

            df_monthly_counts = df_base.groupBy("fecha_informacion").agg(
                f.count("*").cast("long").alias("registros")
            )

            client_col = next((c for c in p_columna_cliente if c in df.columns), None)
            if client_col:
                df_monthly_clients = df_base.groupBy("fecha_informacion").agg(
                    f.countDistinct(f.col(client_col))
                    .cast("long")
                    .alias("clientes_unicos")
                )
            else:
                df_monthly_clients = df_base.groupBy("fecha_informacion").agg(
                    f.lit(0).cast("long").alias("clientes_unicos")
                )

            df_series = (
                df_spine.join(df_monthly_counts, "fecha_informacion", "left")
                .join(df_monthly_clients, "fecha_informacion", "left")
                .select(
                    f.lit(t).alias("table_name"),
                    f.coalesce(f.col("registros"), f.lit(0))
                    .cast("long")
                    .alias("registros"),
                    f.coalesce(f.col("clientes_unicos"), f.lit(0))
                    .cast("long")
                    .alias("clientes_unicos"),
                    f.col("fecha_informacion"),
                )
            )

            df_vol = add_volume_controls_task(df_series, p_sens)

            df_final_volume = (
                df_vol
                if df_final_volume is None
                else df_final_volume.unionByName(df_vol)
            )
        except Exception as e:
            errors.append((t, source_fqn, str(e)))

    return df_final_volume, errors


@task(name="build_errors_task", tags=["qa", "errors"])
def build_errors_task(p_errors: List):
    """
    Registra errores detectados durante el cálculo de QA.
    Args:
        p_errors (List): Lista de errores de QA.
    Returns:
        None
    """

    if p_errors:
        logger.info("QA ERRORS:")
        for e in p_errors:
            logger.info(e)


@task(name="orchestrate_qa_volume_task", tags=["qa", "orchestration"])
def orchestrate_qa_volume_task(
    p_columna_cliente: Iterable[str],
    p_start_dt="",
) -> DataFrame:
    """
    Orquesta el cálculo de métricas de volumen QA.
    Args:
        p_columna_cliente (Iterable[str]): Columnas candidatas de cliente.
        p_start_dt (str): Fecha mínima opcional para el spine.
    Returns:
        DataFrame: DataFrame final de volumen QA.
    """

    env = "sandbox" if os.getenv("env") not in ["prod", "dev"] else os.getenv("env")
    company = load_yaml_file(os.path.join("env/base", "global_settings.yml")).get(
        "company", ""
    )
    workflow = os.getenv("workflow")

    source_catalog = f"{company}_{env}_de"
    source_schema = f"{os.getenv('env')}_{workflow}"

    table_names = get_source_info_task(
        source_catalog,
        source_schema,
    )

    df_volume, volume_errors = build_volumes_task(
        source_catalog, source_schema, table_names, p_columna_cliente, p_start_dt
    )

    build_errors_task(list(volume_errors))

    return df_volume


@task(name="orchestrate_qa_profile_task", tags=["qa", "orchestration"])
def orchestrate_qa_profile_task(
    p_columna_cliente: Iterable[str], p_start_dt=""
) -> DataFrame:
    """
    Orquesta el cálculo del perfil mensual QA por columna.
    Args:
        p_columna_cliente (Iterable[str]): Columnas candidatas de cliente.
        p_start_dt (str): Fecha mínima opcional para el spine.
    Returns:
        DataFrame: DataFrame final de perfil QA.
    """

    env = "sandbox" if os.getenv("env") not in ["prod", "dev"] else os.getenv("env")
    company = load_yaml_file(os.path.join("env/base", "global_settings.yml")).get(
        "company", ""
    )
    workflow = os.getenv("workflow")

    source_catalog = f"{company}_{env}_de"
    source_schema = f"{os.getenv('env')}_{workflow}"

    table_names = get_source_info_task(
        source_catalog,
        source_schema,
    )

    df_profile, profile_errors = build_profiles_task(
        source_catalog, source_schema, table_names, p_columna_cliente, p_start_dt
    )

    build_errors_task(list(profile_errors))

    return df_profile
