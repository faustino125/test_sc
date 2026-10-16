"""Raw Data Validation."""

import os
from datetime import datetime, timedelta

import pyspark.sql.functions as f
from delta.tables import DeltaTable
from prefect import flow, task
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import load_yaml_file

spark = SparkSession.builder.getOrCreate()


@task(name="get_date_range_task", tags=["raw_data", "date_range_in_analysis"])
def get_date_range_task():
    """Retrieve all the days from a given date range.

    Returns:
         DataFrame: DataFrame date sequence.
    """
    spark = SparkSession.builder.getOrCreate()
    start_date = os.getenv("start_dt")
    end_date = os.getenv("end_dt")
    today = datetime.today().date()
    yesterday = today - timedelta(days=1)

    if end_date != "":
        parsed_end = datetime.strptime(end_date, "%Y-%m-%d").date()
        if parsed_end == today:
            end_date = (parsed_end - timedelta(days=2)).strftime("%Y-%m-%d")
        elif parsed_end == yesterday:
            end_date = (parsed_end - timedelta(days=1)).strftime("%Y-%m-%d")

    date_range = spark.sql(
        f"""
            SELECT sequence(
            CASE
                WHEN '{start_date}' != '' AND '{end_date}' != ''
                THEN to_date('{start_date}')
                ELSE to_date(date_trunc('MONTH',
                date_add(current_date() - 2, -day(current_date() - 2))))
            END,
            CASE
                WHEN '{start_date}' != '' AND '{end_date}' != ''
                THEN to_date('{end_date}')
                ELSE to_date(date_add(current_date(), -2))
            END,
            interval 1 day
            ) AS fecha_faltante
            """,
    ).withColumn("fecha_faltante", f.explode(f.col("fecha_faltante")))

    return date_range


@task(name="create_path", tags=["raw_data", "path_dl"])
def create_path_task(
    p_base_path: str, p_tableDomain: str, p_fileName: str, p_date: str
) -> str:
    """Create path for Azure Data Lake.

    Args:
        p_base_path: Base path inside container
        p_tableDomain: general path inside the container
        p_fileName: vista
        p_date: Date string in yyyy/mm format

    Returns:
        str: Data lake path.
    """
    p_tableDomain = p_tableDomain if p_tableDomain[-1] == "/" else (p_tableDomain + "/")
    df_path = p_base_path + p_tableDomain + p_fileName + p_date + "/*"
    return df_path


@task(name="create_output_df_task", tags=["raw_data", "final_df"])
def create_output_df_task(p_base_df: DataFrame, p_missing_data: DataFrame) -> str:
    """Create output dfs.

    Args:
        p_base_path: Base path inside container
        p_tableDomain: general path inside the container

    Returns:
        str: Data lake path.
    """
    return p_base_df.join(p_missing_data, on="VISTA", how="inner").select(
        f.col("VISTA").alias("vista"),
        f.col("fecha"),
        f.col("fecha_faltante"),
        f.col("container"),
        f.col("ruta"),
        f.col("archivo"),
        f.col("tipo_migracion"),
        f.col("base_datos"),
    )


@task(name="read_data_task", tags=["read_raw_data"])
def read_data_task(
    p_dl_path, p_dateColumn, p_tableName, p_data_type, p_start_dt, p_end_dt
):
    """Read data from a given Data lake path.

    Args:
        p_dl_path: Data lake path.
        p_dateColumn: Date column for raw data
        p_tableName: String with historical table Name
        p_data_type: String with data type
        p_start_dt: Start date for analysis
        p_end_dt: End date for analysis

    Returns:
        DataFrame: Dates that have data for the given path.
    """
    try:
        df = spark.read.format("parquet").load(p_dl_path)

        if p_data_type != "catalogo":
            df = (
                df.filter(f.col(p_dateColumn).between(p_start_dt, p_end_dt))
                .groupBy(f.col(p_dateColumn).alias("fecha_faltante"))
                .agg(f.count(p_dateColumn).alias("REGISTROS"))
                .select(
                    f.col("fecha_faltante"),
                    f.col("REGISTROS"),
                    f.lit(p_tableName).alias("VISTA"),
                )
            )
        else:
            df = df.select(
                f.lit("").alias("fecha_faltante"),
                f.count("*").alias("REGISTROS"),
                f.lit(p_tableName).alias("VISTA"),
            )

    except Exception:
        df = spark.createDataFrame(
            data=[{"fecha_faltante": "", "REGISTROS": 0, "VISTA": p_tableName}],
        )

    return df


@task(name="get_missing_data_task", tags=["raw_data", "validation"])
def get_missing_data_task(
    p_raw_data_tables: DataFrame,
    p_raw_records_dwh_summary: DataFrame,
) -> DataFrame:
    """
    Raw data validation.

    This task looks in the azure datalake all the missing dates for
    all data sources extracted from on-premises DB's.

    Args:
        p_raw_data_tables (df): Raw data sources available in the Azure DL Storage.
        p_raw_records_dwh_summary (df): Row count for data sources from
        on premises DB's.

    Returns:
        Tuple: Processed DataFrames with all the missing dates in each data source.
    """
    validation_dates = get_date_range_task()

    table_list = []

    info_schema = StructType(
        [
            StructField(
                "info",
                StructType(
                    [
                        StructField("vista", StringType()),
                        StructField("fecha", StringType()),
                        StructField("ruta", StringType()),
                        StructField("archivo", StringType()),
                        StructField("vista_historica", StringType()),
                    ]
                ),
            )
        ]
    )

    array_schema = ArrayType(info_schema)

    # the Data source comes with an array of JSON objects (all the tables)
    p_raw_data_tables = p_raw_data_tables.withColumn(
        "json_data", f.from_json(f.col("info"), array_schema)
    )

    # Get all elements from each JSON object
    p_raw_data_tables = p_raw_data_tables.withColumn(
        "registro", f.explode(f.col("json_data"))
    )

    # Flatten JSON to source df
    p_raw_data_tables = p_raw_data_tables.select(
        f.col("base_datos"),
        f.col("tipo_migracion"),
        f.col("registro.info.fecha").alias("fecha"),
        f.col("registro.info.vista").alias("tabla"),
        f.col("container"),
        f.col("registro.info.ruta").alias("ruta"),
        f.col("registro.info.archivo").alias("archivo"),
        f.col("registro.info.vista_historica").alias("vista_historica"),
    )

    base_path = load_yaml_file(os.path.join("env/base/global_settings.yml")).get(
        "raw_data_path", ""
    )

    # Traverse tables to append dl_path
    for row in p_raw_data_tables.collect():
        # Create path based on table path
        dl_path = Row(
            dl_path=create_path_task(
                base_path,
                row["ruta"],
                row["archivo"],
                "/*/*" if row["tipo_migracion"] != "catalogo" else "",
            )
        )

        table_list.append(
            Row(
                **{
                    **row.asDict(),
                    **dl_path.asDict(),
                }
            )
        )

    tables_information = spark.createDataFrame(data=table_list).withColumnRenamed(
        "vista_historica", "VISTA"
    )

    schema_for_loop = StructType(
        [
            StructField("fecha_faltante", StringType(), True),
            StructField("intentos", IntegerType(), True),
            StructField("VISTA", StringType(), True),
        ]
    )
    missing_from_dw = spark.createDataFrame([], schema_for_loop)
    df_data_for_ingestion = spark.createDataFrame([], schema_for_loop)

    today = datetime.now().date()
    for row in tables_information.collect():
        dates = spark.createDataFrame(data=[{"fecha_faltante": ""}]).filter(
            f.col("fecha_faltante") != ""
        )

        # get last day for each analysis month
        if row["tipo_migracion"] == "snapshot":
            first = today.replace(day=1)
            last_month = first - timedelta(days=1)
            dates = (
                validation_dates.groupBy(
                    f.last_day("fecha_faltante").alias("fecha_faltante"),
                )
                .count()
                .filter(f.col("fecha_faltante") <= last_month)
                .drop("count")
            )

        else:
            dates = validation_dates

        # Get validation range to avoid traversing
        # all data files
        date_range = dates.agg(
            f.min("fecha_faltante").alias("min_date"),
            f.max("fecha_faltante").alias("max_date"),
        ).first()
        # Get the missing dates in the raw data source files
        try:
            p_raw_data_tables = read_data_task(
                row["dl_path"],
                row["fecha"],
                row["VISTA"],
                row["tipo_migracion"],
                date_range.min_date,
                date_range.max_date,
            ).alias("raw")

        except Exception:
            p_raw_data_tables = spark.createDataFrame(
                data=[{"fecha_faltante": "", "REGISTROS": 0, "VISTA": row["VISTA"]}],
            ).alias("raw")

        record_summary = p_raw_records_dwh_summary.filter(
            f.col("VISTA") == f.lit(row["VISTA"])
        )

        # Data contract to match between on premises DWH and DL
        p_raw_data_tables = p_raw_data_tables.join(
            record_summary.withColumnRenamed("FECHA", "fecha_faltante").alias("dwh"),
            on=["VISTA", "fecha_faltante"],
            how="left",
        )

        if row["tipo_migracion"] != "catalogo":
            # Get all missing data dates
            p_raw_data_tables = (
                p_raw_data_tables.join(
                    dates,
                    on="fecha_faltante",
                    how="right",
                )
                .filter(
                    (f.col("raw.fecha_faltante").isNull())
                    | (
                        (f.col("dwh.REGISTROS") != f.col("raw.REGISTROS"))
                        & (f.lit(row["tipo_migracion"] == "snapshot"))
                    )
                )
                .select(
                    dates.fecha_faltante.cast(StringType()).alias("fecha_faltante"),
                    (
                        f.datediff(
                            f.lit(today - timedelta(days=2)), dates.fecha_faltante
                        )
                        / 7
                    )
                    .cast(IntegerType())
                    .alias("intentos"),
                    f.lit(row["VISTA"]).alias("VISTA"),
                )
            )

            missing_from_dw = missing_from_dw.unionByName(
                p_raw_data_tables.filter(f.col("intentos") >= 1)
            )

            if os.getenv("end_dt") == "":
                # Standard execution, just last week for analysis
                p_raw_data_tables = p_raw_data_tables.filter(f.col("intentos") < 1)
            else:
                p_raw_data_tables = p_raw_data_tables.filter(
                    f.col("fecha_faltante").between(
                        os.getenv("start_dt"), os.getenv("end_dt")
                    )
                )
        else:
            p_raw_data_tables = p_raw_data_tables.filter(
                f.col("raw.REGISTROS") == 0
            ).select(
                f.col("fecha_faltante"),
                f.lit(0).cast(IntegerType()).alias("intentos"),
                f.lit(row["VISTA"]).alias("VISTA"),
            )

        # Append the missing dates to the raw table information
        df_data_for_ingestion = df_data_for_ingestion.unionByName(p_raw_data_tables)

    df_final = create_output_df_task(tables_information, df_data_for_ingestion)

    df_final_missing_from_dwh = create_output_df_task(
        tables_information, missing_from_dw
    ).withColumn("revisada", f.lit(False))

    return (df_final, df_final_missing_from_dwh)


@task(name="standardize_df_task", tags=["Delta", "missing_dates"])
def standardize_df_task(p_missing_dates: DataFrame) -> DataFrame:
    """Collect DataFrame to dump as a single json file.

    Args:
        p_missing_dates (df): DataFrame with all raw data missing dates

    Returns:
        DataFrame: Dataframe with list of records on single row
    """
    return p_missing_dates.select(
        f.collect_list(f.struct(p_missing_dates.columns)).alias("source")
    ).coalesce(1)


@task(name="load_dwh_missing_data_catalog_task", tags=["Delta", "missing_dates"])
def load_dwh_missing_data_catalog_task(
    p_base_path: str, p_env: str, p_catalog: str, p_df_missing_dates_dwh: DataFrame
) -> DataFrame:
    """Collect DataFrame to dump as a single json file.

    Args:
        p_base_path (str): Base path to storage account
        p_env (str): Execution environment
        p_catalog (str): Catalog in Databricks
        p_df_missing_dates_dwh (df): DataFrame with all raw data missing in DWH

    Returns:
        DataFrame: Dataframe with all confirmed missing data from DWH
    """
    full_path = (
        p_base_path
        + "raw_data/"
        + os.getenv("output_domain")
        + "/raw_datos_faltantes_dwh"
    )
    # Create external table (delta) for missing dates in DWH
    # Data that we've tried to ingest for 7 straight days without success
    spark.sql(f"USE CATALOG {p_catalog}")
    folder = os.getenv("folder")
    db_table = f"{p_env}_{folder}.raw_datos_faltantes_dwh"

    spark.sql(
        f"""
    CREATE TABLE IF NOT EXISTS {db_table} (
        vista STRING COMMENT 'Vista histórica en DWH',
        fecha STRING COMMENT 'Columna de partición',
        fecha_faltante STRING COMMENT
        'Fecha que hace falta en fuente de información',
        container STRING  COMMENT
        'Container en el que debería estar ubicada la información',
        ruta STRING  COMMENT 'Ruta dentro de container',
        archivo STRING  COMMENT 'Nombre de archivo en fuente de información',
        tipo_migracion STRING  COMMENT 'Tipo de fuente de información',
        base_datos STRING  COMMENT 'Base de datos en DWH',
        revisada BOOLEAN COMMENT
        'Indica si se validó manualmente que no existiera la información en DWH'
    )
    USING DELTA
    LOCATION '{full_path}'
    COMMENT 'Información que no existe en DWH (más de 7 días tratando de reingestarla)'
    """
    )

    delta_table = DeltaTable.forPath(spark, full_path)

    date_range = (
        get_date_range_task()
        .agg(
            f.min("fecha_faltante").alias("min_date"),
            f.max("fecha_faltante").alias("max_date"),
        )
        .first()
    )

    delta_table.alias("t").merge(
        p_df_missing_dates_dwh.alias("s"),
        "t.vista = s.vista AND t.fecha_faltante = s.fecha_faltante",
    ).whenNotMatchedInsertAll().whenNotMatchedBySourceDelete(
        condition=f"""
        t.fecha_faltante BETWEEN '{date_range.min_date}'
        AND '{date_range.max_date}'
        """
    ).execute()

    return delta_table.toDF()


@task(name="filter_output_df_task", tags=["Delta", "missing_dates"])
def filter_output_df_task(
    p_output_df: DataFrame, p_missing_in_dwh: DataFrame
) -> DataFrame:
    """Filter output DataFrame

    Args:
        p_output_df (df): DataFrame with all raw data missing dates
        p_output_df (df): DataFrame with confirmed missing dates in DWH

    Returns:
        DataFrame: Dataframe with dates to reingest
    """
    return (
        p_output_df.alias("o")
        .join(p_missing_in_dwh.alias("dw"), on=["vista", "fecha_faltante"], how="left")
        .filter((~f.col("dw.revisada")) | (f.col("dw.revisada").isNull()))
        .select(f.col("o.*"))
    )


@flow(name="missing_raw_data_flow")
def missing_raw_data_flow():
    """
    Loads, processes, and saves missing raw data information in the datalake.

    The flow performs the following operations:
    1. Loads raw data sources.
    2. Gets the tables that are missing information for a given date.
    3. Saves the processed data to the appropriate environment using the specified
    overwrite strategy.

    Returns:
        None: This flow does not return a value, but it saves the processed income data.
    """
    raw_data = load_raw_data_flow()

    df_dates_for_reingestion, df_missing_dates_dwh = get_missing_data_task(
        raw_data["raw_tables"],
        raw_data["raw_records_dwh_summary"],
    )

    base_path = "abfss://$env@datalake$saccbi.dfs.core.windows.net/"

    storage_account = ""
    container = ""
    additional_path = ""
    env = os.getenv("env")
    company = load_yaml_file(os.path.join("env/base/global_settings.yml")).get(
        "company"
    )
    catalog = ""
    if env == "prod":
        storage_account = env
        container = env + "de"
        additional_path = env
        catalog = company + "_" + env + "_de"
    elif env == "dev":
        storage_account = "desa"
        container = env + "de"
        additional_path = env
        catalog = company + "_" + env + "_de"
    else:
        storage_account = "desa"
        container = "sandbox"
        additional_path = "de/" + env
        catalog = company + "_sandbox_de"

    base_path = (
        base_path.replace("$env", container).replace("$sacc", storage_account)
        + additional_path
        + "/"
    )

    # Update missing dates in DWH
    # tables we've had tried to ingest for more than 7 days without sucess
    df_dwh_missing_catalog = load_dwh_missing_data_catalog_task(
        base_path, env, catalog, df_missing_dates_dwh
    )

    # Remove tables which we already know they don't have data at the DWH
    df_dates_for_reingestion = filter_output_df_task(
        df_dates_for_reingestion, df_dwh_missing_catalog
    )

    save_data_flow(df_dates_for_reingestion)

    # Json file for ADF
    json_format_df = standardize_df_task(df_dates_for_reingestion)

    path = base_path + "raw_data/jsonForADF"

    # Create JSON file for ADF data ingestion
    json_format_df.write.mode("overwrite").format("json").save(path)
