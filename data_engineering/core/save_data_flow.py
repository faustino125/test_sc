import os

from databricks.connect import DatabricksSession  # type: ignore
from prefect import flow, get_run_logger, task  # type: ignore
from pyspark.sql import functions as f  # type: ignore

from data_engineering.core.documentation_hive import process_product_schemas
from data_engineering.core.utils import (
    add_prefix_to_columns_task,
    create_year_month_period_task,
    load_yaml_file,
    rename_val_columns_task,
)

spark = DatabricksSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")


def _execute_merge_by_dataset(p_output_df, p_table):
    """
    Executes a MERGE operation to delete existing records for the same datasets.

    Args:
        p_output_df (DataFrame): The DataFrame containing the new data to be merged.
        p_table (str): The name of the target database table.
    """
    table_exists = False
    try:
        table_exists = spark.catalog.tableExists(p_table)
    except Exception:
        table_exists = False

    if table_exists:
        combos = (
            p_output_df.select("nombre_dataset", "_observ_end_dt").distinct().collect()
        )

        if combos:
            conditions = []
            for row in combos:
                conditions.append(
                    f"(nombre_dataset = '{row['nombre_dataset']}' "
                    f"AND _observ_end_dt = '{row['_observ_end_dt']}')"
                )

            delete_sql = f"DELETE FROM {p_table} WHERE {' OR '.join(conditions)}"
            spark.sql(delete_sql)

    return ("append" if table_exists else "overwrite", table_exists)


@task(name="save_delta_file_task", tags=["save", "delta"])
def save_delta_file_task(
    p_output_df,
    p_folder_name: str,
    p_overwrite_strategy: str,
    p_partition_col: str,
    p_table_folder: str,
    p_schema: str,
    p_table_description: str,
    p_mode: str = None,
):
    """
    Saves a DataFrame as a Delta file.

    This task writes a DataFrame in Delta format to the specified folder. It uses the
    overwrite strategy to determine whether to overwrite the schema or not and
    partitions the data by the specified partition column.

    Every file from any previous version older than seven days (168 hours) wil be
    deleted.

    Args:
        p_output_df (DataFrame): The DataFrame to save.
        p_folder_name (str): The folder where the Delta file will be saved.
        p_overwrite_strategy (str): The strategy for overwriting data.
        p_partition_col (str): The column by which the data should be partitioned.
        p_table_folder (str): Output folder name.
        p_schema (str): Represents a logical grouping of data objects.
        p_table_description (str) : table description
        p_mode (str, optional): Mode of saving data.
    """
    logger = get_run_logger()
    main_env = os.getenv("new_env")

    prefix_path = os.path.join("env/base", "prefixes.yml")

    prefix = load_yaml_file(prefix_path).get(os.getenv("output_domain"), {})

    root_dir = os.path.dirname(os.path.dirname(__file__))
    company = load_yaml_file(
        os.path.join(root_dir, "env/base/global_settings.yml")
    ).get("company")

    table = prefix + "_" + p_table_folder
    db_table = f"{p_schema}.{table}"

    catalog = company + "_" + main_env + "_de"

    spark.sql(f"USE CATALOG {catalog}")

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {p_schema} MANAGED LOCATION '{p_folder_name}'"
    )

    options = {}
    overwrite = False
    min_date = os.getenv("start_dt")
    max_date = os.getenv("end_dt")
    full_path = p_folder_name + "/" + table

    if "_observ_start_dt" not in p_output_df.columns and p_partition_col != "":
        p_output_df = p_output_df.select(
            f.col("*"),
            f.trunc(f.col(p_partition_col), "month").alias("_observ_start_dt"),
        )
    else:
        p_output_df = p_output_df.select(f.col("*"))

    if p_partition_col != "_observ_end_dt" and p_partition_col != "":
        p_output_df = p_output_df.withColumn(
            "_observ_end_dt", f.last_day(f.col(p_partition_col))
        )

    if p_mode == "MERGE_BY_DATASET":
        write_mode, table_exists = _execute_merge_by_dataset(p_output_df, db_table)

        writer = p_output_df.write.format("delta").mode(write_mode)

        options["mergeSchema"] = "true"
        options["isManaged"] = "true"

        if p_partition_col not in ("fecha_catalogo", ""):
            writer = writer.partitionBy(p_partition_col)

        writer.saveAsTable(name=db_table, path=full_path, **options)

    else:
        writer = p_output_df.write.format("delta").mode("overwrite")

        options = {}
        options["overwriteSchema"] = "true"

        if p_partition_col not in ("fecha_catalogo", ""):
            writer = writer.partitionBy(p_partition_col)

            if p_overwrite_strategy == "replaceWhere":
                options[
                    p_overwrite_strategy
                ] = f"{p_partition_col} BETWEEN '{min_date}' AND '{max_date}'"
                options["overwriteSchema"] = "false"

        writer.option("isManaged", "true").saveAsTable(
            name=db_table, path=full_path, **options
        )

        if p_partition_col not in ("fecha_catalogo", ""):
            spark.sql(
                f"""OPTIMIZE {db_table}
                WHERE {p_partition_col} BETWEEN '{min_date}' AND '{max_date}'"""
            )

        overwrite = (
            True if os.getenv("overwrite_strategy") == "overwriteSchema" else False
        )
        product_folder_path = os.path.join(
            root_dir, os.getenv("folder"), "src", os.getenv("output_domain")
        )
        process_product_schemas(
            product_folder_path,
            db_table,
            p_table_description,
            p_partition_col,
            overwrite,
        )

    logger.info(f"Data Saved at Path:--{full_path}--")
    spark.sql(f"VACUUM {db_table} RETAIN 168 HOURS")
    logger.info(f"VACUUMING process for :--{db_table}-- completed")


@flow(name="save_data_flow")
def save_data_flow(p_output_df, p_mode: str = None):
    """
    Saves processed data into a Delta format, applying configurations from a YAML file.

    The flow performs the following operations:
    1. Loads global settings and data catalog configurations from YAML files.
    2. Constructs the output file path using the provided key and environment settings.
    3. Saves the data in Delta format using the appropriate overwrite strategy and
    partition column.

    Args:
        p_output_df (DataFrame): The DataFrame to be saved.
        p_mode (str, optional): Mode of saving data. Defaults to None.

    Returns:
        None: This flow does not return a value, but it saves the processed data.

    Raises:
        Exception: An error occurred while saving the data. The specific error message
        is logged.
    """
    logger = get_run_logger()

    try:
        data_subfolder = os.getenv("folder")
        env = os.getenv("env")
        output_domain = os.getenv("output_domain")
        pipeline_flow = os.getenv("flow")
        key = os.getenv("flow_key")
        logger.info(f"Starting save_data_flow for : {key}")

        data_catalog = os.path.join(os.path.join(data_subfolder, "conf/io_config.yml"))
        file_info = load_yaml_file(data_catalog).get(key, {})
        file_name = file_info.get("Output", "")
        table_description = file_info.get("description", "")
        input_partition_col = file_info.get("input_partition_col", "")
        global_settings = "env/"
        container_folder = ""
        container = ""
        if env in ["prod", "dev"]:
            global_settings += env
            container = env + "de"
            container_folder = env + "/"
            schema = env + "_" + data_subfolder
            os.environ["new_env"] = env
        else:
            global_settings += "dev"
            os.environ["new_env"] = "sandbox"
            container = "sandbox"
            container_folder = "de/" + env + "/"
            schema = env + "_" + data_subfolder

        global_settings = load_yaml_file(global_settings + "/global_settings.yml")
        output_data = (
            global_settings.get("company_path", "").replace("$env", container)
            + container_folder
        )

        output_data += data_subfolder
        domain_path = os.path.join(output_data, output_domain)

        is_sc_results = output_domain == "sanity_check"
        if (
            data_subfolder in ["data_transformation", "data_integration"]
            and pipeline_flow != "dashboard_flow"
        ) and not is_sc_results:
            prefix_path = os.path.join("env/base", "prefixes.yml")
            prefix = load_yaml_file(prefix_path).get(output_domain, {})

            p_output_df = add_prefix_to_columns_task(p_output_df, prefix)
            p_output_df = rename_val_columns_task(p_output_df)
            if pipeline_flow not in ["full_load_flow", "qa_flow"]:
                p_output_df = create_year_month_period_task(
                    p_output_df, input_partition_col
                )

        overwrite_strategy = os.getenv("overwrite_strategy")

        if os.getenv("auto_sanity_check_enabled") == "1" and not is_sc_results:
            from data_engineering.core.sanity_check.sanity_check_flow import (
                pre_save_quality_checks,
            )

            pre_save_quality_checks(
                p_output_df,
                file_name,
                data_subfolder,
                output_domain,
                p_date_col=input_partition_col or None,
            )

        save_delta_file_task(
            p_output_df,
            domain_path,
            overwrite_strategy,
            input_partition_col,
            file_name,
            schema,
            table_description,
            p_mode,
        )

    except Exception as e:
        logger.error(f"An error occurred while saving data the data: {e}")
        raise
