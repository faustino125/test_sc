import os

from databricks.connect import DatabricksSession
from prefect import flow, get_run_logger, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.utils import load_yaml_file

spark = DatabricksSession.builder.getOrCreate()


@task(name="load_data_source_task", tags=["load", "raw_data"])
def load_data_source_task(
    p_file_path: str, p_file_extension: str, p_is_sandbox_load=False
) -> DataFrame:
    """
    Loads data from a specified file path using pyspark and converting it into a
    dataframe.

    This task uses Spark to read data from a given file path. It supports reading
    different file formats specified by the p_file_extension parameter.
    It logs the process of loading and potential errors.

    Args:
        p_file_path (str): The path to the data file.
        p_file_extension (str): The format of the file to read (parquet or delta files).
        p_is_sandbox_load (Bool): Flag that tells whether or not we are executing from
        a sandbox env.

    Returns:
        DataFrame: The DataFrame loaded from the specified file.

    Raises:
        Exception: An error occurred while reading data from the specified path.
        The specific error message is logged.
    """
    logger = get_run_logger()
    try:
        logger.info(f"Loading data from: {p_file_path}")
        df = spark.read.format(p_file_extension).load(p_file_path)
        if df.limit(1).count() == 0:
            raise ValueError(f"No data found at path: {p_file_path}")
        logger.info(f"file loaded correctly: {p_file_path}")

        return df
    except Exception as e:
        if p_is_sandbox_load:
            logger.info(
                f"Data not available in: {p_file_path}, switching to dev environment"
            )
        else:
            logger.error(
                f"An error occurred while reading data from: {p_file_path} : {e}"
            )
            raise


@task(name="validate_date_range_filter_task", tags=["filter", "date_column"])
def validate_date_range_filter_task(
    p_df,
    p_partition_col: str,
    p_input_partition_col: str,
    p_start_dt: str,
    p_end_dt: str,
) -> DataFrame:
    """
    Filters a DataFrame by a specified date range and modifies the partition column.

    This task filters a DataFrame by a specified date range between `p_start_dt` and
    `p_end_dt` on a specified `p_partition_col`.
    It also transforms the date column and adds an end of month column.

    Args:
        p_df (DataFrame): The DataFrame to filter.
        p_partition_col (str): The column name to filter on.
        p_input_partition_col (str): The new alias for the transformed date column.
        p_start_dt (str): The start date for filtering.
        p_end_dt (str): The end date for filtering.

    Returns:
        DataFrame: The filtered and transformed DataFrame.

    Raises:
        Exception: An error occurred while filtering the DataFrame. The specific error
        message is logged.
    """
    logger = get_run_logger()
    try:
        if p_partition_col:
            if (
                p_start_dt
                and p_end_dt
                and p_input_partition_col != "fecha_catalogo"
                and p_partition_col != "fecha_catalogo"
            ):
                logger.info("Filtering DataFrame by date range")
                p_df = p_df.filter(f.col(p_partition_col).between(p_start_dt, p_end_dt))

            alias_col = (
                p_input_partition_col if p_input_partition_col else p_partition_col
            )
            p_df = p_df.select(
                *filter(lambda c: c != p_partition_col, p_df.columns),
                f.to_date(f.col(p_partition_col), "yyyy-MM-dd").alias(alias_col),
            )

        return p_df
    except Exception as e:
        logger.error(f"Error while trying to filter Dataframe by date range: {e}")
        raise


@flow(name="load_raw_data_flow")
def load_raw_data_flow() -> DataFrame:
    """
    Extracts raw data from specified sources, applies date range filtering, and returns
    the DataFrame.

    The flow performs the following operations:
    1. Loads global settings and data catalog configurations from YAML files.
    2. Constructs file paths and extensions from the data catalog based on the provided
    key.
    3. Loads data using the constructed file path and file extension.
    4. Applies date range filtering to the loaded data.

    Returns:
        DataFrame: The final processed DataFrame.

    Raises:
        Exception: An error occurred during data loading and processing. The specific
        error message is logged.
    """
    logger = get_run_logger()
    fallback_full_path = None
    try:
        data_subfolder = os.getenv("folder")
        env = os.getenv("env")
        is_sandbox = env not in ["prod", "dev"]

        base_path = "env/"

        container = ""
        additional_path = ""
        extended_path = ""
        input_data = ""
        if data_subfolder in ["data_cleaning", "raw_data"]:
            base_path += "base"
            extended_path = "raw_data_path"
        else:
            additional_path = env
            extended_path = "company_path"
            if not is_sandbox:
                base_path += env
                container = env + "de"
            else:
                base_path += "dev"
                container = "sandbox"
                additional_path = "de/" + additional_path

            if data_subfolder == "data_integration":
                input_data = "/data_transformation"
            else:
                input_data = "/data_cleaning"

        key = os.getenv("flow_key")

        logger.info(f"Starting flow_extracting_raw_data for: {key}")
        data_catalog_path = os.path.join(
            os.path.join(data_subfolder, "conf/data_catalog.yml")
        )

        io_config = os.path.join(os.path.join(data_subfolder, "conf/io_config.yml"))
        data_catalog = load_yaml_file(data_catalog_path).get("Input", {})

        io_config_file = load_yaml_file(io_config).get(key, {})
        input_file = io_config_file.get("Input")

        prefix_path = os.path.join("env/base", "prefixes.yml")

        prefix_dict = {v: k for k, v in load_yaml_file(prefix_path).items()}
        globals = load_yaml_file(base_path + "/global_settings.yml")

        dataframes = {}

        if (
            data_subfolder == "data_cleaning"
            and os.getenv("auto_sanity_check_enabled") == "1"
        ):
            from data_engineering.core.sanity_check.engine import LayerUtils
            from data_engineering.core.sanity_check.sanity_check_flow import (
                execute_sanity_check,
            )

            _source_layer = LayerUtils.LAYER_DEPENDENCIES.get(data_subfolder)
            _output_domain = os.getenv("output_domain")
            _flow_key = os.getenv("flow_key")
            _node = (
                f"{_output_domain}.{_flow_key}_flow"
                if _output_domain and _flow_key
                else None
            )
            _p_nodes_upstream = [_node] if _node else None
            _saved_env = {
                "flow": os.getenv("flow"),
                "node": os.getenv("node"),
                "sanity_check_executed": os.getenv("sanity_check_executed"),
                "workflow": os.getenv("workflow"),
                "auto_sanity_check_layer": os.getenv("auto_sanity_check_layer"),
                "sanity_check_deferred": os.getenv("sanity_check_deferred"),
            }
            try:
                execute_sanity_check(_source_layer, p_nodes=_p_nodes_upstream)
                logger.info(f"[STEP 1] Sanity check completado para {_source_layer}")
            except RuntimeError as e:
                logger.error(
                    f"[STEP 1] Upstream sanity check FALLIDO para "
                    f"{_source_layer}: {e}"
                )
                raise
            finally:
                for key, original_value in _saved_env.items():
                    if original_value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = original_value

        if input_file:
            for reference in input_file:
                file_info = data_catalog.get(reference)
                if file_info:
                    relative_path = file_info.get("path", "") + file_info.get(
                        "additional_raw_option", ""
                    )
                    relative_path = (
                        relative_path
                        if data_subfolder in ["data_cleaning", "raw_data"]
                        else prefix_dict[relative_path.split("_")[0]]
                        + "/"
                        + relative_path
                    )

                    base_path = (
                        globals.get(extended_path, "").replace("$env", container)
                        + additional_path
                        + input_data
                    )

                    full_path = f"{base_path}/{relative_path}"

                    if is_sandbox:
                        fallback_base_path = (
                            globals.get(extended_path, "").replace("$env", "devde")
                            + "dev"
                            + input_data
                        )
                        fallback_full_path = f"{fallback_base_path}/{relative_path}"

                    file_extension = file_info.get("file_extension", "")

                    partition_col = file_info.get("partition_col", "")
                    input_partition_col = file_info.get("input_partition_col", "")

                    df = load_data_source_task(
                        full_path,
                        file_extension,
                        is_sandbox
                        and data_subfolder not in ["data_cleaning", "raw_data"],
                    )

                    if (
                        (not df)
                        and is_sandbox
                        and data_subfolder
                        not in [
                            "data_cleaning",
                            "raw_data",
                        ]
                    ):
                        df = load_data_source_task(fallback_full_path, file_extension)

                    df = validate_date_range_filter_task(
                        df,
                        partition_col,
                        input_partition_col,
                        os.getenv("start_dt"),
                        os.getenv("end_dt"),
                    )

                    path = file_info.get("path", "")
                    dataset_name = path.split("/")[-1] if path else reference
                    start_date = os.getenv("start_dt")
                    end_date = os.getenv("end_dt")
                    data_layer = os.getenv("folder")

                    if (
                        start_date
                        and end_date
                        and data_layer
                        and os.getenv("auto_sanity_check_enabled") == "1"
                    ):
                        from data_engineering.core.sanity_check import sanity_check_flow
                        from data_engineering.core.sanity_check.engine import LayerUtils

                        source_layer = LayerUtils.LAYER_DEPENDENCIES.get(data_layer)

                        if source_layer:
                            try:
                                sanity_check_flow.check_quality_gate_task(
                                    dataset_name,
                                    start_date,
                                    end_date,
                                    source_layer,
                                )
                            except RuntimeError as e:
                                logger.error(f"QUALITY_GATE_FAILURE: {str(e)}")
                                raise

                    dataframes[reference] = df

        return dataframes

    except Exception as e:
        logger.error(f"An error occurred while loading data source: {e}")
        raise
