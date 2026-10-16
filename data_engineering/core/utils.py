from __future__ import annotations

import importlib
import json
import os
import sys
import traceback
from typing import Tuple

import requests  # type: ignore
import yaml  # type: ignore
from databricks.connect import DatabricksSession  # type: ignore
from prefect import task  # type: ignore
from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql import functions as f  # type: ignore
from pyspark.sql.types import DecimalType, DoubleType, FloatType  # type: ignore


def load_yaml_file(p_file_path: str) -> dict:
    """
    Loads a YAML file.

    This function reads a YAML file from the specified path  using yaml.safe_load,
    which ensures that only safe YAML is loaded. This is useful for configuration files.

    Args:
        p_file_path (str): The path to the YAML file that should be loaded.

    Returns:
        dict: A dictionary containing the data loaded from the YAML file.

    Raises:
        Exception: An error occurred while trying to load the YAML file. The specific
        error message is logged.
    """
    try:
        yaml_file_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "", p_file_path
        )
        with open(yaml_file_path, "r") as file:
            data = yaml.safe_load(file)
            print(f"YAML file loaded: {yaml_file_path}")
            return data
    except Exception as e:
        print(f"An error occurred while trying to load YAML file {yaml_file_path}: {e}")
        raise


def send_message_teams(p_messages):
    """Funtion send notification to Teams.

    Args:
        p_messages: Message to send.

    Returns:
        _type_: Menssage in text.
    """
    root_dir = os.path.dirname(os.path.dirname(__file__))
    settings = load_yaml_file(os.path.join(root_dir, "env/base/global_settings.yml"))

    webhook = settings.get("webhook")
    teams_webhook = webhook
    message = {"text": p_messages}
    response = requests.post(
        teams_webhook,
        headers={"Content-Type": "application/json"},
        data=json.dumps(message),
    )
    return response.text


def import_flow_module(p_flow: str):
    """
    Import a flow module and return it.

    Args:
        p_flow (str): Name of the flow to import.

    Returns:
        module: The imported module.

    Raises:
        ModuleNotFoundError: If the module is not found.
    """
    print(f"Importing flow: {p_flow}")
    try:
        module = importlib.import_module(p_flow)
        print(f"Module imported successfully: {module}")
        return module
    except ModuleNotFoundError as e:
        print(f"Error importing module: {e}")
        raise


def execute_flow(
    p_module,
    p_flow_name,
    p_flow,
):
    """
    Executes a specified flow within a given module.

    Args:
        p_module: The module containing the flow to be executed.
        p_flow_name: The name of the flow to execute.
        p_flow: The type of flow, used to determine specific execution paths.
    """
    if hasattr(p_module, p_flow_name):
        flow_to_run = getattr(p_module, p_flow_name)
        os.environ["flow"] = p_flow
        flow_to_run()
    else:
        print(
            f"Error: Flow name '{p_flow_name}' not found in module "
            f"'{p_module.__name__}'"
        )


def _default_is_quality_gate_error(error_msg: str) -> bool:
    """Default stub for quality gate error checking when sanity_check is disabled."""
    return False


is_quality_gate_error = _default_is_quality_gate_error


def execute_flows(p_flows, p_flow, p_sanity_check=False, p_nodes=None, workflow=None):
    """
    Run flows with automatic upstream sanity_checks as preconditions.

    Execution Flow (P0: Resilient Node Isolation):
    1. Execute upstream sanity_checks for the workflow (based on dependency chain)
    2. Execute main flows for the workflow (with granular error handling per node)
    3. Execute sanity_check for the current workflow

    Args:
        p_flows (list): List of flow names to run.
        p_flow (str): The type of flow.
        p_sanity_check (bool): If True, execute sanity_check automatically
        p_nodes (list): List of nodes being executed (for isolated mode detection).
        workflow (str): The workflow name

    Returns:
        dict: Execution results with:
            - total_nodes: Total nodes to execute
            - successful: Number of successful nodes
            - failed: Number of failed nodes (QG failures)
            - success_rate: Success rate percentage (0-100)
            - nodes: Dict with successful and failed node lists
            - details: Extended failure details

    Raises:
        RuntimeError: If all nodes fail or critical system error occurs.
    """
    if p_sanity_check:
        from data_engineering.core.sanity_check.sanity_check_flow import (
            is_quality_gate_error as _is_quality_gate_error_real,
        )
        from data_engineering.core.sanity_check.sanity_check_flow import (
            prepare_flow_sanity_checks,
        )

        globals()["is_quality_gate_error"] = _is_quality_gate_error_real

        prepare_flow_sanity_checks(workflow, p_nodes, p_sanity_check)

    for flow in p_flows:
        try:
            module = import_flow_module(flow)
            flow_name = flow.split(".")[-1]
            os.environ["output_domain"] = flow.split(".")[3]
            os.environ["flow_key"] = flow_name.split("_flow")[0]
            execute_flow(module, flow_name, p_flow)
        except RuntimeError as e:
            error_msg = str(e)
            if p_sanity_check and is_quality_gate_error(error_msg):
                continue
            else:
                raise RuntimeError(f"❌ Sistema error en {flow}: {error_msg}")

        except Exception as e:
            tb = traceback.extract_tb(sys.exc_info()[2])[-1]
            line_number = tb.lineno
            file_path = tb.filename
            env_flow = os.getenv("env")
            if env_flow in ("dev", "prod"):
                error_message = (
                    f"❗️ PROCESS FAILED\n\n"
                    f"_____ \n\n"
                    f"🚀 --env: {env_flow}\n\n"
                    f"🧩 --workflow: {workflow}\n\n"
                    f"📦 --flow: {p_flow}\n\n"
                    f"🔄 --nodes: {flow_name}\n\n"
                    f"🗂️ : {file_path} line: {line_number}\n\n"
                    f"❌ : {e}"
                )
                send_message_teams(error_message)

            raise RuntimeError(f"Unexpected error en {flow}: {e}")

        os.environ.pop("sanity_check_deferred", None)


def validate_folders(p_base_module: str, p_folder: str):
    """
    Checks if the specified folder exists.

    Args:
       p_base_module (str): The path of the folder to check.
       p_folder (str): folder to check

    Returns:
       bool: True if the folder exists, False otherwise.
    """
    try:
        elements = os.listdir(p_base_module)
        folder_check = True if p_folder in elements else False
        return folder_check
    except ModuleNotFoundError as e:
        print(f"Error: {e}")
        raise


def arrange_columns(p_start_cols):
    """
    Order the initial columns of the DataFrame

    Args:
       p_start_cols (List): Columns to order

    Returns:
       df: DatraFrame with ordered columns
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            df_to_validate = func(*args, **kwargs)
            start_columns = [
                col for col in p_start_cols if col in df_to_validate.columns
            ]
            end_columns = [
                col for col in df_to_validate.columns if col not in p_start_cols
            ]
            return df_to_validate[start_columns + end_columns]

        return wrapper

    return decorator


@task(name="add_prefix_to_columns_task")
def add_prefix_to_columns_task(
    p_data: DataFrame,
    p_prefix: str,
    p_exclude_columns=[
        "id_cliente",
        "cuenta_corporativa",
        "descripcion_tipo_cuenta",
        "_observ_end_dt",
        "_observ_start_dt",
    ],
):
    """
    Function that renames all column names of a given dataframe based on
    `p_prefix` as a prefix with the exception of the column names in
    `p_exclude columns`.


    Args:
        p_data (DataFrame): DataFrame containing the data to proccess.
        p_prefix (str): Prefix to be concatenated with underscore in
        `p_data` columns.
        p_exclude_columns (list): Names of the columns that will not
        change.

    Returns:
        DataFrame: DataFrame with the renamed columns.
    """
    all_columns = p_data.columns
    renamed_columns = [
        f.col(c).alias(f"{p_prefix}_{c}") if c not in p_exclude_columns else f.col(c)
        for c in all_columns
    ]
    return p_data.select(*renamed_columns)


@task(name="create_year_month_period_task")
def create_year_month_period_task(
    p_dataframe: DataFrame, p_date_column_name: str
) -> DataFrame:
    """
    Creates a new column with the year and month period.

    This task calculates the year and the month of `p_date_column_name` column
    and adds it into `p_dataframe` with the name `_particion`.
    Args:
        p_dataframe (DataFrame): The DataFrame in which we want to add the year month
        column.
        p_date_column_name (str): The column name of the the date that will be used
        to calculate the year month period

    Returns:
        DataFrame: DataFrame with the `_particion` column.
    """
    return p_dataframe.withColumn(
        "_particion", f.date_format(p_date_column_name, "yyyyMM").cast("int")
    )


@task(name="rename_val_columns_task")
def rename_val_columns_task(p_df: DataFrame) -> DataFrame:
    """
    Renames all float or decimal columns in the DataFrame by appending
    `_val` as a suffix to their names.

    Parameters:
    p_df (DataFrame): Input DataFrame to be processed.

    Returns:
    DataFrame: DataFrame with renamed float or decimal columns.
    """
    float_cols = [
        f.name
        for f in p_df.schema.fields
        if isinstance(f.dataType, (FloatType, DoubleType, DecimalType))
    ]

    p_df = p_df.select(
        *[
            (
                f.round(f.col(c), 2)
                .cast(DecimalType(precision=19, scale=2))
                .alias(f"{c}_val")
                if c in float_cols
                else f.col(c)
            )
            for c in p_df.columns
        ]
    )

    return p_df


def _split_table_identifier(table: str) -> Tuple[str, str, str]:
    """Split a fully-qualified table name into catalog, schema, and table.

    Args:
        table: Fully qualified table identifier (catalog.schema.table or similar).

    Returns:
        Tuple[str, str, str]: Tuple with catalog, schema, and table names.
    """
    spark = DatabricksSession.builder.getOrCreate()
    parts = table.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return spark.catalog.currentCatalog(), parts[0], parts[1]
    if len(parts) == 1:
        return (
            spark.catalog.currentCatalog(),
            spark.catalog.currentDatabase(),
            parts[0],
        )
    raise ValueError(f"Unsupported table identifier: {table}")


def convert_df_to_list(p_df, p_cond):
    """Convert DataFrame to list

    Args:
       p_df (DataFrame): Dataframe to convert.
       p_cond (str): Condition to filter DataFrame.

    Returns:
        list(list): List to return.
    """
    list = [
        row["tabla"]
        for row in p_df.filter(f.col("tipo_tabla") == p_cond)
        .select(f.col("tabla"))
        .collect()
    ]
    return list
