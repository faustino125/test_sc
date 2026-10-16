import os

from databricks.connect import DatabricksSession
from prefect import get_run_logger, task

from data_engineering.core.utils import load_yaml_file

spark = DatabricksSession.builder.getOrCreate()


@task(name="document_table", tags=["document", "table"])
def document_table(
    p_db_table, p_field_description, p_table_description, p_partition_col, p_overwrite
):
    """
    Adds or updates table and column comments in Hive or Unity Catalog based on metadata
    provided.

    Args:
        p_db_table (str): The name of the database and table.
        p_field_description (dict): A dictionary where keys are column names and values.
        p_table_description (str): Description for the table.
        p_overwrite (bool): Whether to overwrite existing descriptions.

    Returns:
        None
    """
    logger = get_run_logger()
    if p_table_description:
        logger.info(f"updating table description for '{p_db_table}'")
        spark.sql(
            f"""COMMENT ON TABLE {p_db_table}
                  IS '{p_table_description}. Fecha base: {p_partition_col}'"""
        )

    describe = spark.sql(f"DESCRIBE {p_db_table}")
    for row in describe.collect():
        col_name = row.col_name
        current_comment = row.comment

        if (
            col_name
            and not col_name.startswith("#")
            and col_name in p_field_description
        ):
            new_comment = p_field_description[col_name]

            if (
                p_overwrite
                or current_comment in [None, "", "NULL"]
                or current_comment != new_comment
            ):
                logger.info(
                    f"updating comment for column '{col_name}' in table '{p_db_table}'"
                )
                spark.sql(
                    f"""
                           ALTER TABLE {p_db_table}
                           ALTER COLUMN {col_name}
                           COMMENT '{new_comment}'
                           """
                )


@task(name="process_product_schemas", tags=["yaml", "schema"])
def process_product_schemas(
    p_product_folder_path, p_db_table, p_table_description, p_partition_col, p_overwrite
):
    """
    Processes all YAML files in the specified folder and applies table and
    column documentation to Hive or Unity Catalog.

    Args:
        p_product_folder_path (str): The full path to the folder.
        p_db_table (str): The name of the database and table.
        p_table_description (str): Description for the table.
        p_partition_col (str): The column by which the data should be partitioned.
        p_overwrite (bool): Whether to overwrite existing descriptions.

    Returns:
        None
    """
    schema_files = [
        file
        for file in os.listdir(p_product_folder_path)
        if file == os.getenv("flow_key") + "_schema" + ".yml"
    ]

    for schema_file in schema_files:
        schema_path = os.path.join(p_product_folder_path, schema_file)
        doc = load_yaml_file(schema_path)

        field_descriptions = doc.get("fields", {})

        document_table(
            p_db_table,
            field_descriptions,
            p_table_description,
            p_partition_col,
            p_overwrite,
        )
