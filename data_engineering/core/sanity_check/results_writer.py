"""Direct persistence of sanity check results.

Each write replaces the (nombre_dataset, _observ_end_dt) pairs being written
with `replaceWhere`, in the same Delta commit as the new rows: one job and one
atomic commit, instead of the save_data_flow path (sub-flow, configuration
files, catalog commands, a DELETE and an append in two commits).

The table is created here, when missing, as an EXTERNAL Delta table at the
same location save_data_flow used. The layer schema is never created: if it
does not exist the write fails with a clear error.

Set the environment variable `sanity_check_direct_results_write=0` to persist
through save_data_flow instead.
"""
import logging
import os
from typing import List

from data_engineering.core.sanity_check.base import (
    VALIDATION_RESULT_SCHEMA,
    ValidationRecord,
)
from data_engineering.core.sanity_check.engine import LayerUtils, get_spark
from data_engineering.core.sanity_check.utils import (
    config_path,
    get_catalog_name,
    load_yaml_or_default,
    result_rows,
    timed_phase,
)

logger = logging.getLogger(__name__)

ENV_DIRECT_RESULTS_WRITE = "sanity_check_direct_results_write"
RESULTS_PARTITION_COL = "_observ_end_dt"
_DEFAULT_TABLE_COMMENT = "Resultados de la comprobación de validación de datos"
# Columns of VALIDATION_RESULT_SCHEMA, in its order, with their comments.
_COLUMN_COMMENTS = {
    "nombre_dataset": "Dataset validado",
    "columna": "Columna o check evaluado",
    "validacion": "Tipo de validación (NULL_VALUES, DUPLICATES, ...)",
    "descripcion": "Detalle del resultado",
    "severidad": "LOW, MEDIUM, HIGH o CRITICAL",
    "estado": "PASS, FAIL, WARN o SKIPPED",
    "_observ_start_dt": "Primer día del mes observado",
    "_observ_end_dt": "Último día del mes observado (partición)",
    "fecha_ejecucion": "Fecha y hora de la validación (UTC-6)",
}

def direct_write_enabled() -> bool:
    """Whether results are written here (default) or through save_data_flow."""
    value = os.getenv(ENV_DIRECT_RESULTS_WRITE, "1")
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _env() -> str:
    """Environment as save_data_flow reads it for the results write."""
    return os.getenv("env", "dev")


def results_table(p_layer: str) -> str:
    """Fully qualified results table: <catalog>.<env>_<layer>.sc_sanity_check_results.

    It is the table the quality gate reads.

    Raises:
        ValueError: If a part is not a safe SQL identifier.
    """
    catalog = get_catalog_name()
    schema = f"{_env()}_{p_layer}"
    for value, label in ((catalog, "catalog"), (schema, "schema")):
        if not LayerUtils._validate_sql_identifier(value, label):
            raise ValueError(f"Unsafe {label} for the sanity check results: '{value}'")
    return f"{catalog}.{schema}.{LayerUtils._QUALITY_TABLE_NAME}"


def results_location(p_layer: str) -> str:
    """Storage path of the results table, as save_data_flow builds it.

    <company_path of env/{prod|dev}/global_settings.yml with $env replaced>
    + <env>/ (or de/<env>/ in sandbox) + <layer>/sanity_check/<table>.
    """
    env = _env()
    if env in ("prod", "dev"):
        settings_env, container, container_folder = env, f"{env}de", f"{env}/"
    else:
        settings_env, container, container_folder = "dev", "sandbox", f"de/{env}/"

    settings = load_yaml_or_default(
        config_path("env", settings_env, "global_settings.yml"), {}
    )
    company_path = settings.get("company_path", "") if isinstance(settings, dict) else ""
    if not company_path:
        raise ValueError(
            f"company_path is not configured in env/{settings_env}/global_settings.yml"
        )
    return (
        company_path.replace("$env", container)
        + container_folder
        + "/".join((p_layer, "sanity_check", LayerUtils._QUALITY_TABLE_NAME))
    )


def _table_comment(p_layer: str) -> str:
    """description of `sanity_check_results` in the layer's io_config.yml."""
    io_config = load_yaml_or_default(config_path(p_layer, "conf", "io_config.yml"), {})
    entry = io_config.get("sanity_check_results") if isinstance(io_config, dict) else None
    description = entry.get("description") if isinstance(entry, dict) else None
    return description or _DEFAULT_TABLE_COMMENT


def _sql_string(p_value: str) -> str:
    """Single-quoted SQL string literal."""
    return "'" + str(p_value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def ensure_results_table(p_layer: str) -> str:
    """Return the results table, creating it as an EXTERNAL table if missing.

    Raises:
        RuntimeError: If the layer schema does not exist (it is never created).
    """
    table = results_table(p_layer)
    if table in LayerUtils._KNOWN_QUALITY_TABLES:
        return table

    exists = LayerUtils._with_session_retry(
        lambda: get_spark().catalog.tableExists(table), table
    )
    if not exists:
        location = results_location(p_layer)
        columns = ",\n    ".join(
            f"{name} STRING COMMENT {_sql_string(comment)}"
            for name, comment in _COLUMN_COMMENTS.items()
        )
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {table} (\n    {columns}\n)\n"
            f"USING DELTA\n"
            f"PARTITIONED BY ({RESULTS_PARTITION_COL})\n"
            f"LOCATION {_sql_string(location)}\n"
            f"COMMENT {_sql_string(_table_comment(p_layer))}"
        )
        try:
            LayerUtils._with_session_retry(lambda: get_spark().sql(ddl), table)
        except Exception as e:
            if "schema_not_found" in str(e).lower():
                schema = table.rsplit(".", 1)[0]
                raise RuntimeError(
                    f"El esquema {schema} no existe; créelo antes de ejecutar el "
                    f"sanity check (los resultados no se guardaron)."
                ) from e
            raise
        logger.info(f"Created external sanity check results table {table} at {location}")

    LayerUtils._remember_quality_table(table)
    return table


def write_results(p_records: List[ValidationRecord], p_layer: str) -> None:
    """Persist records, replacing their (dataset, month) pairs in one commit."""
    rows = result_rows(p_records)
    if not rows:
        return

    from data_engineering.core.save_data_flow import _build_merge_delete_condition

    table = ensure_results_table(p_layer)
    condition = _build_merge_delete_condition(rows)

    def _write() -> None:
        (
            get_spark()
            .createDataFrame(rows, schema=VALIDATION_RESULT_SCHEMA)
            .write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", condition)
            .option("mergeSchema", "true")
            .saveAsTable(table)
        )

    with timed_phase(f"persist {len(rows)} result(s) → {table}"):
        LayerUtils._with_session_retry(_write, table)
