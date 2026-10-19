import logging
import os
from typing import Any, Dict, List

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

RESULTS_PARTITION_COL = "_observ_end_dt"
_TABLE_COMMENT = "Resultados de la comprobación de validación de datos"
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


def _env() -> str:
    """Environment of the results table (the `env` variable, dev by default)."""
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
    """Storage path of the results table.

    The same path the tables created by earlier versions use:
    <company_path with $env replaced><env>/ (de/<env>/ in sandbox)
    <layer>/sanity_check/<table>.
    """
    env = _env()
    if env in ("prod", "dev"):
        settings_env, container, folder = env, f"{env}de", env
    else:
        settings_env, container, folder = "dev", "sandbox", f"de/{env}"

    settings = load_yaml_or_default(
        config_path("env", settings_env, "global_settings.yml"), {}
    )
    company_path = settings.get("company_path", "") if isinstance(settings, dict) else ""
    if not company_path:
        raise ValueError(
            f"company_path is not configured in env/{settings_env}/global_settings.yml"
        )
    return (
        f"{company_path.replace('$env', container)}{folder}/"
        f"{p_layer}/sanity_check/{LayerUtils._QUALITY_TABLE_NAME}"
    )


def _replace_condition(p_rows: List[Dict[str, Any]]) -> str:
    """replaceWhere predicate covering the (dataset, month) pairs being written.

    One `nombre_dataset = ... AND _observ_end_dt IN (...)` term per dataset
    instead of one OR term per pair: a 10-year rerun stays a short predicate,
    and the IN list on the partition column prunes partitions. Pairs not in
    p_rows are not touched.
    """

    def _quote(p_value: Any) -> str:
        return "'" + str(p_value).replace("'", "''") + "'"

    months_by_dataset: Dict[str, set] = {}
    for row in p_rows:
        months_by_dataset.setdefault(row["nombre_dataset"], set()).add(
            row["_observ_end_dt"]
        )
    return " OR ".join(
        f"(nombre_dataset = {_quote(dataset)} AND _observ_end_dt IN "
        f"({', '.join(_quote(m) for m in sorted(months))}))"
        for dataset, months in sorted(months_by_dataset.items())
    )


def _sql_string(p_value: str) -> str:
    """Single-quoted SQL string literal."""
    return "'" + str(p_value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _create_results_table(p_table: str, p_layer: str) -> None:
    """Create the results table as an EXTERNAL Delta table.

    Raises:
        RuntimeError: If the layer schema does not exist (it is never created).
    """
    location = results_location(p_layer)
    columns = ",\n    ".join(
        f"{name} STRING COMMENT {_sql_string(comment)}"
        for name, comment in _COLUMN_COMMENTS.items()
    )
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {p_table} (\n    {columns}\n)\n"
        f"USING DELTA\n"
        f"PARTITIONED BY ({RESULTS_PARTITION_COL})\n"
        f"LOCATION {_sql_string(location)}\n"
        f"COMMENT {_sql_string(_TABLE_COMMENT)}"
    )
    try:
        LayerUtils._with_session_retry(lambda: get_spark().sql(ddl), p_table)
    except Exception as e:
        if "schema_not_found" not in str(e).lower():
            raise
        raise RuntimeError(
            f"El esquema {p_table.rsplit('.', 1)[0]} no existe; créelo antes de "
            f"ejecutar el sanity check (los resultados no se guardaron)."
        ) from e
    logger.info(f"Created external sanity check results table {p_table} at {location}")


def ensure_results_table(p_layer: str) -> str:
    """Return the results table, creating it as an EXTERNAL table if missing."""
    table = results_table(p_layer)
    if table in LayerUtils._KNOWN_QUALITY_TABLES:
        return table

    if not LayerUtils._with_session_retry(
        lambda: get_spark().catalog.tableExists(table), table
    ):
        _create_results_table(table, p_layer)
    LayerUtils._remember_quality_table(table)
    return table


def write_results(p_records: List[ValidationRecord], p_layer: str) -> None:
    """Persist records, replacing their (dataset, month) pairs in one commit."""
    rows = result_rows(p_records)
    if not rows:
        return

    table = ensure_results_table(p_layer)
    condition = _replace_condition(rows)

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
