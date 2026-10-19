import logging
import os
import re
import threading
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

from databricks.connect import DatabricksSession  # type: ignore
from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql import functions as f  # type: ignore

from data_engineering.core.sanity_check.base import BaseValidator, find_column
from data_engineering.core.sanity_check.standard_validators import (
    StandardConsistencyValidator,
    StandardDimensionalValidator,
    StandardDuplicatesValidator,
    StandardNullValuesValidator,
)
from data_engineering.core.sanity_check.utils import (
    _TIMEZONE_UTC_6,
    GLOBAL_SETTINGS_FILE,
    PREFIXES_FILE,
    config_path,
    get_catalog_name,
    get_environment,
    load_yaml_file,
)
from data_engineering.core.utils import send_message_teams

logger = logging.getLogger(__name__)

_SPARK_SESSION: Optional[DatabricksSession] = None
_SPARK_SESSION_LOCK: threading.Lock = threading.Lock()


def get_spark() -> DatabricksSession:
    """Get or create a Databricks session (singleton pattern, thread-safe).

    Returns:
        Active or newly created DatabricksSession instance.
    """
    global _SPARK_SESSION

    if _SPARK_SESSION is not None:
        return _SPARK_SESSION

    with _SPARK_SESSION_LOCK:
        if _SPARK_SESSION is None:
            _SPARK_SESSION = DatabricksSession.builder.getOrCreate()
        return _SPARK_SESSION


def reset_spark() -> None:
    """Open a new session for the sanity check after a session error.
    """
    global _SPARK_SESSION

    with _SPARK_SESSION_LOCK:
        old_session = _SPARK_SESSION
        _SPARK_SESSION = None

    try:
        new_session = DatabricksSession.builder.create()
    except Exception as e:
        logger.warning(f"Could not create a new Spark session ({e}); stopping the old one")
        if old_session is not None:
            try:
                old_session.stop()
            except Exception as stop_error:
                logger.debug(f"Error stopping stale Spark session: {stop_error}")
        return

    with _SPARK_SESSION_LOCK:
        _SPARK_SESSION = new_session

STANDARD_VALIDATORS: Dict[str, Type[BaseValidator]] = {
    "duplicates": StandardDuplicatesValidator,
    "null_values": StandardNullValuesValidator,
    "dimensional": StandardDimensionalValidator,
    "consistency": StandardConsistencyValidator,
}


def build_validators(p_config: Dict[str, Any]) -> List[BaseValidator]:
    """Instantiate the enabled standard validators of a dataset config.

    An enabled type that is not a standard validator is reported and skipped.
    """
    enabled = [
        (key, value)
        for key, value in p_config.items()
        if isinstance(value, dict) and value.get("enabled")
    ]
    validators: List[BaseValidator] = []
    for validator_type, validator_config in enabled:
        validator_class = STANDARD_VALIDATORS.get(validator_type)
        if validator_class is None:
            logger.warning(
                f"Validator type '{validator_type}' is enabled but not registered. "
                f"Available: {sorted(STANDARD_VALIDATORS)}"
            )
            continue
        validators.append(validator_class(validator_config, get_spark()))
    return validators


def stop_totals(p_stop_summaries: List[Dict[str, Any]]) -> Tuple[int, int]:
    """Total (critical failures, validator errors) of a list of month summaries."""
    return (
        sum(s.get("critical_failures", 0) for s in p_stop_summaries),
        sum(s.get("validator_errors", 0) for s in p_stop_summaries),
    )


class LayerUtils:
    """Utility functions for layer-specific sanity check operations."""

    LAYER_DEPENDENCIES = {
        "raw_data": None,
        "data_cleaning": "raw_data",
        "data_transformation": "data_cleaning",
        "data_integration": "data_transformation",
    }

    @staticmethod
    def get_dataset_config(
        p_config_all: Dict[str, Any], p_dataset_name: str
    ) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific dataset.

        Args:
            p_config_all: Dictionary containing all dataset configurations.
            p_dataset_name: Name of the dataset to retrieve configuration for.

        Returns:
            Configuration dictionary for the specified dataset.
        """
        if not p_config_all:
            return None
        if p_dataset_name in p_config_all:
            return p_config_all[p_dataset_name]

        lower_map = {k.lower(): v for k, v in p_config_all.items()}
        name = p_dataset_name.lower()

        if name in lower_map:
            return lower_map[name]

        matches = [
            (key_lower, val)
            for key_lower, val in lower_map.items()
            if (
                name.endswith("_" + key_lower)
                or key_lower.endswith("_" + name)
                or key_lower.startswith(name + "_")
                or name.startswith(key_lower + "_")
            )
        ]

        if len(matches) == 1:
            logger.warning(
                f"No exact config for '{p_dataset_name}'; applying the partial "
                f"match '{matches[0][0]}'. Rename the config key to the dataset "
                f"name to make this explicit."
            )
            return matches[0][1]
        if len(matches) > 1:
            logger.warning(
                f"Ambiguous config match for '{p_dataset_name}': "
                f"{[m[0] for m in matches]}. No config applied."
            )

        return None

    @staticmethod
    def _resolve_date_col(
        p_config: Dict[str, Any], p_metadata: Dict[str, Any]
    ) -> Optional[str]:
        """Determine the date column to use for filtering based on config and metadata.

        Args:
            p_config: Dataset quality configuration.
            p_metadata: Dataset metadata.

        Returns:
            The name of the date column to use for filtering, or None if not found.
        """
        return (
            p_config.get("date_col")
            or p_config.get("partition_col")
            or p_metadata.get("date_col")
        )

    @staticmethod
    def _apply_date_filter(
        p_df: DataFrame,
        p_date_col: Optional[str],
        p_start_dt: str,
        p_end_dt: str,
        p_source_name: str,
        p_schema: Any = None,
    ) -> DataFrame:
        """Apply date filtering to a DataFrame.

        Args:
            p_df: Input DataFrame to filter
            p_date_col: Name of date column (case-insensitive); None means no filtering
            p_start_dt: Start date boundary (string format)
            p_end_dt: End date boundary (string format)
            p_source_name: Name of source for logging
            p_schema: p_df's schema if already known (saves a server round trip)

        Returns:
            Filtered DataFrame between specified date boundaries
        """
        if not p_date_col:
            logger.warning(
                f"No date column for {p_source_name}. Validating ALL records."
            )
            return p_df

        schema = p_schema
        if schema is None:
            try:
                schema = p_df.schema
            except Exception:
                schema = None
        columns = schema.names if schema is not None else p_df.columns
        actual_col = find_column(columns, p_date_col)
        if not actual_col:
            logger.warning(f"Date column '{p_date_col}' not in {p_source_name}")
            return p_df

        kind = LayerUtils._column_kind(schema, actual_col)
        return p_df.filter(
            LayerUtils._date_range_condition(kind, actual_col, p_start_dt, p_end_dt)
        )

    @staticmethod
    def _column_kind(p_schema: Any, p_actual_col: str) -> str:
        """Simple type name of a column ('date', 'string', ...) or '' if unknown."""
        try:
            return p_schema[p_actual_col].dataType.simpleString()
        except Exception:
            return ""

    @staticmethod
    def _date_range_condition(
        p_kind: str, p_actual_col: str, p_start_dt: str, p_end_dt: str
    ):
        """Build an inclusive date-range condition that keeps partition pruning.

        Args:
            p_kind: Column type from _column_kind
            p_actual_col: Actual (resolved) column name
            p_start_dt: First day included (YYYY-MM-DD)
            p_end_dt: Last day included (YYYY-MM-DD)

        Returns:
            Boolean Column expression.
        """
        col = f.col(p_actual_col)
        start_date = f.to_date(f.lit(p_start_dt))
        end_date = f.to_date(f.lit(p_end_dt))

        if p_kind == "date":
            return col.between(start_date, end_date)
        if p_kind.startswith("timestamp"):
            return (col >= start_date.cast(p_kind)) & (
                col < f.date_add(end_date, 1).cast(p_kind)
            )
        condition = f.to_date(col).between(start_date, end_date)
        if p_kind == "string" and LayerUtils._string_date_pushdown():
            try:
                day_after_end = date.fromisoformat(p_end_dt) + timedelta(days=1)
            except ValueError:
                return condition
            return (
                (col >= f.lit(p_start_dt))
                & (col < f.lit(day_after_end.isoformat()))
                & condition
            )
        return condition

    @staticmethod
    def _string_date_pushdown() -> bool:
        """Whether STRING date columns get the pushable text range (default on)."""
        value = os.getenv("sanity_check_string_date_pushdown", "1")
        return value.strip().lower() not in {"0", "false", "no", "off"}

    _MISSING_TABLE_PHRASES = (
        "table_or_view_not_found",
        "delta_table_not_found",
        "table or view not found",
    )

    _SESSION_ERROR_PHRASES = (
        "session not found",
        "invalid_handle",
        "incorrect server side session identifier",
        "session invalid",
    )

    @staticmethod
    def _is_missing_table_error(p_error_msg: str) -> bool:
        """Return True if an error message indicates a missing table/view.

        Args:
            p_error_msg: Error message to classify (any casing).

        Returns:
            True if the error denotes a non-existent table/view, False otherwise.
        """
        error_msg = (p_error_msg or "").lower()
        return any(phrase in error_msg for phrase in LayerUtils._MISSING_TABLE_PHRASES)

    @staticmethod
    def _is_session_error(p_error_msg: str) -> bool:
        """Return True if an error message indicates a dead Spark Connect session."""
        error_msg = (p_error_msg or "").lower()
        return any(phrase in error_msg for phrase in LayerUtils._SESSION_ERROR_PHRASES)

    @staticmethod
    def _with_session_retry(p_action: Callable[[], Any], p_label: str) -> Any:
        """Run a Spark action; if the session was lost, reopen it and retry once.

        Raises:
            Exception: The error of the last attempt.
        """
        for attempt in (1, 2):
            try:
                return p_action()
            except Exception as e:
                if attempt == 1 and LayerUtils._is_session_error(str(e)):
                    logger.warning(
                        f"Session error for {p_label}, reopening session and retrying"
                    )
                    reset_spark()
                    continue
                logger.error(f"Spark operation failed for {p_label}: {e}")
                raise

    _ALL_MONTH_FOLDERS = "/*/*/*"
    _MISSING_PATH_PHRASES = (
        "path_not_found",
        "path does not exist",
        "unable_to_infer_schema",
    )

    @staticmethod
    def _month_folders_path(
        p_path: str, p_start_dt: Optional[str], p_end_dt: Optional[str]
    ) -> Optional[str]:
        """Path reading only the yyyy/mm folders of the range, or None.
        """
        enabled = os.getenv("sanity_check_raw_month_folders", "1").strip().lower()
        if (
            enabled in {"0", "false", "no", "off"}
            or not (p_start_dt and p_end_dt)
            or not p_path.endswith(LayerUtils._ALL_MONTH_FOLDERS)
        ):
            return None
        try:
            months = LayerUtils._resolve_month_ranges(p_start_dt, p_end_dt)
        except ValueError:
            return None
        folders = [f"{start[:4]}/{start[5:7]}" for start, _ in months]
        inner = folders[0] if len(folders) == 1 else "{" + ",".join(folders) + "}"
        return f"{p_path[: -len(LayerUtils._ALL_MONTH_FOLDERS)]}/{inner}/*"

    @staticmethod
    def load_dataset_with_columns(
        p_metadata: Dict[str, Any],
        p_config: Dict[str, Any],
        p_start_dt: Optional[str] = None,
        p_end_dt: Optional[str] = None,
    ) -> Tuple[DataFrame, List[str]]:
        """Load a dataset (Parquet or Delta/SQL) with optional date filtering.

        Args:
            p_metadata: Dataset metadata (dataset_name, path or database, date_col)
            p_config: Dataset quality configuration
            p_start_dt: Start date for filtering; skipped when None
            p_end_dt: End date for filtering; skipped when None

        Returns:
            Tuple of (filtered DataFrame, possibly empty; its columns).

        Raises:
            ValueError: If the metadata has neither a path nor a table.
            Exception: Any error raised while reading the source.
        """
        label = p_metadata.get("dataset_name") or p_metadata.get("path") or "unknown"
        date_col = LayerUtils._resolve_date_col(p_config or {}, p_metadata)
        path = p_metadata.get("path")

        if path:
            month_path = LayerUtils._month_folders_path(path, p_start_dt, p_end_dt)

            def _read_path(p_path: str) -> Tuple[DataFrame, Any]:
                try:
                    df = get_spark().read.format("parquet").load(p_path)
                except Exception as e:
                    if "JVM_ATTRIBUTE_NOT_SUPPORTED" not in str(e):
                        raise
                    df = get_spark().read.parquet(p_path)
                return df, df.schema

            def _read() -> Tuple[DataFrame, Any]:
                if month_path:
                    try:
                        result = _read_path(month_path)
                        logger.info(f"{label}: reading month folders {month_path}")
                        return result
                    except Exception as e:
                        message = str(e).lower()
                        if not any(
                            p in message for p in LayerUtils._MISSING_PATH_PHRASES
                        ):
                            raise
                        logger.info(
                            f"{label}: no month folders for the range "
                            f"({month_path}); reading {path}"
                        )
                return _read_path(path)

        else:
            database = p_metadata.get("database")
            dataset_name = p_metadata.get("dataset_name")
            if not (database and dataset_name):
                raise ValueError(
                    f"Invalid metadata for {label}: missing path or "
                    f"database/dataset_name"
                )
            catalog = get_catalog_name()
            if not (
                LayerUtils._validate_sql_identifier(catalog, "catalog")
                and LayerUtils._validate_sql_identifier(database, "database")
                and LayerUtils._validate_sql_identifier(dataset_name, "dataset_name")
            ):
                raise ValueError(f"Unsafe table identifier for {label}")
            query = f"SELECT * FROM {catalog}.{database}.{dataset_name}"

            def _read() -> Tuple[DataFrame, Any]:
                df = get_spark().sql(query)
                return df, df.schema

        # The schema is requested inside the retry (missing tables and dead
        # sessions fail here) and reused by the date filter.
        df, schema = LayerUtils._with_session_retry(_read, label)
        columns = list(schema.names) if schema is not None else list(df.columns)
        if p_start_dt and p_end_dt:
            df = LayerUtils._apply_date_filter(
                df, date_col, p_start_dt, p_end_dt, label, p_schema=schema
            )
        return df, columns

    @staticmethod
    def load_metadata_from_catalog(
        p_catalog_layer: str,
        p_data_layer: str,
    ) -> List[Dict[str, Any]]:
        """Load dataset metadata from a downstream layer's data_catalog.yml.

        Args:
            p_catalog_layer: Layer containing the data catalog.
            p_data_layer: Target data layer name (e.g., 'data_cleaning')

        Returns:
            List of metadata dictionaries or empty list if catalog not found
        """
        catalog_path = config_path(p_catalog_layer, "conf", "data_catalog.yml")

        if not os.path.exists(catalog_path):
            logger.error(f"data_catalog.yml not found: {catalog_path}")
            return []

        catalog = load_yaml_file(catalog_path)
        inputs = catalog.get("Input", {})
        if not inputs:
            logger.warning(f"No 'Input' section in {catalog_path}")
            return []

        entries = [v for v in inputs.values() if isinstance(v, dict)]
        env = get_environment()
        is_parquet = any(
            e.get("file_extension", "parquet") == "parquet" for e in entries
        )

        if not is_parquet:
            database = f"{env}_{p_data_layer}"
            return [
                {
                    "dataset_name": e["path"],
                    "database": database,
                    "date_col": e.get("partition_col") or None,
                }
                for e in entries
                if e.get("path")
            ]

        settings = load_yaml_file(config_path(GLOBAL_SETTINGS_FILE))
        env_path = config_path("env", env, "global_settings.yml")
        if os.path.exists(env_path):
            settings = {**settings, **load_yaml_file(env_path)}
        base_path = settings.get("raw_data_path", "")

        metadata = []
        for entry in entries:
            raw_path = entry.get("path", "").rstrip("/")
            metadata.append(
                {
                    "dataset_name": raw_path.split("/")[-1],
                    "date_col": entry.get("partition_col") or None,
                    "path": base_path
                    + raw_path
                    + entry.get("additional_raw_option", "/*/*/*"),
                }
            )
        return metadata

    @staticmethod
    def _resolve_month_ranges(
        p_start_dt: Optional[str], p_end_dt: Optional[str]
    ) -> List[Tuple[str, str]]:
        """Resolve month ranges based on start and end dates.

        Args:
            p_start_dt: Start date boundary (YYYY-MM-DD)
            p_end_dt: End date boundary (YYYY-MM-DD)

        Returns:
            List of tuples containing start and end dates for each month.

        Raises:
            ValueError: If a date is malformed or start_dt is after end_dt.
        """
        if not (p_start_dt and p_end_dt):
            if p_start_dt or p_end_dt:
                logger.warning(
                    f"Only one of start_dt/end_dt was provided "
                    f"('{p_start_dt}' / '{p_end_dt}'); both are required to use "
                    f"a custom range."
                )
            today = datetime.now(_TIMEZONE_UTC_6)
            first_day = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
            last_day = today.replace(day=1) - timedelta(days=1)
            start = first_day.strftime("%Y-%m-%d")
            end = last_day.strftime("%Y-%m-%d")
            logger.info(
                f"📅 No dates provided, defaulting to previous month: {start} to {end}"
            )
            return [(start, end)]

        start = datetime.strptime(p_start_dt, "%Y-%m-%d").date()
        end = datetime.strptime(p_end_dt, "%Y-%m-%d").date()
        if start > end:
            raise ValueError(
                f"Invalid date range: start_dt '{p_start_dt}' is after "
                f"end_dt '{p_end_dt}'."
            )

        current = start.replace(day=1)
        month_ranges = []

        while current <= end:
            next_month = current.replace(day=28) + timedelta(days=4)
            next_month = next_month.replace(day=1)
            month_end = next_month - timedelta(days=1)
            month_ranges.append(
                (current.strftime("%Y-%m-%d"), month_end.strftime("%Y-%m-%d"))
            )
            current = next_month

        if len(month_ranges) > 1:
            logger.info(
                f"📅 Processing {len(month_ranges)} months: {p_start_dt} to {p_end_dt}"
            )
        return month_ranges

    @staticmethod
    def _extract_columns_for_caching(
        p_config: Dict[str, Any],
        p_date_col: Optional[str],
        p_df_columns: List[str],
    ) -> List[str]:
        """Extract columns needed for sanity checks to optimize memory usage.

        Args:
            p_config: Dataset validator configuration
            p_date_col: Date/partition column for filtering
            p_df_columns: All columns available in the DataFrame

        Returns:
            List of actual column names to keep.
        """
        enabled = {
            k: v
            for k, v in p_config.items()
            if isinstance(v, dict) and v.get("enabled", False)
        }
        unknown_types = set(enabled) - set(STANDARD_VALIDATORS)
        if unknown_types:
            logger.info(
                f"Unknown validator type(s) {sorted(unknown_types)} enabled; "
                f"skipping column pruning."
            )
            return list(p_df_columns)

        columns_by_lower = {col.lower(): col for col in p_df_columns}

        def _map(name: str) -> Optional[str]:
            """Resolve a name case-insensitively, tolerating qualified names."""
            name = name.lower()
            if name in columns_by_lower:
                return columns_by_lower[name]
            if "." in name:
                return columns_by_lower.get(name.rsplit(".", 1)[-1])
            return None

        names: List[str] = []
        rule_tokens: List[str] = []
        for validator_type, cfg in enabled.items():
            cfg_names, cfg_tokens = STANDARD_VALIDATORS[validator_type].config_columns(cfg)
            names += cfg_names
            rule_tokens += cfg_tokens

        columns_to_cache = {col for col in map(_map, names) if col}
        unmapped_identifiers = set()
        for token in rule_tokens:
            actual_col = _map(token)
            if actual_col:
                columns_to_cache.add(actual_col)
            else:
                unmapped_identifiers.add(token)

        if p_date_col and p_date_col.lower() in columns_by_lower:
            columns_to_cache.add(columns_by_lower[p_date_col.lower()])

        if unmapped_identifiers:
            logger.warning(
                f"Consistency rule(s) reference identifier(s) not matched to any "
                f"column: {sorted(unmapped_identifiers)}. Pruning will be skipped "
                f"to avoid dropping a referenced column."
            )
            return list(p_df_columns)

        if not columns_to_cache and enabled:
            logger.warning(
                "Column extraction produced no columns while validators are "
                "enabled; skipping prune and validating with all columns."
            )
            return list(p_df_columns)

        result_cols = sorted(columns_to_cache)
        logger.debug(
            f"Extracted {len(result_cols)} columns for caching "
            f"(from {len(p_df_columns)} total)"
        )
        return result_cols

    @staticmethod
    def _send_stop_notification(
        p_dataset_name: str, p_stop_summaries: List[Dict[str, Any]]
    ) -> None:
        """Send one Teams message per dataset listing every month that STOPped.

        Args:
            p_dataset_name: Dataset name
            p_stop_summaries: Month summaries with decision STOP
        """
        if not p_stop_summaries:
            return

        env = get_environment()
        if env not in ["dev", "prod"]:
            return

        months = [str(s.get("month", "N/A")) for s in p_stop_summaries]
        shown = ", ".join(months[:24]) + (
            f" (+{len(months) - 24} más)" if len(months) > 24 else ""
        )
        critical, errors_count = stop_totals(p_stop_summaries)
        message = (
            f"🚨 CRITICAL sanity check failure ({env})\n"
            f"Dataset: {p_dataset_name}\n"
            f"Months blocked: {len(months)} → {shown}\n"
            f"Critical failures: {critical}\n"
            f"Validator errors: {errors_count}\n"
            f"Decision: STOP"
        )
        errors = {s["error"] for s in p_stop_summaries if s.get("error")}
        if errors:
            message += f"\nError: {'; '.join(sorted(errors))[:500]}"
        try:
            send_message_teams(message)
        except Exception as e:
            logger.error(
                f"Could not send Teams notification for {p_dataset_name}: {e}"
            )

    @staticmethod
    def _validate_sql_identifier(p_identifier: str, p_label: str) -> bool:
        """Validate that an identifier is safe for SQL interpolation.

        Args:
            p_identifier: Identifier to validate (e.g., dataset name, prefix)
            p_label: Label for logging (e.g., "dataset_name", "prefix")

        Returns:
            True if identifier contains only safe characters, False otherwise
        """
        if not p_identifier:
            return True

        if not re.match(r"^[a-zA-Z0-9_.-]+$", p_identifier):
            logger.error(
                f"❌ SECURITY: Invalid characters in {p_label}: '{p_identifier}'. "
                f"Only alphanumeric, underscore, hyphen, and dot allowed."
            )
            return False

        return True

    @staticmethod
    def _is_valid_iso_date(p_date: str) -> bool:
        """Validate that a string is a safe ISO date (YYYY-MM-DD) for interpolation.

        Args:
            p_date: Date string to validate

        Returns:
            True if the value is a well-formed YYYY-MM-DD date, False otherwise.
        """
        if not p_date or not re.match(r"^\d{4}-\d{2}-\d{2}$", p_date):
            return False
        try:
            datetime.strptime(p_date, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    _QUALITY_TABLE_NAME = "sc_sanity_check_results"
    _KNOWN_QUALITY_TABLES: set = set()

    @staticmethod
    def _remember_quality_table(p_table: str) -> None:
        """Record that a quality history table exists (skips later lookups)."""
        LayerUtils._KNOWN_QUALITY_TABLES.add(p_table)

    @staticmethod
    def _resolve_quality_table(
        p_catalog: str, p_schema_name: str, p_label: str
    ) -> Optional[str]:
        """Find the quality history table of the layer schema.

        Args:
            p_catalog: Catalog name
            p_schema_name: Layer schema name (e.g. 'dev_data_cleaning')
            p_label: Label for logging

        Returns:
            Fully qualified table name, or None if it does not exist yet.
        """
        table = f"{p_catalog}.{p_schema_name}.{LayerUtils._QUALITY_TABLE_NAME}"
        if table in LayerUtils._KNOWN_QUALITY_TABLES:
            return table
        if LayerUtils._with_session_retry(
            lambda: get_spark().catalog.tableExists(table), p_label
        ):
            LayerUtils._remember_quality_table(table)
            return table
        return None

    @staticmethod
    def _log_quality_table_absent(
        p_layer: str, p_dataset_name: str, p_detail: str = ""
    ) -> None:
        """Log that the history table does not exist yet, so the gate passes."""
        logger.info(
            f"ℹ️  Quality gate table not yet created for {p_layer}{p_detail}. "
            f"First sanity check execution will initialize it. "
            f"Allowing {p_dataset_name} to proceed."
        )

    @staticmethod
    def check_quality_gate(
        p_dataset_name: str,
        p_start_date: str,
        p_end_date: str,
        p_layer: str = "raw_data",
    ) -> bool:
        """Check if a dataset passed its quality gate for a date range.

        Args:
            p_dataset_name: Name of the dataset
            p_start_date: Start date for the quality gate check (YYYY-MM-DD)
            p_end_date: End date for the quality gate check (YYYY-MM-DD)
            p_layer: Layer of the dataset (default: "raw_data")

        Returns:
            False if critical failures were found or the gate could not be
            verified; True if passed or the history table does not exist yet.
        """
        try:
            if not p_dataset_name or not LayerUtils._validate_sql_identifier(
                p_dataset_name, "dataset_name"
            ):
                return False

            if not (
                LayerUtils._is_valid_iso_date(p_start_date)
                and LayerUtils._is_valid_iso_date(p_end_date)
            ):
                logger.error(
                    f"❌ Quality gate blocked for '{p_dataset_name}': invalid date "
                    f"range '{p_start_date}' → '{p_end_date}' (expected YYYY-MM-DD)."
                )
                return False

            catalog = get_catalog_name()
            prefix_map = load_yaml_file(config_path(PREFIXES_FILE)) or {}
            prefix_value = prefix_map.get(os.getenv("output_domain", ""))

            env_value = os.getenv("env") or get_environment()
            schema_name = f"{env_value}_{p_layer}"

            if not (
                LayerUtils._validate_sql_identifier(catalog, "catalog")
                and LayerUtils._validate_sql_identifier(schema_name, "schema_name")
            ):
                return False

            if env_value not in ("prod", "dev"):
                logger.warning(
                    f"Quality gate reading from catalog '{catalog}' for env "
                    f"'{env_value}'"
                )

            if prefix_value and not LayerUtils._validate_sql_identifier(
                prefix_value, "prefix"
            ):
                return False

            candidates = [p_dataset_name]
            if prefix_value:
                candidates.append(f"{prefix_value}_{p_dataset_name}")
            dataset_list = "', '".join(candidates)

            quality_table = LayerUtils._resolve_quality_table(
                catalog, schema_name, p_dataset_name
            )
            if quality_table is None:
                LayerUtils._log_quality_table_absent(
                    p_layer, p_dataset_name, f" (schema='{schema_name}')"
                )
                return True

            query = (
                f"SELECT nombre_dataset, _observ_end_dt, estado, severidad "
                f"FROM {quality_table} "
                f"WHERE estado = 'FAIL' AND severidad = 'CRITICAL' "
                f"AND nombre_dataset IN ('{dataset_list}') "
                f"AND _observ_start_dt <= '{p_end_date}' "
                f"AND _observ_end_dt >= '{p_start_date}'"
            )
            critical_count = LayerUtils._with_session_retry(
                lambda: get_spark().sql(query).count(), p_dataset_name
            )

            if critical_count > 0:
                logger.error(
                    f"❌ Quality Gate FATAL [{p_layer}]: '{p_dataset_name}' has "
                    f"{critical_count} CRITICAL failure(s) between "
                    f"{p_start_date} and {p_end_date}."
                )
                return False

            logger.info(f"✅ Quality Gate PASSED [{p_layer}]: {p_dataset_name}")
            return True

        except Exception as e:
            if LayerUtils._is_missing_table_error(str(e)):
                LayerUtils._log_quality_table_absent(p_layer, p_dataset_name)
                return True

            logger.error(
                f"❌ Quality Gate verification error [{p_layer}] for "
                f"{p_dataset_name}. Blocking by default. Error: {e}"
            )
            return False

    @staticmethod
    def load_layer_config(
        p_layer_name: str, p_domain_filter: Optional[str] = None
    ) -> Dict[str, Any]:
        """Load a layer's sanity check configuration, optionally for one domain.

        Returns:
            Merged configuration dictionary ({} if nothing is found).
        """
        config_dir = Path(config_path(p_layer_name, "src"))
        if p_layer_name == "raw_data":
            config_dir = config_dir / "sanity_check" / "conf"
        if not config_dir.is_dir():
            logger.warning(
                f"No sanity check config directory for layer '{p_layer_name}': "
                f"{config_dir}. If running from an installed package, verify the "
                f"conf directory ships an __init__.py so its YAML files are packaged."
            )
            return {}

        if p_domain_filter:
            files = list(config_dir.glob(f"{p_domain_filter}_sanity_check.yml")) or list(
                (config_dir / p_domain_filter).glob("*_sanity_check.yml")
            )
        else:
            files = list(config_dir.glob("*_sanity_check.yml")) or [
                yaml_file
                for d in config_dir.iterdir()
                if d.is_dir()
                and not d.name.startswith(("_", "."))
                and d.name not in {"sanity_check", "utils", "conf"}
                for yaml_file in d.glob("*_sanity_check.yml")
            ]
        if not files:
            logger.warning(
                f"No sanity check config in {config_dir} (domain={p_domain_filter})"
            )
            return {}

        merged_config: Dict[str, Any] = {}
        for yaml_file in sorted(files):
            try:
                domain_config = load_yaml_file(str(yaml_file))
            except Exception as e:
                logger.error(f"Failed to load {yaml_file.name}: {e}", exc_info=True)
                continue
            if domain_config:
                merged_config.update(domain_config)
            else:
                logger.warning(f"Empty or invalid config in {yaml_file.name}")
        return merged_config
