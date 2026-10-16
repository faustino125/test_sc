import importlib
import logging
import os
import re
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

from databricks.connect import DatabricksSession  # type: ignore
from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql import functions as f  # type: ignore

from data_engineering.core.sanity_check.base import BaseValidator
from data_engineering.core.sanity_check.standard_validators import (
    StandardConsistencyValidator,
    StandardDimensionalValidator,
    StandardDuplicatesValidator,
    StandardNullValuesValidator,
)
from data_engineering.core.sanity_check.utils import _TIMEZONE_UTC_6, EnvironmentConfig
from data_engineering.core.utils import load_yaml_file, send_message_teams

logger = logging.getLogger(__name__)

_SPARK_SESSION: Optional[DatabricksSession] = None
_SPARK_SESSION_LOCK: threading.Lock = threading.Lock()
_SQL_TOKEN_SKIPLIST = frozenset(
    {
        "and",
        "or",
        "not",
        "in",
        "is",
        "null",
        "true",
        "false",
        "between",
        "like",
        "rlike",
        "case",
        "when",
        "then",
        "else",
        "end",
        "as",
        "on",
        "exists",
        "distinct",
        "cast",
        "coalesce",
        "nullif",
        "asc",
        "desc",
        "interval",
        "current_date",
        "current_timestamp",
        "date",
        "timestamp",
        "ilike",
        "regexp",
        "div",
    }
)


_SQL_STRING_LITERAL = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"")
# Backticked names, or bare identifiers that are not part of a number and
# not followed by "(" (function calls).
_SQL_IDENTIFIER = re.compile(
    r"`([^`]+)`|(?<![A-Za-z0-9_.])([A-Za-z_][A-Za-z0-9_.]*)\b(?!\s*\()"
)


def _rule_identifiers(p_condition: str) -> List[str]:
    """Lower-cased identifiers a SQL condition references (columns, maybe).

    String literals, function calls and SQL keywords are ignored.
    """
    tokens = []
    without_literals = _SQL_STRING_LITERAL.sub(" ", p_condition)
    for quoted, plain in _SQL_IDENTIFIER.findall(without_literals):
        token = (quoted or plain).strip().rstrip(".").lower()
        if token and token not in _SQL_TOKEN_SKIPLIST:
            tokens.append(token)
    return tokens


def _list(p_value: Any) -> list:
    """The value if it is a list, else an empty list."""
    return p_value if isinstance(p_value, list) else []


def _str_list(p_value: Any) -> List[str]:
    """A column name or list of names as a list of strings (others dropped)."""
    if isinstance(p_value, str):
        return [p_value]
    return [v for v in _list(p_value) if isinstance(v, str)]


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
    """Drop the cached session so the next get_spark() opens a new one.

    The old session is stopped so getOrCreate() does not hand back the same
    broken default session.
    """
    global _SPARK_SESSION

    with _SPARK_SESSION_LOCK:
        old_session = _SPARK_SESSION
        _SPARK_SESSION = None

    if old_session is not None:
        try:
            old_session.stop()
        except Exception as e:
            logger.debug(f"Error stopping stale Spark session: {e}")


_GLOBAL_REGISTRY: Optional["ValidatorRegistry"] = None
_REGISTRY_LOCK: threading.Lock = threading.Lock()

STANDARD_VALIDATORS: Dict[str, Type[BaseValidator]] = {
    "duplicates": StandardDuplicatesValidator,
    "null_values": StandardNullValuesValidator,
    "dimensional": StandardDimensionalValidator,
    "consistency": StandardConsistencyValidator,
}

# Optional registry class each layer may define in
# data_engineering.<layer>.src.sanity_check.validators
_LAYER_REGISTRY_CLASSES = {
    "raw_data": "RawValidatorRegistry",
    "data_cleaning": "CleaningValidatorRegistry",
    "data_transformation": "TransformationValidatorRegistry",
    "data_integration": "IntegrationDataValidatorRegistry",
}


def _initialize_layer_validators(p_registry: "ValidatorRegistry") -> None:
    """Register the standard validators for every layer, then each layer's own.

    A layer's registry can add new types or override a standard one without
    making the other standard types disappear.

    Args:
        p_registry: Central validator registry to populate
    """
    for layer_name, registry_class_name in _LAYER_REGISTRY_CLASSES.items():
        for validator_type, validator_class in STANDARD_VALIDATORS.items():
            p_registry.register_validator(
                validator_type, validator_class, p_layer=layer_name
            )

        module_path = f"data_engineering.{layer_name}.src.sanity_check.validators"
        try:
            module = importlib.import_module(module_path)
        except ModuleNotFoundError as e:
            if e.name and module_path.startswith(e.name):
                logger.debug(f"No layer-specific validators module for {layer_name}")
            else:
                logger.error(
                    f"Layer validators module {module_path} failed to import: {e}"
                )
            continue
        except Exception as e:
            logger.error(f"Layer validators module {module_path} failed to load: {e}")
            continue

        registry_class = getattr(module, registry_class_name, None)
        validators_map = getattr(registry_class, "VALIDATORS", None)
        if not isinstance(validators_map, dict):
            logger.warning(
                f"{module_path} has no {registry_class_name}.VALIDATORS mapping"
            )
            continue

        for validator_type, validator_class in validators_map.items():
            p_registry.register_validator(
                validator_type, validator_class, p_layer=layer_name
            )
        logger.info(
            f"Registered layer-specific validators for {layer_name}: "
            f"{sorted(validators_map)}"
        )

    logger.info("Validator initialization complete.")


class ValidatorRegistry:
    """Validator classes by layer; the `None` layer holds common fallbacks.

    It is filled once by get_registry() and only read afterwards.
    """

    def __init__(self):
        self._validators: Dict[Optional[str], Dict[str, Type[BaseValidator]]] = {}

    def register_validator(
        self,
        p_validator_type: str,
        p_validator_class: Type[BaseValidator],
        p_layer: Optional[str] = None,
    ) -> None:
        """Register or override a validator for a layer (or for all, if None)."""
        self._validators.setdefault(p_layer, {})[p_validator_type] = p_validator_class

    def get_validator_class(
        self, p_validator_type: str, p_layer: Optional[str] = None
    ) -> Optional[Type[BaseValidator]]:
        """Layer-specific validator class, else the common one, else None."""
        return self._validators.get(p_layer, {}).get(
            p_validator_type
        ) or self._validators.get(None, {}).get(p_validator_type)

    def get_validators(
        self,
        p_validator_types: List[str],
        p_config: Dict[str, Any],
        p_layer: Optional[str] = None,
    ) -> List[BaseValidator]:
        """Build list of validator instances.

        Args:
            p_validator_types: List of validator type names to instantiate
            p_config: Configuration dict for all validators
            p_layer: Optional layer name for layer-specific lookup

        Returns:
            List of instantiated BaseValidator objects
        """
        validators = []
        spark = get_spark()
        for validator_type in p_validator_types:
            validator_class = self.get_validator_class(validator_type, p_layer)
            if not validator_class:
                logger.warning(
                    f"Validator type '{validator_type}' is enabled but not "
                    f"registered for layer {p_layer}"
                )
                continue
            validator_config = p_config.get(validator_type, {})
            validators.append(validator_class(validator_config, spark))
        return validators


def get_registry() -> ValidatorRegistry:
    """Process-wide validator registry, initialized on first use."""
    global _GLOBAL_REGISTRY

    with _REGISTRY_LOCK:
        if _GLOBAL_REGISTRY is None:
            registry = ValidatorRegistry()
            _initialize_layer_validators(registry)
            _GLOBAL_REGISTRY = registry
    return _GLOBAL_REGISTRY


class LayerUtils:
    """Utility functions for layer-specific sanity check operations."""

    VALID_LAYERS = [
        "raw_data",
        "data_cleaning",
        "data_transformation",
        "data_integration",
    ]

    LAYER_DEPENDENCIES = {
        "raw_data": None,
        "data_cleaning": "raw_data",
        "data_transformation": "data_cleaning",
        "data_integration": "data_transformation",
    }

    @staticmethod
    def load_config(
        p_path: str, p_domain_filter: Optional[str] = None
    ) -> Dict[str, Any]:
        """Load sanity check configuration from YAML files.

        Files named `<domain>_sanity_check.yml` are read from the given
        directory (or the layer's `src/sanity_check/conf`), or from its domain
        subdirectories, and merged in name order.

        Args:
            p_path: Path to the configuration file or directory.
            p_domain_filter: Optional domain filter to select specific configurations.

        Returns:
            Merged configuration dictionary ({} if nothing is found).
        """
        try:
            absolute_path = os.path.abspath(p_path)
            layer_name = next(
                (
                    part
                    for part in absolute_path.split(os.sep)
                    if part in LayerUtils.VALID_LAYERS
                ),
                None,
            )
            if not layer_name:
                logger.warning(f"Could not identify layer from path: {absolute_path}")
                return {}

            config_dir = Path(absolute_path)
            if config_dir.is_file() or absolute_path.lower().endswith(".yml"):
                config_dir = config_dir.parent

            candidates = [
                config_dir,
                Path(EnvironmentConfig.get_root_dir())
                / layer_name
                / "src"
                / "sanity_check"
                / "conf",
            ]
            config_path = next((c for c in candidates if c.is_dir()), None)
            if not config_path:
                logger.warning(
                    f"No sanity check config directory found for layer "
                    f"'{layer_name}'. Tried: {[str(c) for c in candidates]}. "
                    f"If running from an installed package, verify the conf "
                    f"directory ships an __init__.py so its YAML files are packaged."
                )
                return {}

            if p_domain_filter:
                domain_files = list(
                    config_path.glob(f"{p_domain_filter}_sanity_check.yml")
                ) or list((config_path / p_domain_filter).glob("*_sanity_check.yml"))
                if not domain_files:
                    logger.warning(f"No config found for domain '{p_domain_filter}'")
                    return {}
            else:
                domain_files = list(config_path.glob("*_sanity_check.yml"))
                if not domain_files:
                    for d in config_path.iterdir():
                        if (
                            d.is_dir()
                            and not d.name.startswith(("_", "."))
                            and d.name not in {"sanity_check", "utils", "conf"}
                        ):
                            domain_files.extend(d.glob("*_sanity_check.yml"))
                if not domain_files:
                    logger.warning(f"No config files in {config_path}")
                    return {}

            merged_config = {}
            for yaml_file in sorted(domain_files):
                try:
                    domain_config = load_yaml_file(str(yaml_file))
                    if domain_config:
                        merged_config.update(domain_config)
                    else:
                        logger.warning(f"Empty or invalid config in {yaml_file.name}")
                except Exception as e:
                    logger.error(f"Failed to load {yaml_file.name}: {e}", exc_info=True)

            return merged_config

        except Exception as e:
            logger.error(f"Error loading config: {e}", exc_info=True)
            return {}

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
    ) -> DataFrame:
        """Apply date filtering to a DataFrame.

        Args:
            p_df: Input DataFrame to filter
            p_date_col: Name of date column (case-insensitive); None means no filtering
            p_start_dt: Start date boundary (string format)
            p_end_dt: End date boundary (string format)
            p_source_name: Name of source for logging

        Returns:
            Filtered DataFrame between specified date boundaries
        """
        if not p_date_col:
            logger.warning(
                f"No date column for {p_source_name}. Validating ALL records."
            )
            return p_df

        actual_col = next(
            (c for c in p_df.columns if c.lower() == p_date_col.lower()), None
        )
        if not actual_col:
            logger.warning(f"Date column '{p_date_col}' not in {p_source_name}")
            return p_df

        return p_df.filter(
            LayerUtils._date_range_condition(
                p_df, actual_col, p_start_dt, p_end_dt
            )
        )

    @staticmethod
    def _date_range_condition(
        p_df: DataFrame, p_actual_col: str, p_start_dt: str, p_end_dt: str
    ):
        """Build an inclusive date-range condition that keeps partition pruning.

        The column is compared in its own type: wrapping a DATE or TIMESTAMP
        partition column in to_date() can stop Delta from skipping files.

        Args:
            p_df: DataFrame owning the column
            p_actual_col: Actual (resolved) column name
            p_start_dt: First day included (YYYY-MM-DD)
            p_end_dt: Last day included (YYYY-MM-DD)

        Returns:
            Boolean Column expression.
        """
        col = f.col(p_actual_col)
        start_date = f.to_date(f.lit(p_start_dt))
        end_date = f.to_date(f.lit(p_end_dt))
        try:
            kind = p_df.schema[p_actual_col].dataType.simpleString()
        except Exception:
            kind = ""

        if kind == "date":
            return col.between(start_date, end_date)
        if kind.startswith("timestamp"):
            return (col >= start_date.cast(kind)) & (
                col < f.date_add(end_date, 1).cast(kind)
            )
        return f.to_date(col).between(start_date, end_date)

    # Only error codes that unambiguously mean "this table does not exist".
    # Generic phrases ("does not exist", "cannot be found") also appear for a
    # missing schema/catalog or column, and must not let the gate pass.
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

        The callable must include the action that actually talks to the server
        (count, collect, schema...) and call get_spark() itself, so the retry
        uses the new session. Spark Connect builds DataFrames lazily, so
        retrying only the construction would never see a session error.

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

    @staticmethod
    def load_dataset(
        p_metadata: Dict[str, Any],
        p_config: Dict[str, Any],
        p_start_dt: Optional[str] = None,
        p_end_dt: Optional[str] = None,
    ) -> DataFrame:
        """Load a dataset (Parquet or Delta/SQL) with optional date filtering.

        Errors are raised, not swallowed: a dataset that cannot be read must be
        reported as a failure, never skipped as if it had no data. An empty
        result is returned as-is so the engine can apply `fail_on_empty`.

        Args:
            p_metadata: Dataset metadata (dataset_name, path or database, date_col)
            p_config: Dataset quality configuration
            p_start_dt: Start date for filtering; skipped when None
            p_end_dt: End date for filtering; skipped when None

        Returns:
            Filtered DataFrame (possibly empty).

        Raises:
            ValueError: If the metadata has neither a path nor a table.
            Exception: Any error raised while reading the source.
        """
        label = p_metadata.get("dataset_name") or p_metadata.get("path") or "unknown"
        date_col = LayerUtils._resolve_date_col(p_config or {}, p_metadata)
        path = p_metadata.get("path")

        if path:

            def _read() -> DataFrame:
                try:
                    df = get_spark().read.format("parquet").load(path)
                except Exception as e:
                    if "JVM_ATTRIBUTE_NOT_SUPPORTED" not in str(e):
                        raise
                    df = get_spark().read.parquet(path)
                _ = df.columns
                return df

        else:
            database = p_metadata.get("database")
            dataset_name = p_metadata.get("dataset_name")
            if not (database and dataset_name):
                raise ValueError(
                    f"Invalid metadata for {label}: missing path or "
                    f"database/dataset_name"
                )
            catalog = EnvironmentConfig.get_catalog_name()
            if not (
                LayerUtils._validate_sql_identifier(catalog, "catalog")
                and LayerUtils._validate_sql_identifier(database, "database")
                and LayerUtils._validate_sql_identifier(dataset_name, "dataset_name")
            ):
                raise ValueError(f"Unsafe table identifier for {label}")
            query = f"SELECT * FROM {catalog}.{database}.{dataset_name}"

            def _read() -> DataFrame:
                df = get_spark().sql(query)
                _ = df.columns  # analyze now: missing tables/dead sessions fail here
                return df

        df = LayerUtils._with_session_retry(_read, label)
        if p_start_dt and p_end_dt:
            df = LayerUtils._apply_date_filter(
                df, date_col, p_start_dt, p_end_dt, label
            )
        return df

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
        base_dir = EnvironmentConfig.get_root_dir()
        catalog_path = os.path.join(
            base_dir, p_catalog_layer, "conf", "data_catalog.yml"
        )

        if not os.path.exists(catalog_path):
            logger.error(f"data_catalog.yml not found: {catalog_path}")
            return []

        catalog = load_yaml_file(catalog_path)
        inputs = catalog.get("Input", {})
        if not inputs:
            logger.warning(f"No 'Input' section in {catalog_path}")
            return []

        entries = [v for v in inputs.values() if isinstance(v, dict)]
        env = EnvironmentConfig.get_environment()
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

        settings = load_yaml_file(
            os.path.join(base_dir, "env", "base", "global_settings.yml")
        )
        env_path = os.path.join(base_dir, "env", env, "global_settings.yml")
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

    # Validator types whose config keys _extract_columns_for_caching understands.
    # Any other enabled type (a layer-specific validator) disables pruning,
    # since its referenced columns cannot be known.
    _PRUNABLE_VALIDATOR_TYPES = frozenset(STANDARD_VALIDATORS)
    # Config keys holding column names (as a string or a list) ...
    _COLUMN_LIST_KEYS = (
        "columns",
        "fields",
        "column_list",
        "check_columns",
        "primary_key",
        "pk",
        "unique_id",
        "unique_keys",
    )
    # ... and config keys holding a mapping keyed by column name.
    _COLUMN_MAPPING_KEYS = (
        "null_thresholds",
        "null_limits",
        "null_percentages",
        "dimensions",
        "allowed_values",
        "domain_constraints",
    )

    @staticmethod
    def _extract_columns_for_caching(
        p_config: Dict[str, Any],
        p_date_col: Optional[str],
        p_df_columns: List[str],
    ) -> List[str]:
        """Extract columns needed for sanity checks to optimize memory usage.

        Pruning is skipped (all columns returned) whenever a referenced column
        cannot be determined with certainty.

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
        custom_types = set(enabled) - LayerUtils._PRUNABLE_VALIDATOR_TYPES
        if custom_types:
            logger.info(
                f"Layer-specific validator(s) {sorted(custom_types)} enabled; "
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
            for key in LayerUtils._COLUMN_LIST_KEYS:
                names += _str_list(cfg.get(key))
            for key in LayerUtils._COLUMN_MAPPING_KEYS:
                if isinstance(cfg.get(key), dict):
                    names += _str_list(list(cfg[key]))
            for check in _list(cfg.get("value_checks")):
                if isinstance(check, dict):
                    names += _str_list(check.get("column"))
            if validator_type == "consistency" or "rules" in cfg:
                for rule in _list(cfg.get("rules")):
                    if not isinstance(rule, dict):
                        continue
                    names += _str_list(rule.get("column"))
                    if isinstance(rule.get("condition"), str):
                        rule_tokens += _rule_identifiers(rule["condition"])

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

        env = EnvironmentConfig.get_environment()
        if env not in ["dev", "prod"]:
            return

        months = [str(s.get("month", "N/A")) for s in p_stop_summaries]
        shown = ", ".join(months[:24]) + (
            f" (+{len(months) - 24} más)" if len(months) > 24 else ""
        )
        message = (
            f"🚨 CRITICAL sanity check failure ({env})\n"
            f"Dataset: {p_dataset_name}\n"
            f"Months blocked: {len(months)} → {shown}\n"
            f"Critical failures: "
            f"{sum(s.get('critical_failures', 0) for s in p_stop_summaries)}\n"
            f"Validator errors: "
            f"{sum(s.get('validator_errors', 0) for s in p_stop_summaries)}\n"
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

    @staticmethod
    def _resolve_quality_table(
        p_catalog: str, p_schema_name: str, p_label: str
    ) -> Optional[str]:
        """Find the quality history table, preferring the layer schema.

        Errors (permissions, network, session) are raised so the caller fails
        closed; only a confirmed absence returns None.

        Args:
            p_catalog: Catalog name
            p_schema_name: Layer schema name (e.g. 'dev_data_cleaning')
            p_label: Label for logging

        Returns:
            Fully qualified table name, or None if it does not exist yet.
        """
        candidates = [
            f"{p_catalog}.{p_schema_name}.{LayerUtils._QUALITY_TABLE_NAME}",
            f"{p_catalog}.{LayerUtils._QUALITY_TABLE_NAME}",
        ]
        for table in candidates:
            if LayerUtils._with_session_retry(
                lambda t=table: get_spark().catalog.tableExists(t), p_label
            ):
                return table
        return None

    @staticmethod
    def check_quality_gate(
        p_dataset_name: str,
        p_start_date: str,
        p_end_date: str,
        p_layer: str = "raw_data",
    ) -> bool:
        """Check if a dataset passed its quality gate for a date range.

        Fails closed: any error other than a confirmed missing history table
        blocks the dataset.

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

            catalog = EnvironmentConfig.get_catalog_name()
            prefix_path = os.path.join(
                EnvironmentConfig.get_root_dir(), "env", "base", "prefixes.yml"
            )
            prefix_map = load_yaml_file(prefix_path) or {}
            prefix_value = prefix_map.get(os.getenv("output_domain", ""))

            env_value = os.getenv("env") or EnvironmentConfig.get_environment()
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
                logger.info(
                    f"ℹ️  Quality gate table not yet created for {p_layer} "
                    f"(schema='{schema_name}'). First sanity check execution "
                    f"will initialize it. Allowing {p_dataset_name} to proceed."
                )
                return True

            query = (
                f"SELECT nombre_dataset, _observ_end_dt, estado, severidad "
                f"FROM {quality_table} "
                f"WHERE estado = 'FAIL' AND severidad = 'CRITICAL' "
                f"AND nombre_dataset IN ('{dataset_list}') "
                f"AND _observ_end_dt >= '{p_start_date}' "
                f"AND _observ_end_dt <= '{p_end_date}'"
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
                logger.info(
                    f"ℹ️  Quality gate table not yet created for {p_layer}. "
                    f"First sanity check execution will initialize it. "
                    f"Allowing {p_dataset_name} to proceed."
                )
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

        raw_data keeps its YAML files in src/sanity_check/conf; the other layers
        keep them next to each domain under src/.
        """
        layer_dir = os.path.join(EnvironmentConfig.get_root_dir(), p_layer_name, "src")
        if p_layer_name == "raw_data":
            layer_dir = os.path.join(layer_dir, "sanity_check", "conf")
        return LayerUtils.load_config(layer_dir, p_domain_filter=p_domain_filter)


@dataclass
class FlowExecutionContext:
    """Captures execution context from environment variables."""

    workflow: str
    flow_name: str
    node_name: Optional[str]
    domain: Optional[str]
    is_isolated_execution: bool
    start_dt: Optional[str]
    end_dt: Optional[str]

    @classmethod
    def from_environment(cls) -> "FlowExecutionContext":
        """Build context from environment variables."""
        node_name = os.getenv("node")
        context = cls(
            workflow=os.getenv("workflow", "unknown"),
            flow_name=os.getenv("flow", "unknown"),
            node_name=node_name,
            domain=node_name.split(".")[0] if node_name and "." in node_name else None,
            is_isolated_execution=node_name is not None,
            start_dt=os.getenv("start_dt"),
            end_dt=os.getenv("end_dt"),
        )

        logger.info(f"Execution context: {context}")
        return context

    def __str__(self) -> str:
        return (
            f"FlowExecutionContext("
            f"workflow={self.workflow}, "
            f"flow={self.flow_name}, "
            f"node={self.node_name}, "
            f"domain={self.domain}, "
            f"isolated={self.is_isolated_execution})"
        )
