import logging
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import reduce
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from databricks.connect import DatabricksSession  # type: ignore
from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql import functions as f  # type: ignore
from pyspark.storagelevel import StorageLevel  # type: ignore

from data_engineering.core.sanity_check.base import (
    BaseValidator,
    QualitySeverity,
    ValidationRecord,
    ValidationStatus,
)
from data_engineering.core.sanity_check.standard_validators import (
    StandardConsistencyValidator,
    StandardDimensionalValidator,
    StandardDuplicatesValidator,
    StandardNullValuesValidator,
)
from data_engineering.core.sanity_check.utils import (
    _TIMEZONE_UTC_6,
    VALIDATION_RESULT_SCHEMA,
    EnvironmentConfig,
    create_record,
    to_dataframe,
)
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
    }
)


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


def df_is_empty(p_df: DataFrame) -> bool:
    """Efficient check whether a DataFrame is empty without forcing shuffle.

    Args:
        p_df: Spark DataFrame to check

    Returns:
        True if DataFrame has no rows, False otherwise
    """
    try:
        return len(p_df.limit(1).collect()) == 0
    except Exception as e:
        logger.warning(
            f"limit(1).collect() failed ({type(e).__name__}), "
            f"falling back to count(): {e}"
        )
        return p_df.count() == 0


_GLOBAL_REGISTRY: Optional["ValidatorRegistry"] = None
_REGISTRY_LOCK: threading.Lock = threading.Lock()


def _initialize_layer_validators(p_registry: "ValidatorRegistry") -> None:
    """Initialize all layer-specific validators in the central registry.

    Args:
        p_registry: Central validator registry to populate
    """
    standard_validators_map = {
        "duplicates": StandardDuplicatesValidator,
        "null_values": StandardNullValuesValidator,
        "dimensional": StandardDimensionalValidator,
        "consistency": StandardConsistencyValidator,
    }

    layers_config = [
        ("raw_data", "RawValidatorRegistry"),
        ("data_cleaning", "CleaningValidatorRegistry"),
        ("data_transformation", "TransformationValidatorRegistry"),
        ("data_integration", "IntegrationDataValidatorRegistry"),
    ]

    for layer_name, registry_class_name in layers_config:
        layer_specific_found = False

        try:
            module_path = f"data_engineering.{layer_name}.src.sanity_check.validators"
            module = __import__(module_path, fromlist=[registry_class_name])
            registry_class = getattr(module, registry_class_name, None)

            if registry_class and hasattr(registry_class, "VALIDATORS"):
                for (
                    validator_type,
                    validator_class,
                ) in registry_class.VALIDATORS.items():
                    p_registry.register_validator(
                        validator_type, validator_class, p_layer=layer_name
                    )
                layer_specific_found = True
                logger.info(f"Registered layer-specific validators for {layer_name}")
        except (ModuleNotFoundError, ImportError, AttributeError):
            logger.debug(f"No layer-specific validators module found for {layer_name}")
        except Exception as e:
            logger.debug(f"Error loading layer validators for {layer_name}: {e}")

        if not layer_specific_found:
            for validator_type, validator_class in standard_validators_map.items():
                p_registry.register_validator(
                    validator_type, validator_class, p_layer=layer_name
                )
            logger.info(f"Registered standard validators for layer {layer_name}")

    logger.info("Validator initialization complete.")


class ValidatorRegistry:
    """Central registry for validators with layer-specific support."""

    def __init__(self):
        """Initialize registry with layer-specific validator overrides."""
        self._layer_validators: Dict[str, Dict[str, Type[BaseValidator]]] = {}
        self._common_validators: Dict[str, Type[BaseValidator]] = {}
        self._lock = threading.RLock()
        self.spark = get_spark()

    def register_validator(
        self,
        p_validator_type: str,
        p_validator_class: Type[BaseValidator],
        p_layer: Optional[str] = None,
    ) -> None:
        """Register or override a validator (thread-safe).

        Args:
            p_validator_type: Type of validator (duplicates, null_values, etc.)
            p_validator_class: Validator class to register
            p_layer: Optional layer name for layer-specific registration
        """
        with self._lock:
            validators_dict = (
                self._layer_validators.setdefault(p_layer, {})
                if p_layer
                else self._common_validators
            )
            validators_dict[p_validator_type] = p_validator_class

    def get_validator_class(
        self, p_validator_type: str, p_layer: Optional[str] = None
    ) -> Optional[Type[BaseValidator]]:
        """Get validator class by type and optional layer (thread-safe).

        Args:
            p_validator_type: Type of validator to retrieve
            p_layer: Optional layer name for layer-specific lookup

        Returns:
            Validator class if found, None otherwise
        """
        with self._lock:
            if (
                p_layer
                and p_layer in self._layer_validators
                and p_validator_type in self._layer_validators[p_layer]
            ):
                return self._layer_validators[p_layer][p_validator_type]
            return self._common_validators.get(p_validator_type)

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
        for validator_type in p_validator_types:
            validator_class = self.get_validator_class(validator_type, p_layer)
            if not validator_class:
                continue
            validator_config = p_config.get(validator_type, {})
            validators.append(validator_class(validator_config, self.spark))
        return validators


class QualityEngine:
    """Main engine for executing quality validations."""

    def __init__(self):
        """Initialize QualityEngine with validator registry."""
        global _GLOBAL_REGISTRY

        with _REGISTRY_LOCK:
            if _GLOBAL_REGISTRY is None:
                _GLOBAL_REGISTRY = ValidatorRegistry()
                _initialize_layer_validators(_GLOBAL_REGISTRY)

        self.registry = _GLOBAL_REGISTRY
        self.spark = get_spark()

    @staticmethod
    def _as_bool(p_value: Any, p_default: bool = True) -> bool:
        """Normalize boolean-like values from config/env vars.

        Args:
            p_value: Value to convert to boolean
            p_default: Default boolean to return if conversion fails

        Returns:
            Boolean interpretation of value or default
        """
        if p_value is None:
            return p_default
        if isinstance(p_value, bool):
            return p_value
        if isinstance(p_value, (int, float)):
            return p_value != 0
        if isinstance(p_value, str):
            normalized = p_value.strip().lower()
            if normalized in {"1", "true", "yes", "y", "on"}:
                return True
            if normalized in {"0", "false", "no", "n", "off"}:
                return False
        return p_default

    @staticmethod
    def _run_validator_safely(
        validator: BaseValidator,
        p_df: DataFrame,
        p_dataset_name: str,
    ) -> Tuple[Optional[DataFrame], Optional[ValidationRecord]]:
        """Execute a single validator in a thread-safe manner.

        Args:
            validator: Validator instance to execute
            p_df: DataFrame to validate
            p_dataset_name: Dataset name for logging

        Returns:
            Tuple of (result_df, error_record) - one will be None
        """
        try:
            result_df = validator.validate(p_df, p_dataset_name)
            return result_df, None
        except Exception as e:
            validator_name = validator.__class__.__name__
            logger.error(f"Validator {validator_name} failed for {p_dataset_name}: {e}")

            error_record = create_record(
                p_dataset_name=p_dataset_name,
                p_check_name=validator_name,
                p_category="VALIDATOR_ERROR",
                p_severity=QualitySeverity.HIGH,
                p_description=f"Validator execution error: {str(e)[:500]}",
                p_status=ValidationStatus.FAIL,
            )
            return None, error_record

    def validate(
        self,
        p_df: DataFrame,
        p_dataset_name: str,
        p_config: Dict[str, Any],
        p_layer: str = "raw_data",
    ) -> Tuple[DataFrame, Dict[str, Any]]:
        """Execute validations on a DataFrame.

        Args:
            p_df: DataFrame to validate
            p_dataset_name: Name of dataset being validated
            p_config: Validation configuration dictionary
            p_layer: Data layer name (default: 'raw_data')

        Returns:
            Tuple of (results_df, summary_dict) with validation outcomes
        """
        logger.info(f"Starting validation for {p_dataset_name} in layer {p_layer}")

        if not p_config.get("enabled", True):
            logger.info(f"Validation disabled for '{p_dataset_name}' in config")
            return to_dataframe(self.spark, []), {"decision": "PASS"}

        if df_is_empty(p_df):
            fail_on_empty = self._as_bool(
                p_config.get("fail_on_empty", False), p_default=False
            )
            status = ValidationStatus.FAIL if fail_on_empty else ValidationStatus.WARN
            severity = (
                QualitySeverity.CRITICAL if fail_on_empty else QualitySeverity.LOW
            )
            empty_record = create_record(
                p_dataset_name=p_dataset_name,
                p_check_name="__dataset__",
                p_category="EMPTY_DATASET",
                p_severity=severity,
                p_description=(
                    f"El dataset '{p_dataset_name}' está vacío para el rango "
                    f"solicitado."
                ),
                p_status=status,
            )
            decision = "STOP" if fail_on_empty else "PASS"
            logger.info(
                f"Dataset '{p_dataset_name}' is empty. "
                f"fail_on_empty={fail_on_empty}, decision={decision}"
            )
            return (
                to_dataframe(self.spark, [empty_record]),
                {
                    "dataset_name": p_dataset_name,
                    "total_checks": 0,
                    "failures": 1 if fail_on_empty else 0,
                    "critical_failures": 1 if fail_on_empty else 0,
                    "validator_errors": 0,
                    "decision": decision,
                },
            )

        block_on_validator_error = self._as_bool(
            p_config.get(
                "block_on_validator_error",
                os.getenv("sanity_check_block_on_validator_error", "1"),
            ),
            p_default=True,
        )

        validator_types = [
            key
            for key, value in p_config.items()
            if isinstance(value, dict) and value.get("enabled")
        ]
        if not validator_types:
            return to_dataframe(self.spark, []), {"decision": "PASS"}

        validators = self.registry.get_validators(validator_types, p_config, p_layer)

        if not validators:
            logger.warning(
                f"No validators found for types {validator_types} in layer {p_layer}"
            )
            return to_dataframe(self.spark, []), {"decision": "PASS"}

        results_dfs = []
        error_records = []

        max_workers = min(4, len(validators))

        persisted_here = False
        if len(validators) > 1:
            try:
                storage = p_df.storageLevel
                already_persisted = storage.useMemory or storage.useDisk
            except Exception:
                already_persisted = False
            if not already_persisted:
                p_df = p_df.persist(StorageLevel.MEMORY_AND_DISK)
                persisted_here = True

        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_validator = {
                    executor.submit(
                        self._run_validator_safely, validator, p_df, p_dataset_name
                    ): validator
                    for validator in validators
                }

                for future in as_completed(future_to_validator):
                    result_df, error_record = future.result()
                    if result_df is not None and not df_is_empty(result_df):
                        results_dfs.append(result_df)
                    if error_record is not None:
                        error_records.append(error_record)
        finally:
            if persisted_here:
                try:
                    p_df.unpersist()
                except Exception:
                    pass

        if error_records:
            error_df = to_dataframe(self.spark, error_records)
            results_dfs.append(error_df)
            logger.warning(
                f"Added {len(error_records)} validator error record(s) "
                f"for {p_dataset_name}"
            )

        if not results_dfs:
            return to_dataframe(self.spark, []), {"decision": "PASS"}

        final_results_df = reduce(
            lambda d1, d2: d1.unionByName(d2, allowMissingColumns=True),
            results_dfs,
        )

        agg_result = final_results_df.agg(
            f.count(f.when(final_results_df.estado == "FAIL", 1)).alias("fail_count"),
            f.count(
                f.when(
                    (final_results_df.estado == "FAIL")
                    & (final_results_df.severidad == "CRITICAL"),
                    1,
                )
            ).alias("critical_count"),
        ).collect()[0]

        fail_count = agg_result["fail_count"] or 0
        critical_count = agg_result["critical_count"] or 0

        should_stop_on_validator_errors = (
            block_on_validator_error and len(error_records) > 0
        )
        decision = (
            "STOP" if critical_count > 0 or should_stop_on_validator_errors else "PASS"
        )
        summary = {
            "dataset_name": p_dataset_name,
            "total_checks": len(validators),
            "failures": fail_count,
            "critical_failures": critical_count,
            "validator_errors": len(error_records),
            "block_on_validator_error": block_on_validator_error,
            "decision": decision,
        }

        logger.info(
            f"Validation completed for {p_dataset_name}. "
            f"Failures: {fail_count}, Critical: {critical_count}, "
            f"Errors: {len(error_records)}, "
            f"BlockOnValidatorErrors: {block_on_validator_error}, "
            f"Decision: {decision}"
        )
        return final_results_df, summary


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

        Args:
            p_path: Path to the configuration file or directory.
            p_domain_filter: Optional domain filter to select specific configurations.

        Returns:
            Merged configuration dictionary.
        """
        try:
            absolute_path = os.path.abspath(p_path)
            path_parts = absolute_path.split(os.sep)
            layer_name = next(
                (part for part in path_parts if part in LayerUtils.VALID_LAYERS), None
            )
            if not layer_name:
                logger.warning(f"Could not identify layer from path: {absolute_path}")
                return {}

            try:
                config_dir = Path(absolute_path)
            except Exception:
                return {}

            if config_dir.is_file() or str(config_dir).lower().endswith(".yml"):
                config_dir = config_dir.parent

            base_dir = Path(EnvironmentConfig.get_root_dir())
            candidates = [
                config_dir,
                base_dir / layer_name / "src" / "sanity_check" / "conf",
            ]

            config_path = None
            for cand in candidates:
                if cand.exists() and cand.is_dir():
                    config_path = cand
                    break

            if not config_path:
                logger.warning(
                    f"No sanity check config directory found for layer "
                    f"'{layer_name}'. Tried: {[str(c) for c in candidates]}. "
                    f"If running from an installed package, verify the conf "
                    f"directory ships an __init__.py so its YAML files are packaged."
                )
                return {}

            pattern = (
                f"{p_domain_filter}_sanity_check.yml"
                if p_domain_filter
                else "*_sanity_check.yml"
            )
            domain_files = (
                list(config_path.glob(pattern)) if config_path.exists() else []
            )
            if not domain_files and p_domain_filter:
                sub = config_path / p_domain_filter
                if sub.exists():
                    domain_files = list(sub.glob("*_sanity_check.yml"))
                if not domain_files:
                    logger.warning(f"No config found for domain '{p_domain_filter}'")
                    return {}
            elif not domain_files:
                excluded = {"sanity_check", "utils", "conf"}
                if config_path.exists() and config_path.is_dir():
                    for d in config_path.iterdir():
                        if (
                            d.is_dir()
                            and not d.name.startswith(("_", "."))
                            and d.name not in excluded
                        ):
                            domain_files.extend(list(d.glob("*_sanity_check.yml")))
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
            return matches[0][1]
        if len(matches) > 1:
            logger.warning(
                f"Ambiguous config match for '{p_dataset_name}': "
                f"{[m[0] for m in matches]}. No config applied."
            )

        return None

    @staticmethod
    def consolidate_results(p_results: List[DataFrame]) -> DataFrame:
        """Consolidate multiple DataFrame results into a single DataFrame.

        Args:
            p_results: List of DataFrames to consolidate.

        Returns:
            A single DataFrame containing all valid results.
        """
        empty = get_spark().createDataFrame([], schema=VALIDATION_RESULT_SCHEMA)
        valid_results = [
            r for r in (p_results or []) if r is not None and not df_is_empty(r)
        ]

        if not valid_results:
            return empty
        if len(valid_results) == 1:
            return valid_results[0]

        try:
            return reduce(
                lambda d1, d2: d1.unionByName(d2, allowMissingColumns=True),
                valid_results,
            )
        except Exception as e:
            logger.warning(f"unionByName failed: {e}, falling back to union")
            return reduce(lambda d1, d2: d1.union(d2), valid_results)

    @staticmethod
    def log_summary(p_logger: Any, p_summary: Union[Dict[str, Any], List[Any]]) -> None:
        """Log the validation summary.

        Args:
            p_logger: Logger instance to use for logging.
            p_summary: Validation summary to log.
        """
        (p_logger or logger).info(f"Validation summary: {p_summary}")

    @staticmethod
    def _add_observation_dates(
        p_df: DataFrame,
        p_month_start: Optional[str],
        p_month_end: Optional[str],
    ) -> DataFrame:
        """Add observation start and end dates to the DataFrame.

        Args:
            p_df: Input DataFrame.
            p_month_start: Optional start date.
            p_month_end: Optional end date.

        Returns:
            DataFrame with added observation dates.
        """
        if p_month_end:
            p_df = p_df.withColumn("_observ_end_dt", f.lit(p_month_end))
        if p_month_start:
            p_df = p_df.withColumn("_observ_start_dt", f.lit(p_month_start))
        return p_df

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

        col_expr = f.to_date(f.col(actual_col))
        start_expr = f.to_date(f.lit(p_start_dt))
        end_expr = f.to_date(f.lit(p_end_dt))

        filtered = p_df.filter(col_expr.between(start_expr, end_expr))

        return filtered

    _MISSING_TABLE_PHRASES = (
        "table_or_view_not_found",
        "does_not_exist",
        "does not exist",
        "cannot be found",
        "invalid_table_or_view",
        "unresolvedrelation",
        "unresolved relation",
        "delta_table_not_found",
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
    def _execute_spark_sql_with_retry(
        p_sql_query: str,
        p_label: str,
        p_max_retries: int = 2,
        p_raise_on_failure: bool = False,
    ) -> Optional[DataFrame]:
        """Execute Spark SQL with automatic session.

        Args:
            p_sql_query: SQL query to execute
            p_label: Label for logging and error messages
            p_max_retries: Maximum number of retry attempts
            p_raise_on_failure: If True, re-raise the last error instead of
                returning None. Lets callers distinguish a real failure from an
                empty/None result (e.g. the quality gate must fail closed).

        Returns:
            DataFrame result or None if execution failed (when not raising).

        Raises:
            Exception: The last error encountered, only when p_raise_on_failure.
        """
        last_error = None

        for attempt in range(p_max_retries):
            try:
                return get_spark().sql(p_sql_query)
            except Exception as e:
                last_error = e
                error_msg = str(e).lower()

                is_session_error = any(
                    phrase in error_msg
                    for phrase in [
                        "session not found",
                        "invalid_handle",
                        "incorrect server side session identifier",
                        "session invalid",
                    ]
                )

                if is_session_error and attempt < p_max_retries - 1:
                    global _SPARK_SESSION
                    _SPARK_SESSION = None
                    logger.warning(
                        f"Session error for {p_label}, retrying "
                        f"(attempt {attempt + 1}/{p_max_retries})"
                    )
                    continue
                else:
                    break

        if last_error:
            logger.error(f"Failed to execute SQL for {p_label}: {last_error}")
            if p_raise_on_failure:
                raise last_error
        return None

    @staticmethod
    def load_dataset(
        p_metadata: Dict[str, Any],
        p_config: Dict[str, Any],
        p_start_dt: Optional[str] = None,
        p_end_dt: Optional[str] = None,
    ) -> Optional[DataFrame]:
        """Load a dataset (Parquet or Delta/SQL) with optional date filtering.

        Args:
            p_metadata: Dataset metadata (dataset_name, path or database, date_col)
            p_config: Dataset quality configuration
            p_start_dt: Start date for filtering; skipped when None
            p_end_dt: End date for filtering; skipped when None

        Returns:
            Filtered DataFrame or None if an error occurs.
        """

        label = p_metadata.get("dataset_name") or p_metadata.get("path") or "unknown"
        date_col = LayerUtils._resolve_date_col(p_config, p_metadata)

        try:
            path = p_metadata.get("path")
            if path:
                try:
                    df = get_spark().read.format("parquet").load(path)
                except Exception as e:
                    if "JVM_ATTRIBUTE_NOT_SUPPORTED" in str(e):
                        df = get_spark().read.parquet(path)
                    else:
                        raise
            else:
                database = p_metadata.get("database")
                dataset_name = p_metadata.get("dataset_name")
                if database and dataset_name:
                    full_table = f"{database}.{dataset_name}"
                    try:
                        catalog = EnvironmentConfig.get_catalog_name()
                        get_spark().sql(f"USE CATALOG {catalog}")
                    except Exception as e:
                        logger.warning(f"Could not set catalog: {e}")
                    df = LayerUtils._execute_spark_sql_with_retry(
                        f"SELECT * FROM {full_table}", p_label=label, p_max_retries=2
                    )
                else:
                    logger.error(
                        "Invalid metadata: missing path or database/dataset_name"
                    )
                    return None

            if df is None:
                return None

            if p_start_dt and p_end_dt:
                df = LayerUtils._apply_date_filter(
                    df, date_col, p_start_dt, p_end_dt, label
                )

            if df_is_empty(df):
                logger.warning(f"⚠️ No data found for {label} in the requested range.")
                return None

            return df

        except Exception as e:
            logger.error(f"Error loading {label}: {e}")
            return None

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

        is_parquet = any(
            v.get("file_extension", "parquet") == "parquet"
            for v in inputs.values()
            if isinstance(v, dict)
        )
        metadata = []

        if is_parquet:
            env = EnvironmentConfig.get_environment()
            settings = load_yaml_file(
                os.path.join(base_dir, "env", "base", "global_settings.yml")
            )
            if env:
                env_path = os.path.join(base_dir, "env", env, "global_settings.yml")
                if os.path.exists(env_path):
                    settings = {**settings, **load_yaml_file(env_path)}

            base_path = settings.get("raw_data_path", "")
            for entry in inputs.values():
                if not isinstance(entry, dict):
                    continue
                raw_path = entry.get("path", "").rstrip("/")
                full_path = (
                    base_path + raw_path + entry.get("additional_raw_option", "/*/*/*")
                )
                metadata.append(
                    {
                        "dataset_name": raw_path.split("/")[-1],
                        "date_col": entry.get("partition_col") or None,
                        "path": full_path,
                    }
                )
        else:
            env = EnvironmentConfig.get_environment()
            database = f"{env}_{p_data_layer}"
            for entry in inputs.values():
                if not isinstance(entry, dict):
                    continue
                table_name = entry.get("path", "")
                if table_name:
                    metadata.append(
                        {
                            "dataset_name": table_name,
                            "database": database,
                            "date_col": entry.get("partition_col") or None,
                        }
                    )
        return metadata

    @staticmethod
    def _group_month_ranges_by_year(
        p_month_ranges: List[Tuple[str, str]]
    ) -> List[List[Tuple[str, str]]]:
        """Group month ranges by year.

        Args:
            p_month_ranges: List of tuples containing start.

        Returns:
            List of lists.
        """
        if not p_month_ranges:
            return []

        years_map: Dict[str, List[Tuple[str, str]]] = {}
        for m_start, m_end in p_month_ranges:
            year = m_start[:4]
            years_map.setdefault(year, []).append((m_start, m_end))

        return [years_map[y] for y in sorted(years_map.keys())]

    @staticmethod
    def _resolve_month_ranges(
        p_start_dt: Optional[str], p_end_dt: Optional[str]
    ) -> List[Tuple[str, str]]:
        """Resolve month ranges based on start and end dates.

        Args:
            p_start_dt: Start date boundary (string format)
            p_end_dt: End date boundary (string format)

        Returns:
            List of tuples containing start and end dates for each month.
        """
        if not (p_start_dt and p_end_dt):
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
    def _validate_df_over_months(
        p_df: DataFrame,
        p_dataset_name: str,
        p_config: Dict[str, Any],
        p_validate_func,
        p_date_col: Optional[str],
        p_month_ranges: List[Tuple[str, str]],
        p_notify_on_critical: bool = False,
    ) -> Tuple[List[DataFrame], List[Dict[str, Any]]]:
        """Validate one DataFrame across several month ranges.

        Args:
            p_df: DataFrame to validate (already loaded; may be persisted).
            p_dataset_name: Dataset/table name for logging and records.
            p_config: Dataset validator configuration.
            p_validate_func: Callable (name, df, config) -> (results_df, summary).
            p_date_col: Date/partition column for monthly filtering (or None).
            p_month_ranges: List of (month_start, month_end) tuples.
            p_notify_on_critical: When True, send a Teams notification per STOP.

        Returns:
            Tuple of (list_of_result_dataframes, list_of_stop_summaries).
        """
        result_dfs: List[DataFrame] = []
        stop_summaries: List[Dict[str, Any]] = []

        for month_start, month_end in p_month_ranges:
            month_df = LayerUtils._apply_date_filter(
                p_df, p_date_col, month_start, month_end, p_dataset_name
            )

            if df_is_empty(month_df):
                logger.debug(
                    f"'{p_dataset_name}': no data for {month_start} → {month_end}"
                )
                continue

            try:
                res_df, summary = p_validate_func(p_dataset_name, month_df, p_config)
            except Exception as e:
                logger.error(
                    f"❌ Error validating '{p_dataset_name}' for "
                    f"{month_start} → {month_end}: {e}",
                    exc_info=True,
                )
                continue

            if res_df is not None and not df_is_empty(res_df):
                res_df = LayerUtils._add_observation_dates(
                    res_df, month_start, month_end
                )
                result_dfs.append(res_df)

            if summary and summary.get("decision") == "STOP":
                summary["month"] = month_end
                summary["dataset_name"] = p_dataset_name
                stop_summaries.append(summary)
                if p_notify_on_critical:
                    LayerUtils._send_critical_notification(summary)

        return result_dfs, stop_summaries

    @staticmethod
    def _process_single_month(
        p_datasets_metadata: List[Dict[str, Any]],
        p_config_all: Dict[str, Any],
        p_validate_func,
        p_month_start: str,
        p_month_end: str,
    ) -> Tuple[List[DataFrame], List[Dict[str, Any]], int, int]:
        """Process all datasets for a single month.

        Args:
            p_datasets_metadata: List of dataset metadata dictionaries
            p_config_all: Full configuration for all datasets
            p_validate_func: Callable that validates a single dataset
            p_month_start: Start date for the month
            p_month_end: End date for the month

        Returns:
            Tuple of (result_dataframes, critical_summaries)
        """
        result_dfs = []
        critical_summaries = []
        validated_count = 0
        skipped_count = 0

        datasets_to_process = len(p_datasets_metadata)
        if datasets_to_process == 0:
            return result_dfs, critical_summaries, validated_count, skipped_count

        for meta in p_datasets_metadata:
            dataset_name = meta.get("dataset_name", "unknown")
            config = LayerUtils.get_dataset_config(p_config_all, dataset_name)

            if config is None:
                skipped_count += 1
                continue

            logger.info(f"🔍 Validating dataset: {dataset_name}")
            res_df, summary = p_validate_func(
                meta, p_config_all, p_month_start, p_month_end
            )

            if res_df is not None:
                res_df = LayerUtils._add_observation_dates(
                    res_df, p_month_start, p_month_end
                )
                result_dfs.append(res_df)
                validated_count += 1
            else:
                skipped_count += 1

            if summary and summary.get("decision") == "STOP":
                summary["month"] = p_month_end
                critical_summaries.append(summary)
                LayerUtils._send_critical_notification(summary)

        return result_dfs, critical_summaries, validated_count, skipped_count

    @staticmethod
    def process_all_datasets(
        p_datasets_metadata: List[Dict[str, Any]],
        p_config_all: Dict[str, Any],
        p_validate_func,
        p_start_dt: Optional[str] = None,
        p_end_dt: Optional[str] = None,
    ) -> Tuple[Optional[DataFrame], List[Dict[str, Any]]]:
        """Generic function to process all datasets and collect validation results.

        Args:
            p_datasets_metadata: List of dataset metadata dictionaries
            p_config_all: Full configuration for all datasets
            p_validate_func: Callable that validates a single dataset
            p_start_dt: Optional start date for filtering
            p_end_dt: Optional end date for filtering

        Returns:
            Tuple of (consolidated_result_dataframe, critical_summaries_list)
        """
        month_ranges = LayerUtils._resolve_month_ranges(p_start_dt, p_end_dt)

        all_results_dfs = []
        all_critical_summaries = []
        total_validated = 0
        total_skipped = 0

        for month_start, month_end in month_ranges:
            (
                result_dfs,
                critical_summaries,
                validated,
                skipped,
            ) = LayerUtils._process_single_month(
                p_datasets_metadata,
                p_config_all,
                p_validate_func,
                month_start,
                month_end,
            )

            all_results_dfs.extend(result_dfs)
            all_critical_summaries.extend(critical_summaries)
            total_validated += validated
            total_skipped += skipped

            if len(month_ranges) > 1:
                logger.info(
                    f"   ✓ Month {month_end}: validated {validated}, skipped {skipped}"
                )

        return LayerUtils.consolidate_results(all_results_dfs), all_critical_summaries

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
            List of column names to cache (lowercased for case-insensitive matching)
        """
        columns_to_cache = set()
        columns_by_lower = {col.lower(): col for col in p_df_columns}
        unmapped_identifiers: set = set()

        def _map_token(token: str) -> Optional[str]:
            """Map a token to an actual column, tolerating qualified names."""
            actual = columns_by_lower.get(token)
            if actual:
                return actual
            if "." in token:
                return columns_by_lower.get(token.rsplit(".", 1)[-1])
            return None

        def add_configured_columns(raw_cols: Any) -> None:
            if isinstance(raw_cols, str):
                raw_cols = [raw_cols]
            if not isinstance(raw_cols, list):
                return

            for col_spec in raw_cols:
                if not isinstance(col_spec, str):
                    continue
                actual_col = _map_token(col_spec.lower())
                if actual_col:
                    columns_to_cache.add(actual_col)

        def add_dict_keys_as_columns(raw_mapping: Any) -> None:
            if isinstance(raw_mapping, dict):
                add_configured_columns(list(raw_mapping.keys()))

        def add_columns_from_consistency_rules(rules: Any) -> None:
            if not isinstance(rules, list):
                return

            pattern = r"`([^`]+)`|([A-Za-z_][A-Za-z0-9_.]*)"
            for rule in rules:
                if not isinstance(rule, dict):
                    continue

                add_configured_columns(rule.get("column"))

                condition = rule.get("condition")
                if not isinstance(condition, str):
                    continue

                for quoted_col, plain_col in re.findall(pattern, condition):
                    token = (quoted_col or plain_col).strip().lower()
                    if not token or token in _SQL_TOKEN_SKIPLIST:
                        continue
                    actual_col = _map_token(token)
                    if actual_col:
                        columns_to_cache.add(actual_col)
                    else:
                        unmapped_identifiers.add(token)

        if p_date_col:
            actual_col = columns_by_lower.get(p_date_col.lower())
            if actual_col:
                columns_to_cache.add(actual_col)

        for validator_type, validator_config in p_config.items():
            if not isinstance(validator_config, dict):
                continue
            if not validator_config.get("enabled", False):
                continue

            for key in [
                "columns",
                "fields",
                "column_list",
                "check_columns",
                "primary_key",
                "pk",
                "unique_id",
                "unique_keys",
            ]:
                add_configured_columns(validator_config.get(key))

            for key in [
                "null_thresholds",
                "null_limits",
                "null_percentages",
                "dimensions",
                "allowed_values",
                "domain_constraints",
            ]:
                add_dict_keys_as_columns(validator_config.get(key))

            value_checks = validator_config.get("value_checks")
            if isinstance(value_checks, list):
                for value_check in value_checks:
                    if not isinstance(value_check, dict):
                        continue
                    add_configured_columns(value_check.get("column"))

            if validator_type == "consistency" or "rules" in validator_config:
                add_columns_from_consistency_rules(validator_config.get("rules"))

        if unmapped_identifiers:
            logger.warning(
                f"Consistency rule(s) reference identifier(s) not matched to any "
                f"column: {sorted(unmapped_identifiers)}. Pruning will be skipped "
                f"to avoid dropping a referenced column."
            )
            return list(p_df_columns)

        has_enabled_validator = any(
            isinstance(v, dict) and v.get("enabled", False) for v in p_config.values()
        )
        if not columns_to_cache and has_enabled_validator:
            logger.warning(
                "Column extraction produced no columns while validators are "
                "enabled; skipping prune and validating with all columns."
            )
            return list(p_df_columns)

        result_cols = sorted(list(columns_to_cache))
        logger.debug(
            f"Extracted {len(result_cols)} columns for caching "
            f"(from {len(p_df_columns)} total)"
        )

        return result_cols

    @staticmethod
    def process_all_datasets_with_cache(
        p_datasets_metadata: List[Dict[str, Any]],
        p_config_all: Dict[str, Any],
        p_loader_func,
        p_validate_func,
        p_start_dt: Optional[str] = None,
        p_end_dt: Optional[str] = None,
    ) -> Tuple[Optional[DataFrame], List[Dict[str, Any]]]:
        """Process all datasets with caching for large date ranges.

        Args:
            p_datasets_metadata: List of dataset metadata dictionaries
            p_config_all: Full configuration for all datasets
            p_loader_func: Callable to load dataset for date range
            p_validate_func: Callable that validates dataset for month
            p_start_dt: Start date for filtering
            p_end_dt: End date for filtering

        Returns:
            Tuple of (consolidated results DataFrame, critical summaries list)
        """
        month_ranges = LayerUtils._resolve_month_ranges(p_start_dt, p_end_dt)

        dataset_cache: Dict[str, Tuple[DataFrame, Dict, Optional[str]]] = {}

        for meta in p_datasets_metadata:
            dataset_name = meta.get("dataset_name", "unknown")
            config = LayerUtils.get_dataset_config(p_config_all, dataset_name)

            if config is None:
                logger.warning(f"⚠️ No config found for dataset '{dataset_name}'")
                continue

            try:
                full_df = p_loader_func(meta, p_config_all, p_start_dt, p_end_dt)

                if full_df is None or df_is_empty(full_df):
                    logger.warning(
                        f"Dataset '{dataset_name}' is empty for range "
                        f"{p_start_dt} → {p_end_dt}"
                    )
                    continue

                date_col = LayerUtils._resolve_date_col(config, meta)

                original_col_count = len(full_df.columns)
                columns_for_cache = LayerUtils._extract_columns_for_caching(
                    config, date_col, full_df.columns
                )

                if len(columns_for_cache) < original_col_count:
                    optimized_df = full_df.select(*columns_for_cache)
                else:
                    optimized_df = full_df

                optimized_df.persist(StorageLevel.MEMORY_AND_DISK)

                dataset_cache[dataset_name] = (optimized_df, config, date_col)

            except Exception as e:
                logger.error(
                    f"❌ Error loading dataset '{dataset_name}': {e}", exc_info=True
                )
                continue

        if not dataset_cache:
            logger.warning("No datasets loaded for caching")
            return LayerUtils.consolidate_results([]), []

        all_results_dfs = []
        all_critical_summaries = []

        try:
            for dataset_name, (full_df, config, date_col) in dataset_cache.items():
                res_dfs, stop_summaries = LayerUtils._validate_df_over_months(
                    full_df,
                    dataset_name,
                    config,
                    p_validate_func,
                    date_col,
                    month_ranges,
                    p_notify_on_critical=True,
                )
                all_results_dfs.extend(res_dfs)
                all_critical_summaries.extend(stop_summaries)
        finally:
            for full_df, _, _ in dataset_cache.values():
                try:
                    full_df.unpersist()
                except Exception as e:
                    logger.warning(f"⚠️ Error unpersisting DataFrame: {e}")

        return LayerUtils.consolidate_results(all_results_dfs), all_critical_summaries

    @staticmethod
    def _send_critical_notification(p_summary: Dict[str, Any]) -> None:
        """Notify Teams when a dataset reports CRITICAL failures.

        Args:
            p_summary: Validation summary dictionary with failures and dataset_name
        """
        if not p_summary:
            return

        critical_count = p_summary.get("critical_failures", 0)
        if critical_count <= 0:
            return

        env = EnvironmentConfig.get_environment()
        dataset_name = p_summary.get("dataset_name", "unknown")
        if env in ["dev", "prod"]:
            message = (
                f"🚨 CRITICAL sanity check failure ({env})\n"
                f"Dataset: {dataset_name}\n"
                f"Total checks: {p_summary.get('total_checks')}\n"
                f"Failures: {p_summary.get('failures')}\n"
                f"Critical failures: {critical_count}\n"
                f"Decision: {p_summary.get('decision')}"
            )
            send_message_teams(message)

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
            False if critical failures found, True if passed or table not yet created
        """
        catalog = EnvironmentConfig.get_catalog_name()
        prefix_path = os.path.join(
            EnvironmentConfig.get_root_dir(), "env", "base", "prefixes.yml"
        )
        prefix_map = load_yaml_file(prefix_path) or {}
        try:
            if not LayerUtils._validate_sql_identifier(p_dataset_name, "dataset_name"):
                return False

            if not (
                LayerUtils._is_valid_iso_date(p_start_date)
                and LayerUtils._is_valid_iso_date(p_end_date)
            ):
                logger.warning(
                    f"Quality gate skipped for '{p_dataset_name}': invalid date range "
                    f"'{p_start_date}' → '{p_end_date}' (expected YYYY-MM-DD)."
                )
                return True

            domain = os.getenv("output_domain", "")
            prefix_value = prefix_map.get(domain)

            env_value = os.getenv("env") or EnvironmentConfig.get_environment()
            schema_name = f"{env_value}_{p_layer}"
            quality_table = f"{catalog}.{schema_name}.sc_sanity_check_results"

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

            try:
                tables_df = LayerUtils._execute_spark_sql_with_retry(
                    f"SHOW TABLES IN {catalog}.{schema_name}",
                    p_label=p_dataset_name,
                    p_max_retries=1,
                )
                if (
                    tables_df is None
                    or tables_df.filter(
                        tables_df.tableName == "sc_sanity_check_results"
                    ).count()
                    == 0
                ):
                    fallback_table = f"{catalog}.sc_sanity_check_results"
                    try:
                        tables_df2 = LayerUtils._execute_spark_sql_with_retry(
                            f"SHOW TABLES IN {catalog}",
                            p_label=p_dataset_name,
                            p_max_retries=1,
                        )

                        catalog_has_table = (
                            False
                            if tables_df2 is None
                            else tables_df2.filter(
                                tables_df2.tableName == "sc_sanity_check_results"
                            ).count()
                            > 0
                        )

                        if catalog_has_table:
                            quality_table = fallback_table
                        else:
                            logger.info(
                                f"ℹ️  Quality gate table not yet created for "
                                f"{p_layer} (schema='{schema_name}'). "
                                f"First sanity check execution will initialize it. "
                                f"Allowing {p_dataset_name} to proceed."
                            )
                            return True
                    except Exception:
                        logger.debug("Could not list tables in catalog")
            except Exception:
                quality_table = f"{catalog}.sc_sanity_check_results"

            quality_df = LayerUtils._execute_spark_sql_with_retry(
                f"SELECT nombre_dataset, _observ_end_dt, estado, severidad "
                f"FROM {quality_table} "
                f"WHERE estado = 'FAIL' AND severidad = 'CRITICAL' "
                f"AND nombre_dataset IN ('{dataset_list}') "
                f"AND _observ_end_dt >= '{p_start_date}' "
                f"AND _observ_end_dt <= '{p_end_date}'",
                p_label=p_dataset_name,
                p_max_retries=2,
                p_raise_on_failure=True,
            )

            critical_count = quality_df.count()

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
    def validate_single_dataset(
        p_metadata: Dict[str, Any],
        p_config_all: Dict[str, Any],
        p_load_func,
        p_validate_func,
        p_log_func,
        p_start_dt: Optional[str] = None,
        p_end_dt: Optional[str] = None,
    ) -> Tuple[Optional[DataFrame], Optional[Dict[str, Any]]]:
        """Generic function to validate a single dataset with layer-specific log.

        Args:
            p_metadata: Metadata dict for the dataset.
            p_config_all: Full configuration dict for all datasets.
            p_load_func: Callable to load the dataset.
            p_validate_func: Callable to validate the dataset.
            p_log_func: Callable to log the summary results.
            p_start_dt: Optional start date for filtering.
            p_end_dt: Optional end date for filtering.

        Returns:
            Tuple of (results_dataframe, summary_dictionary)
        """
        dataset_name = p_metadata.get("dataset_name", "unknown")
        config = LayerUtils.get_dataset_config(p_config_all, dataset_name)

        if config and "_config_file" in config:
            for key, val in p_config_all.items():
                if val is config:
                    dataset_name = key
                    p_metadata["dataset_name"] = key
                    break

        try:
            df = p_load_func(p_metadata, config, p_start_dt, p_end_dt)

            if df is None:
                return None, None

            results_df, summary = p_validate_func(dataset_name, df, config)

            if summary is not None:
                p_log_func(logger, summary)
            else:
                logger.warning(f"{dataset_name}: validation produced no summary")

            return results_df, summary

        except Exception as e:
            logger.error(f"❌ {dataset_name}: {e}", exc_info=True)
            return None, {"dataset_name": dataset_name, "decision": "STOP"}

    @staticmethod
    def _get_layer_config_path(p_layer_name: str) -> str:
        """Get default config path for a layer.

        Args:
            p_layer_name: Name of the layer (raw_data, data_cleaning, etc.)

        Returns:
            Full path to layer's configuration directory
        """
        base_dir = EnvironmentConfig.get_root_dir()
        if p_layer_name == "raw_data":
            return os.path.join(base_dir, p_layer_name, "src", "sanity_check", "conf")
        else:
            return os.path.join(base_dir, p_layer_name, "src")

    @staticmethod
    def create_layer_config_loader(
        p_layer_name: str, p_domain_filter: Optional[str] = None
    ) -> Any:
        """Create a config loader function for a specific layer."""

        def load_config(
            p_path: Optional[str] = None, p_domain_filter_override: Optional[str] = None
        ) -> Dict[str, Any]:
            filter_to_use = p_domain_filter_override or p_domain_filter
            from data_engineering.core.sanity_check.utils import EnvironmentConfig

            base_dir = EnvironmentConfig.get_root_dir()
            layer_config_path = (
                os.path.join(base_dir, p_layer_name, "src", "sanity_check", "conf")
                if p_layer_name == "raw_data"
                else os.path.join(base_dir, p_layer_name, "src")
            )
            path = p_path or layer_config_path
            return LayerUtils.load_config(path, p_domain_filter=filter_to_use)

        return load_config

    @staticmethod
    def create_layer_validator(p_layer_name: str) -> Any:
        """Create a validate_dataset function for a specific layer."""

        def validate_dataset(
            p_dataset_name: str, p_df: DataFrame, p_config: Dict[str, Any]
        ) -> Tuple[DataFrame, Any]:
            return QualityEngine().validate(
                p_df, p_dataset_name, p_config, p_layer=p_layer_name
            )

        return validate_dataset


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
        workflow = os.getenv("workflow", "unknown")
        flow_name = os.getenv("flow", "unknown")
        node_name = os.getenv("node", None)
        is_isolated = node_name is not None
        domain = None
        if is_isolated and node_name and "." in node_name:
            domain = node_name.split(".")[0]

        context = cls(
            workflow=workflow,
            flow_name=flow_name,
            node_name=node_name,
            domain=domain,
            is_isolated_execution=is_isolated,
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
