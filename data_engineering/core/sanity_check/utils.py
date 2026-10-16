import logging
import os
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from pyspark.sql import DataFrame, SparkSession  # type: ignore

from data_engineering.core.sanity_check.base import (
    VALIDATION_RESULT_SCHEMA,
    QualitySeverity,
    ValidationRecord,
    ValidationStatus,
)

logger = logging.getLogger(__name__)

ENV_VAR_ENVIRONMENT = "env"
ENV_VAR_END_DATE = "end_dt"
DEFAULT_ENVIRONMENT = "dev"
ENV_VAR_ROOT_DIR = "root_dir"
DEFAULT_LAYER = "raw_data"

_TIMEZONE_UTC_6 = timezone(timedelta(hours=-6))


@contextmanager
def timed_phase(p_label: str) -> Iterator[None]:
    """Log how long a sanity check phase took, even when it raises.

    Lines are prefixed with [SC-TIMING] so a run can be profiled by filtering
    the logs, without the Spark UI.

    Args:
        p_label: Phase description (dataset, block, step)
    """
    start = time.perf_counter()
    try:
        yield
    finally:
        logger.info(f"[SC-TIMING] {p_label}: {time.perf_counter() - start:.1f}s")


def create_record(
    p_dataset_name: str,
    p_check_name: str,
    p_category: str,
    p_severity: QualitySeverity,
    p_description: str,
    p_status: ValidationStatus = ValidationStatus.FAIL,
) -> ValidationRecord:
    """Create a validation record with unified structure.

    Args:
        p_dataset_name: Name of the dataset being validated
        p_check_name: Name of the specific check or column
        p_category: Validation category
        p_severity: Severity level (LOW, MEDIUM, HIGH, CRITICAL)
        p_description: Human-readable description of the validation result
        p_status: Validation status (PASS, FAIL, WARN, SKIPPED)

    Returns:
        ValidationRecord instance with all fields populated
    """
    return ValidationRecord(
        nombre_dataset=p_dataset_name,
        columna=p_check_name,
        validacion=p_category,
        estado=p_status.value,
        severidad=p_severity.value,
        descripcion=p_description,
    )


def to_dataframe(
    p_spark: SparkSession,
    p_records: List[ValidationRecord],
) -> DataFrame:
    """Convert validation records to Spark DataFrame safely.

    Args:
        p_spark: Active SparkSession
        p_records: List of ValidationRecord instances

    Returns:
        DataFrame with schema matching VALIDATION_RESULT_SCHEMA
    """
    if not p_records:
        return p_spark.createDataFrame([], schema=VALIDATION_RESULT_SCHEMA)

    now_utc_6 = datetime.now(_TIMEZONE_UTC_6)
    now = now_utc_6.strftime("%Y-%m-%d %H:%M:%S")
    observ_end_dt = os.getenv(ENV_VAR_END_DATE, now_utc_6.strftime("%Y-%m-%d"))
    try:
        end_dt_date = datetime.strptime(observ_end_dt, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        logger.warning(
            f"Invalid '{ENV_VAR_END_DATE}' value '{observ_end_dt}' "
            f"(expected YYYY-MM-DD). Falling back to current date."
        )
        end_dt_date = now_utc_6.date()
        observ_end_dt = end_dt_date.strftime("%Y-%m-%d")
    observ_start_dt = end_dt_date.replace(day=1)

    default_start = observ_start_dt.strftime("%Y-%m-%d")

    def _start_str(p_value: Any) -> str:
        """Record start date as YYYY-MM-DD, or the default when unusable."""
        if isinstance(p_value, str) and p_value:
            return p_value
        try:
            return p_value.strftime("%Y-%m-%d") if p_value else default_start
        except Exception:
            return default_start

    records_for_df = [
        {
            **r_dict,
            "fecha_ejecucion": r_dict.get("fecha_ejecucion") or now,
            "_observ_end_dt": r_dict.get("_observ_end_dt") or observ_end_dt,
            "_observ_start_dt": _start_str(r_dict.get("_observ_start_dt")),
        }
        for r_dict in (r.to_dict() for r in p_records)
    ]

    return p_spark.createDataFrame(records_for_df, schema=VALIDATION_RESULT_SCHEMA)


class ConfigLoader:
    """Centralized YAML configuration loader with standardized error handling."""

    _yaml_cache: Dict[str, Any] = {}

    @staticmethod
    def load_yaml(p_file_path: str, p_config_name: str, p_fallback_value=None):
        """Load YAML config file with error handling and caching.

        Args:
            p_file_path: Path to the YAML file
            p_config_name: Human-readable config name for error messages
            p_fallback_value: Default value if file not found or load fails

        Returns:
            Parsed YAML content or fallback value
        """
        if p_file_path in ConfigLoader._yaml_cache:
            return ConfigLoader._yaml_cache[p_file_path]

        if not os.path.exists(p_file_path):
            ConfigLoader._yaml_cache[p_file_path] = p_fallback_value
            return p_fallback_value

        try:
            from data_engineering.core.utils import load_yaml_file

            config = load_yaml_file(p_file_path)
            ConfigLoader._yaml_cache[p_file_path] = config or p_fallback_value
            return config or p_fallback_value
        except Exception as e:
            logger.error(f"Error loading {p_config_name} from {p_file_path}: {e}")
            ConfigLoader._yaml_cache[p_file_path] = p_fallback_value
            return p_fallback_value

    @staticmethod
    def load_global_settings(p_root_dir: Optional[str] = None) -> Dict[str, Any]:
        """Load global_settings.yml with company and environment info.

        Args:
            p_root_dir: Root directory (auto-detected if None)

        Returns:
            Global settings dictionary with company, environment, catalog config
        """
        if p_root_dir is None:
            p_root_dir = EnvironmentConfig.get_root_dir()

        settings_path = os.path.join(p_root_dir, "env", "base", "global_settings.yml")
        return ConfigLoader.load_yaml(
            settings_path, "global_settings.yml", p_fallback_value={}
        )


class EnvironmentConfig:
    """Unified access to environment variables with caching and validation."""

    _cache: Dict[str, str] = {}

    @classmethod
    def get_environment(cls) -> str:
        """Get and validate environment configuration (cached).

        Returns:
            Environment name (dev, prod, etc.)

        Raises:
            ValueError: If environment variable is empty after resolution
        """
        if "environment" in cls._cache:
            return cls._cache["environment"]

        env = os.getenv(ENV_VAR_ENVIRONMENT, DEFAULT_ENVIRONMENT).strip()
        if not env:
            raise ValueError(
                f"Environment variable '{ENV_VAR_ENVIRONMENT}' cannot be empty"
            )

        cls._cache["environment"] = env
        return env

    @classmethod
    def get_root_dir(cls) -> str:
        """Return the repository root directory.

        Preference order:
        - Cached value (the first resolution wins for the whole process)
        - Environment variable `root_dir`
        - Auto-detected relative to this file
        """
        if "root_dir" not in cls._cache:
            cls._cache["root_dir"] = os.getenv(ENV_VAR_ROOT_DIR) or str(
                Path(__file__).resolve().parents[2]
            )
        return cls._cache["root_dir"]

    @classmethod
    def get_company(cls, p_root_dir: Optional[str] = None) -> str:
        """Get company name from global settings (cached).

        Args:
            p_root_dir: Root directory path (auto-detected if None)

        Returns:
            Company identifier from global settings or default 'bi'
        """
        if "company" in cls._cache:
            return cls._cache["company"]

        try:
            settings = ConfigLoader.load_global_settings(p_root_dir)
            company = settings.get("company", "bi")
        except Exception:
            company = "bi"

        cls._cache["company"] = company
        return company

    @classmethod
    def get_catalog_name(cls, p_root_dir: Optional[str] = None) -> str:
        """Get Databricks catalog name following project conventions.

        Args:
            p_root_dir: Root directory path (auto-detected if None)

        Returns:
            Catalog identifier constructed from company and environment
        """
        if "catalog" in cls._cache:
            return cls._cache["catalog"]

        company = cls.get_company(p_root_dir)
        env = cls.get_environment()

        if env not in ["prod", "dev"]:
            env = "sandbox"

        catalog = f"{company}_{env}_de"
        cls._cache["catalog"] = catalog
        return catalog

    @classmethod
    def clear_cache(cls):
        """Clear the cache (useful for testing)."""
        cls._cache.clear()
