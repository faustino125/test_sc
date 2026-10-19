import copy
import logging
import os
import time
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple

from pyspark.sql import DataFrame, SparkSession  # type: ignore

from data_engineering.core.sanity_check.base import (  # noqa: F401 (re-exported)
    VALIDATION_RESULT_SCHEMA,
    ValidationRecord,
    create_record,
)
from data_engineering.core.utils import load_yaml_file as _load_yaml_file_uncached

logger = logging.getLogger(__name__)

# Parsed YAML by real path; the mtime invalidates an entry if the file changes.
_YAML_CACHE: Dict[str, Tuple[float, Any]] = {}
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

ENV_VAR_ENVIRONMENT = "env"
ENV_VAR_END_DATE = "end_dt"
DEFAULT_ENVIRONMENT = "dev"
ENV_VAR_ROOT_DIR = "root_dir"
DEFAULT_LAYER = "raw_data"
GLOBAL_SETTINGS_FILE = os.path.join("env", "base", "global_settings.yml")
PREFIXES_FILE = os.path.join("env", "base", "prefixes.yml")

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
    return p_spark.createDataFrame(
        result_rows(p_records), schema=VALIDATION_RESULT_SCHEMA
    )


def result_rows(p_records: List[ValidationRecord]) -> List[Dict[str, Any]]:
    """Rows written for the records, with execution and observation dates filled.

    Missing `_observ_end_dt` / `_observ_start_dt` default to end_dt from the
    environment (or today, UTC-6) and the first day of that month.
    """
    if not p_records:
        return []

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

    default_start = end_dt_date.replace(day=1).strftime("%Y-%m-%d")

    def _start_str(p_value: Any) -> str:
        """Record start date as YYYY-MM-DD, or the default when unusable."""
        if isinstance(p_value, str) and p_value:
            return p_value
        try:
            return p_value.strftime("%Y-%m-%d") if p_value else default_start
        except Exception:
            return default_start

    return [
        {
            **r_dict,
            "fecha_ejecucion": r_dict.get("fecha_ejecucion") or now,
            "_observ_end_dt": r_dict.get("_observ_end_dt") or observ_end_dt,
            "_observ_start_dt": _start_str(r_dict.get("_observ_start_dt")),
        }
        for r_dict in (asdict(r) for r in p_records)
    ]


def load_yaml_file(p_file_path: str) -> Any:
    """core.utils.load_yaml_file, parsed once per process for the sanity check.

    The sanity check reads the same catalog, io_config, prefixes and settings
    for every dataset and node. A deep copy is returned, so callers can modify
    the result without touching the cache. Errors are raised as before.
    """
    # Same resolution as core.utils.load_yaml_file (relative to data_engineering/).
    real_path = os.path.realpath(os.path.join(_BASE_DIR, p_file_path))
    try:
        mtime = os.path.getmtime(real_path)
    except OSError:
        return _load_yaml_file_uncached(p_file_path)  # raises its usual error
    cached = _YAML_CACHE.get(real_path)
    if cached is None or cached[0] != mtime:
        cached = (mtime, _load_yaml_file_uncached(p_file_path))
        _YAML_CACHE[real_path] = cached
    return copy.deepcopy(cached[1])


def load_yaml_or_default(p_file_path: str, p_fallback: Any = None) -> Any:
    """Parsed YAML file, or p_fallback if it is missing, empty or invalid.

    Parsing is cached by load_yaml_file, so repeated calls are cheap.
    """
    if not os.path.exists(p_file_path):
        return p_fallback
    try:
        return load_yaml_file(p_file_path) or p_fallback
    except Exception as e:
        logger.error(f"Error loading {p_file_path}: {e}")
        return p_fallback


# Environment values below are resolved once per process (first call wins).
# Tests can reset them with <function>.cache_clear().


@lru_cache(maxsize=None)
def get_environment() -> str:
    """Environment name (dev, prod, ...) from the `env` variable.

    Raises:
        ValueError: If the variable is set but empty.
    """
    env = os.getenv(ENV_VAR_ENVIRONMENT, DEFAULT_ENVIRONMENT).strip()
    if not env:
        raise ValueError(f"Environment variable '{ENV_VAR_ENVIRONMENT}' cannot be empty")
    return env


@lru_cache(maxsize=None)
def get_root_dir() -> str:
    """Repository root: the `root_dir` variable, else detected from this file."""
    return os.getenv(ENV_VAR_ROOT_DIR) or str(Path(__file__).resolve().parents[2])


def config_path(*p_parts: str) -> str:
    """Absolute path of a file under the repository root."""
    return os.path.join(get_root_dir(), *p_parts)


@lru_cache(maxsize=None)
def get_catalog_name() -> str:
    """Databricks catalog: <company>_<dev|prod|sandbox>_de."""
    settings = load_yaml_or_default(config_path(GLOBAL_SETTINGS_FILE), {})
    company = settings.get("company", "bi") if isinstance(settings, dict) else "bi"
    env = get_environment()
    return f"{company}_{env if env in ('prod', 'dev') else 'sandbox'}_de"
