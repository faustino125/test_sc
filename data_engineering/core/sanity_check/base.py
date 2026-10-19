import logging
from dataclasses import dataclass
from enum import Enum
from functools import reduce
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Sequence, Tuple

from pyspark.sql import Column, DataFrame, SparkSession  # type: ignore
from pyspark.sql import functions as f  # type: ignore
from pyspark.sql.types import StringType, StructField, StructType  # type: ignore

logger = logging.getLogger(__name__)


class QualitySeverity(str, Enum):
    """Severity levels for data quality issues."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class ValidationStatus(str, Enum):
    """Status of a validation check."""

    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"
    SKIPPED = "SKIPPED"


@dataclass
class ValidationRecord:
    """Individual validation result record."""

    nombre_dataset: str
    columna: str
    validacion: str
    severidad: str
    descripcion: str
    estado: str = "FAIL"
    fecha_ejecucion: Optional[str] = None
    _observ_end_dt: Optional[str] = None
    _observ_start_dt: Optional[str] = None


VALIDATION_RESULT_SCHEMA = StructType(
    [
        StructField("nombre_dataset", StringType(), False),
        StructField("columna", StringType(), False),
        StructField("validacion", StringType(), False),
        StructField("descripcion", StringType(), False),
        StructField("severidad", StringType(), False),
        StructField("estado", StringType(), False),
        StructField("_observ_start_dt", StringType(), True),
        StructField("_observ_end_dt", StringType(), True),
        StructField("fecha_ejecucion", StringType(), True),
    ]
)


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


def find_column(p_columns: Sequence[str], p_name: str) -> Optional[str]:
    """Actual column name matching p_name case-insensitively, or None."""
    name = p_name.lower()
    return next((c for c in p_columns if c.lower() == name), None)


class GroupedMode(str, Enum):
    """How a validator is evaluated for many months in one Spark job.
    """

    AGGREGATE = "aggregate"
    KEYED = "keyed"


def keyed_stats_frame(
    p_df: DataFrame, p_keys: Sequence[str], p_group_cols: Sequence[str] = ()
) -> DataFrame:
    """Count duplicated keys, affected rows and null-key rows per group.

    Args:
        p_df: DataFrame to evaluate
        p_keys: Actual key column names
        p_group_cols: Grouping columns (e.g. the month); empty for one global row

    Returns:
        DataFrame with p_group_cols plus dup_keys, dup_rows and null_key_rows.
    """
    counted = p_df.groupBy(*p_group_cols, *p_keys).agg(
        f.count(f.lit(1)).alias("__rows_per_key")
    )
    null_key = reduce(lambda a, b: a | b, [f.col(k).isNull() for k in p_keys])
    duplicated = (~null_key) & (f.col("__rows_per_key") > 1)
    return counted.groupBy(*p_group_cols).agg(
        f.count(f.when(duplicated, 1)).alias("dup_keys"),
        f.sum(f.when(duplicated, f.col("__rows_per_key"))).alias("dup_rows"),
        f.sum(f.when(null_key, f.col("__rows_per_key"))).alias("null_key_rows"),
    )


class BaseValidator:
    """Base class for the validators.
    """

    GROUPED_MODE: GroupedMode
    CONFIG_KEYS: FrozenSet[str] = frozenset()
    _COMMON_KEYS = frozenset({"enabled", "severity"})

    def __init__(self, config: Dict[str, Any], spark: SparkSession) -> None:
        """Initialize validator with configuration and Spark session."""
        self.config = config
        self.spark = spark
        self._column_cache: Dict[str, Optional[str]] = {}
        self._df_columns: Optional[List[str]] = None

        unknown = set(config) - self._COMMON_KEYS - self.CONFIG_KEYS
        if unknown:
            logger.warning(
                f"{type(self).__name__}: unknown config key(s) {sorted(unknown)} "
                f"are ignored. Valid keys: {sorted(self._COMMON_KEYS | self.CONFIG_KEYS)}"
            )

    @classmethod
    def config_columns(cls, p_config: Mapping[str, Any]) -> Tuple[List[str], List[str]]:
        """Columns this validator reads, for column pruning.

        Returns:
            Tuple of (column names, identifiers parsed from SQL conditions).
            An identifier that matches no column disables pruning.
        """
        return [], []

    def bind_columns(self, p_columns: Sequence[str]) -> None:
        """Columns of the DataFrame this validator will be prepared with.

        With Spark Connect every `df.columns` is a round trip to the server
        (and may list the source files again), so the runner reads them once.
        """
        self._df_columns = list(p_columns)

    def _resolve_column(self, p_df: DataFrame, p_col_name: str) -> Optional[str]:
        """Resolve a column name case-insensitively with caching.

        Args:
            p_df: DataFrame to search for the column
            p_col_name: Column name (case-insensitive)

        Returns:
            Actual column name if found, None otherwise
        """
        if self._df_columns is None:
            self._df_columns = list(p_df.columns)
        if p_col_name not in self._column_cache:
            self._column_cache[p_col_name] = find_column(self._df_columns, p_col_name)
        return self._column_cache[p_col_name]

    @staticmethod
    def coerce_severity(
        sev: Any, default: QualitySeverity = QualitySeverity.MEDIUM
    ) -> QualitySeverity:
        """Coerce an arbitrary value into a QualitySeverity with strict validation.

        Args:
            sev: Raw severity value (None, str or QualitySeverity)
            default: Default severity if value is None

        Returns:
            QualitySeverity enum matching the configured value.

        Raises:
            ValueError: If the value is a non-empty, unrecognized severity.
        """
        if sev is None:
            return default

        if isinstance(sev, QualitySeverity):
            return sev

        if isinstance(sev, str):
            # Member names and values are identical (LOW == "LOW", ...).
            try:
                return QualitySeverity[sev.strip().upper()]
            except KeyError:
                valid_opts = ", ".join(s.name for s in QualitySeverity)
                raise ValueError(
                    f"Invalid severity '{sev}'. Valid options: {valid_opts}"
                ) from None

        raise ValueError(f"QualitySeverity enum, got {type(sev).__name__}")

    def _get_severity(
        self, default: QualitySeverity = QualitySeverity.MEDIUM
    ) -> QualitySeverity:
        """Resolve severity from config or return default with strict validation."""
        return self.coerce_severity(self.config.get("severity", default), default)

    def _cfg(self, p_key: str, p_default: Any = None) -> Any:
        """Config value of p_key, or p_default when it is missing or None."""
        value = self.config.get(p_key)
        return p_default if value is None else value

    def _result_record(
        self,
        dataset_name: str,
        check_name: str,
        category: str,
        failed: bool,
        fail_description: str,
        pass_description: str,
        fail_severity: Optional[QualitySeverity] = None,
    ) -> ValidationRecord:
        """PASS (LOW) or FAIL (fail_severity or the configured one) record."""
        if failed:
            return create_record(
                dataset_name,
                check_name,
                category,
                fail_severity or self._get_severity(),
                fail_description,
                ValidationStatus.FAIL,
            )
        return create_record(
            dataset_name,
            check_name,
            category,
            QualitySeverity.LOW,
            pass_description,
            ValidationStatus.PASS,
        )

    @staticmethod
    def _missing_column_record(
        dataset_name: str,
        column: str,
        category: str,
        fail_severity: QualitySeverity,
    ) -> ValidationRecord:
        """FAIL for a configured column that is not in the DataFrame.
        """
        logger.warning(
            f"{category}: configured column '{column}' not found in {dataset_name}"
        )
        return create_record(
            dataset_name,
            column,
            "MISSING_COLUMN",
            fail_severity,
            f"La columna '{column}' configurada para la validación "
            f"{category} no existe en el dataset.",
            ValidationStatus.FAIL,
        )

    def prepare_aggregations(
        self, p_df: DataFrame, p_dataset_name: str, p_alias_prefix: str
    ) -> Tuple[List[Column], List[ValidationRecord]]:
        """AGGREGATE mode: build the aggregate expressions for this validator.

        Args:
            p_df: DataFrame that will be aggregated (used to resolve columns)
            p_dataset_name: Dataset name for the records
            p_alias_prefix: Prefix that makes this validator's aliases unique

        Returns:
            Tuple of (aggregate expressions, static records)
        """
        raise NotImplementedError

    def build_records(
        self, p_row: Mapping[str, Any], p_dataset_name: str
    ) -> List[ValidationRecord]:
        """AGGREGATE mode: build the records of one month from its aggregate row."""
        raise NotImplementedError

    def prepare_keys(
        self, p_df: DataFrame, p_dataset_name: str
    ) -> Tuple[List[str], List[ValidationRecord]]:
        """KEYED mode: resolve the key columns.

        Returns:
            Tuple of (actual key columns, static records). An empty key list
            means there is nothing to compute.
        """
        raise NotImplementedError

    def build_keyed_records(
        self, p_row: Mapping[str, Any], p_dataset_name: str
    ) -> List[ValidationRecord]:
        """KEYED mode: build the records of one month from its keyed_stats_frame row."""
        raise NotImplementedError
