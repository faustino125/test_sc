import logging
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from enum import Enum
from functools import reduce
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

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

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


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


class GroupedMode(str, Enum):
    """How a validator can be evaluated for many months in one Spark job.

    NONE: only `validate()` on one month at a time (layer-specific validators).
    AGGREGATE: its metrics are conditional counts; the engine merges them with
        the other validators' into a single `groupBy(month).agg(...)`.
    KEYED: it counts rows per key (duplicates); the engine runs
        `keyed_stats_frame` grouped by month, one shuffle per block of months.
    """

    NONE = "none"
    AGGREGATE = "aggregate"
    KEYED = "keyed"


def keyed_stats_frame(
    p_df: DataFrame, p_keys: Sequence[str], p_group_cols: Sequence[str] = ()
) -> DataFrame:
    """Count duplicated keys, affected rows and null-key rows per group.

    One shuffle: rows are counted per (group, key) and then summarized per group.
    Rows with any null key column are reported separately, never as duplicates.

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


class BaseValidator(ABC):
    """Abstract base class for all validators.

    Layer-specific validators only need `validate()`. Validators that set
    GROUPED_MODE also implement the matching prepare/build methods, which let
    the engine evaluate all months of a history in a few Spark jobs.
    """

    GROUPED_MODE: GroupedMode = GroupedMode.NONE

    def __init__(self, config: Dict[str, Any], spark: SparkSession) -> None:
        """Initialize validator with configuration and Spark session."""
        self.config = config
        self.spark = spark
        self._column_cache: Dict[str, Optional[str]] = {}

    def _resolve_column(self, p_df: DataFrame, p_col_name: str) -> Optional[str]:
        """Resolve a column name case-insensitively with caching.

        Args:
            p_df: DataFrame to search for the column
            p_col_name: Column name (case-insensitive)

        Returns:
            Actual column name if found, None otherwise
        """
        if p_col_name in self._column_cache:
            return self._column_cache[p_col_name]

        actual = next(
            (c for c in p_df.columns if c.lower() == p_col_name.lower()), None
        )
        self._column_cache[p_col_name] = actual
        return actual

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
        """Resolve severity from config or return default with strict validation.

        Args:
            default: Default severity if not found in config

        Returns:
            QualitySeverity enum matching configured value.
        """
        return self.coerce_severity(self.config.get("severity", default), default)

    def _get_config_value(self, keys: List[str], default: Any = None) -> Any:
        """Get configuration value by testing multiple possible keys in order.

        Args:
            keys: List of config keys to try in order
            default: Default value if no key is found

        Returns:
            Value from first found key, or default
        """
        for key in keys:
            value = self.config.get(key)
            if value is not None:
                return value
        return default

    def _add_validation_record(
        self,
        records: List,
        dataset_name: str,
        check_name: str,
        category: str,
        failed: bool,
        fail_description: str,
        pass_description: str,
        fail_severity: Optional["QualitySeverity"] = None,
    ) -> None:
        """Add validation record with unified PASS/FAIL logic.

        Args:
            records: List to append record to
            dataset_name: Dataset name
            check_name: Check/column name
            category: Validation category
            failed: Whether check failed
            fail_description: Description for FAIL case
            pass_description: Description for PASS case
            fail_severity: Severity for FAIL case (overrides default)
        """
        from data_engineering.core.sanity_check.utils import create_record

        if failed:
            records.append(
                create_record(
                    p_dataset_name=dataset_name,
                    p_check_name=check_name,
                    p_category=category,
                    p_severity=fail_severity or self._get_severity(),
                    p_description=fail_description,
                    p_status=ValidationStatus.FAIL,
                )
            )
        else:
            records.append(
                create_record(
                    p_dataset_name=dataset_name,
                    p_check_name=check_name,
                    p_category=category,
                    p_severity=QualitySeverity.LOW,
                    p_description=pass_description,
                    p_status=ValidationStatus.PASS,
                )
            )

    def _add_missing_column_record(
        self,
        records: List,
        dataset_name: str,
        column: str,
        category: str,
        fail_severity: QualitySeverity,
    ) -> None:
        """Record a FAIL for a configured column that is not in the DataFrame.

        A renamed or misspelled column must not make a check disappear silently,
        so it is reported with the severity the check would have had.

        Args:
            records: List to append record to
            dataset_name: Dataset name
            column: Configured column name that could not be resolved
            category: Validation category of the affected check
            fail_severity: Severity configured for the affected check
        """
        from data_engineering.core.sanity_check.utils import create_record

        logger.warning(
            f"{category}: configured column '{column}' not found in {dataset_name}"
        )
        records.append(
            create_record(
                p_dataset_name=dataset_name,
                p_check_name=column,
                p_category="MISSING_COLUMN",
                p_severity=fail_severity,
                p_description=(
                    f"La columna '{column}' configurada para la validación "
                    f"{category} no existe en el dataset."
                ),
                p_status=ValidationStatus.FAIL,
            )
        )

    def prepare_aggregations(
        self, p_df: DataFrame, p_dataset_name: str, p_alias_prefix: str
    ) -> Tuple[List[Column], List[ValidationRecord]]:
        """AGGREGATE mode: build the aggregate expressions for this validator.

        Called once per dataset. Records returned here (missing columns,
        invalid rules) do not depend on the data and apply to every month.

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

    def _validate_grouped_once(self, p_df: DataFrame, p_dataset_name: str) -> DataFrame:
        """Run a grouped-capable validator on a whole DataFrame as one group.

        Standard validators implement `validate()` with this, so the per-month
        path and the grouped engine share the same record-building code.
        """
        from data_engineering.core.sanity_check.utils import to_dataframe

        if self.GROUPED_MODE is GroupedMode.AGGREGATE:
            exprs, records = self.prepare_aggregations(p_df, p_dataset_name, "__v_")
            if exprs:
                row = p_df.agg(*exprs).collect()[0].asDict()
                records = records + self.build_records(row, p_dataset_name)
        elif self.GROUPED_MODE is GroupedMode.KEYED:
            keys, records = self.prepare_keys(p_df, p_dataset_name)
            if keys:
                row = keyed_stats_frame(p_df, keys).collect()[0].asDict()
                records = records + self.build_keyed_records(row, p_dataset_name)
        else:
            raise NotImplementedError(
                f"{type(self).__name__} has no grouped mode; implement validate()"
            )
        return to_dataframe(self.spark, records)

    @abstractmethod
    def validate(self, p_df: DataFrame, p_dataset_name: str) -> DataFrame:
        """Execute validation on DataFrame.

        Args:
            p_df: DataFrame to validate
            p_dataset_name: Name of dataset being validated

        Returns:
            DataFrame with validation results
        """
        pass
